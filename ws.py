# Tornado-powered workserver backend.
#
# Authors: Yutong Zhao <proteneer@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import signal
import uuid
import os
import json
import time
import shutil
import hashlib
import common
import apollo
import base64
import functools

import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httputil
import tornado.httpserver
import tornado.httpclient
import tornado.options

# Capacity

# Suppose each stream returns a frame once every 5 minutes. A single stream
# returns 288 frames per day. The WS is designed to handle about 50 frame
# PUTs per second. In a single day, a WS can handle about 4,320,000 frames.
# This is equal to about 86,400 active streams. Note that 4.3 million frames
# @ 80kb/frame = 328GB worth of data per day. We will fill up 117 TB
# worth of a data a year - so we will run out of disk space way before that.

# [MISC Redis DB]

# ZSET  KEY     'heartbeats'                   | { stream_id : expire_time }

# download_token: issued by CC, set to expire after 10 days
# shared_token: issued by CC, deleted by heartbeat
# heartbeats: each key deleted on restart, and by check_heartbeats

# Expiration mechanism:
# hearbeats is a sorted set. A POST to ws/update extends expire_time in
# heartbeats. A checker callback is passed into ioloop.PeriodicCallback(),
# which checks for expired streams against the current time. Streams that
# expire can be obtained by: redis.zrangebyscore('heartbeat',0,current_time)

# PG Downloading streams: siegetank will first send a query to CC. CC assigns
# a download token (that expire in 30 days), and responds with an IP and a
# download_token. PG simply sends the token to the WS to get the downloaded
# file
#
# TODO:
# [ ] Stats
# [ ] md5 checksum of headers
# [ ] delete mechanisms

###################
# CC/PG Interface #
###################

# POST x.com/streams              - add a new stream
# PUT x.com/streams/delete        - delete a stream
# GET x.com/streams/stream_id     - download a stream

##################
# CORE Interface #
##################

# GET x.com/core/start            - start a stream (given an auth token)
# PUT x.com/core/frame            - add a frame to a stream (idempotent)
# PUT x.com/core/stop             - stop a stream
# PUT x.com/core/checkpoint       - send a checkpoint file corresponding
#                                   to the last frame received
# POST x.com/core/heartbeat       - send a heartbeat

# checkpointing technique
# if x.com/core/checkpoint is sent, message is:
# POST {frame_md5: 10gj3n60twemp9g8,
#       checkpoint: checkpoint.xml.gz.b64,
#      }
# this is how we can verify

##################

# In general, we should try and use PUTs whenever possible. Idempotency
# is an incredibly useful way of dealing with failures. Suppose a core
# either POSTs (non idempotent), or PUTs (idempotent) a frame to a stream.

# One of two failure scenarios can happen:

#              FAILS
#   Core --Send Request--> Client --Send Reply--> Core

#                                      FAILS
#   Core --Send Request--> Client --Send Reply--> Core

# Note that the Core does NOT know which scenario happened. All it knows
# is that it did not get a reply. In the second scenario, POSTing the same
# frame twice would be bad, since the stream would end up with a duplicate
# stream. However, PUTing the same frame twice (by means of checking the
# md5sum of last frame) would be the same as PUTing it once.


class Stream(apollo.Entity):
    prefix = 'stream'
    fields = {'frames': int,            # total number of frames completed
              'status': str,            # 'OK', 'DISABLED'
              'error_count': int,       # number of consecutive errors
              }


class ActiveStream(apollo.Entity):
    prefix = 'active_stream'
    fields = {'buffer_frames': int,     # number of frames in buffer.xtc
              'auth_token': str,        # used by core to send requests
              'donor': str,             # the donor assigned
              'steps': int,             # number of steps completed
              'start_time': float,      # time started
              'last_frame_md5': str     # md5sum of the last completed frame
              }


class Target(apollo.Entity):
    prefix = 'target'
    fields = {'queue': apollo.zset(str),    # queue of inactive streams
              'stream_files': {str},        # set of filenames for the stream
              'target_files': {str},        # set of filenames for the target
              'cc': str                     # which cc the target belongs to
              }


class CommandCenter(apollo.Entity):
    prefix = 'cc'
    fields = {'ip': str,        # ip of the command center
              'http_port': str  # http port
              }

ActiveStream.add_lookup('auth_token')
Target.add_lookup('owner')
apollo.relate(Target, 'streams', {Stream}, 'target')
apollo.relate(Target, 'active_streams', {ActiveStream})


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

    @property
    def deactivate_stream(self):
        return self.application.deactivate_stream


def authenticate_core(method):
    """ Decorate handlers whose authorization token maps to a specific stream
        identifier.

        If the authorization token is valid, then the stream_id is sent to the
        method as an argument.

    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            token = self.request.headers['Authorization']
        except:
            self.write(json.dumps({'error': 'missing Authorization header'}))
            return self.set_status(401)

        stream_id = ActiveStream.lookup('auth_token', token, self.db)

        if stream_id:
            return method(self, stream_id)
        else:
            self.write(json.dumps({'error': 'bad Authorization header'}))
            return self.set_status(401)

    return wrapper


class PostStreamHandler(BaseHandler):
    def post(self):
        """ Accessible by CC only.

        Add a new stream to WS. The POST method on this URI
        can only be accessed by known CCs (IP restricted)

        Request:
            {
                'target_id': target_id

                [required if target_id does not exist on ws]
                'target_files': {file1_name: file1.b64,
                                 file2_name: file2.b64,
                                 ...
                                 }

                'stream_files': {file3_name: file3.b64,
                                 file4_name: file4.b64,
                                 ...
                                 }

            }

        Response:
            {
                'stream_id' : hash
            }

        Notes: Binaries in files must be base64 encoded.

        """
        #if not CommandCenter.lookup('ip', self.request.remote_ip, self.db):
        #    return self.set_status(401)
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        target_id = content['target_id']
        stream_files = content['stream_files']
        targets_folder = self.application.targets_folder

        if not Target.exists(target_id, self.db):
            target_files = content['target_files']
            target_dir = os.path.join(targets_folder, target_id)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            target = Target.create(target_id, self.db)
            for filename, binary in target_files.items():
                target_file = os.path.join(target_dir, filename)
                with open(target_file, 'w') as handle:
                    handle.write(binary)
                target.sadd('target_files', filename)
            for filename, binary in stream_files.items():
                target.sadd('stream_files', filename)
        else:
            target = Target(target_id, self.db)
            if target.smembers('stream_files') != stream_files.keys():
                self.write(json.dumps({'error': 'inconsistent stream files'}))
                return

        stream_id = str(uuid.uuid4())
        stream_dir = os.path.join(self.application.streams_folder, stream_id)
        if not os.path.exists(stream_dir):
            os.makedirs(stream_dir)

        stream = Stream.create(stream_id, self.db)
        for filename, binary in stream_files.items():
            with open(os.path.join(stream_dir, filename), 'w') as handle:
                handle.write(binary)

        target = Target(target_id, self.db)
        target.zadd('queue', stream_id, 0)
        stream.hset('target', target)
        stream.hset('frames', 0)
        stream.hset('status', 'OK')
        stream.hset('error_count', 0)

        response = {'stream_id': stream_id}

        self.set_status(200)
        self.write(json.dumps(response))


class DeleteStreamHandler(BaseHandler):
    def put(self):
        """ Accessible by CC only. TODO: make it authenticated! So that
        this method can be called either from the CC or the WS.

        Request:
            {
                'stream_id': stream_id,
            }

        """
        stream_id = json.loads(self.request.body.decode())['stream_id']
        if not Stream.exists(stream_id, self.db):
            return self.set_status(400)
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        stream.delete()

        try:
            active_stream = ActiveStream(stream_id, self.db)
            if active_stream:
                active_stream.delete()
        except KeyError:
            pass
        shutil.rmtree(os.path.join(self.application.streams_folder, stream_id))
        target = Target(target_id, self.db)
        # manual cleanup
        target.zrem('queue', stream_id)
        if target.scard('streams') == 0:
            target.delete()
            shutil.rmtree(os.path.join(self.application.targets_folder,
                                       target_id))

        self.set_status(200)


class CoreStartHandler(BaseHandler):
    @authenticate_core
    def get(self, stream_id):
        """ The core first goes to the CC to get an authorization token. The CC
        activates a stream, and maps the authorization token to the stream.

        stream_id is passed in by the decorator from authenticate_core

        Request Header:

            Authorization - shared_token

        Reply:

            {
                'stream_files': {file1_name: file1.b64,
                                 file2_name: file2.b64,
                                 ...
                                 }
                'target_files': {file1_name: file1.b64,
                                 file2_name: file2.b64,
                                 ...
                                 }
                'stream_id': str,
                'target_id': str
            }

        We need to be extremely careful about checkpoints and frames, as
        it is important we avoid writing duplicate frames on the first
        step for the core. We use the follow scheme:

                      (0-10]                      (10-20]
                    frameset_10                 frameset_20
              ------------------------------------------------------------
              |c       core 1      |c|              core 2         |c|
              ---                  --|--                           --|--
        frame x |1 2 3 4 5 6 7 8 9 10| |11 12 13 14 15 16 17 18 19 20| |21
                ---------------------| ------------------------------- ---

        When a core fetches a state.xml, it makes sure to NOT write the
        first frame (equivalent to the frame of fetched state.xml file).

        """
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        target = Target(target_id, self.db)
        # a core should NEVER be able to get a non OK stream
        assert stream.hget('status') == 'OK'

        reply = dict()
        reply['stream_files'] = dict()
        # if target is OpenMM engine type, use the following recipe:
        base_name = 'state.xml.gz.b64'
        frame_count = stream.hget('frames')
        if frame_count == 0:
            filename = 'state.xml.gz.b64'
        else:
            filename = str(stream.hget('frames'))+'_'+filename
        file_path = os.path.join(self.application.streams_folder,
                                 stream_id, filename)
        with open(file_path, 'r') as handle:
            reply['stream_files'][base_name] = handle.read()
        # create additional methods later for gromacs/amber/etc.

        reply['target_files'] = dict()
        for filename in target.smembers('target_files'):
            file_path = os.path.join(self.application.targets_folder,
                                     target_id, filename)
            with open(file_path, 'r') as handle:
                reply['target_files'][filename] = handle.read()

        reply['stream_id'] = stream_id
        reply['target_id'] = target_id

        return self.write(json.dumps(reply))


class CoreFrameHandler(BaseHandler):
    @authenticate_core
    def put(self, stream_id):
        """ Add a new frame.

        If the core posts to this method, then the WS assumes that the frame is
        good.

        Frames are written directly to a buffer file. When a checkpoint is sent
        buffered_frames are renamed (safely) to a frameset file. renames are in
        general very fast compared to copy & moves.

        Example files on disk for a given stream:

            10_frames.xtc (0-10]
            15_frames.xtc (10-15]
            29_frames.xtc (15-29]
            39_frames.xtc (29-39] <- 39_state.xml.gz.b64 |  39
            buffer.xtc                                     | ????

        When a checkpoint is received, the following steps happen

        nframes = stream.hget('frames')+active_stream.hget('buffer_frames')
        buffer.xtc --> nframes+_frames.xtc (via os.rename())
        checkpoint.json --> nframes+_state.xml.gz.b64
        stream.hset('frames', nframes)
        39_state.xml.gz.b64 file is safely removed

        presence of k_frameset.xtc and k_state.xml.gz.b64 guarantees that the
        frames up to k are valid.

        If the WS crashes, we can directly read from the files to rebuild the
        frames value in redis.

        Request Header:

            Authorization - core_token

        Request Body:
            {
                'frame' : frame.xtc (b64 encoded)
            }

        Reply:

            200 - OK

        """
        active_stream = ActiveStream(stream_id, self.db)
        content = json.loads(self.request.body.decode())
        frame_bytes = base64.b64decode(content['frame'])
        # see if this frame has been submitted before
        frame_hash = hashlib.md5(frame_bytes).hexdigest()
        if active_stream.hget('last_frame_md5') == frame_hash:
            return self.set_status(200)
        active_stream.hset('last_frame_md5', frame_hash)
        streams_folder = self.application.streams_folder
        buffer_path = os.path.join(streams_folder, stream_id, 'buffer.xtc')
        with open(buffer_path, 'ab') as buffer_file:
            buffer_file.write(frame_bytes)
        active_stream.hincrby('buffer_frames', 1)
        return self.set_status(200)


class CoreCheckpointHandler(BaseHandler):
    @authenticate_core
    def put(self, stream_id):
        """ Add a checkpoint. A checkpoint renames the buffer into a valid set
        of frames.

        Request Header:

            Authorization - core_token

        Request Body:
            {
                [required]
                "last_frame_hash" : frame.xtc (b64 encoded)
                "checkpoint" : checkpoint.xml.tar.gz (b64 encoded)
            }

        Reply:

            200 - OK

        """
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        last_frame_md5 = content['last_frame_hash']
        stream = Stream(stream_id, self.db)
        active_stream = ActiveStream(stream_id, self.db)
        if last_frame_md5 != active_stream.hget('last_frame_md5'):
            return
        streams_folder = self.application.streams_folder
        buffer_path = os.path.join(streams_folder, stream_id, 'buffer.xtc')
        checkpoint_bytes = content['checkpoint'].encode()

        stream_frames = stream.hget('frames')
        buffer_frames = active_stream.hget('buffer_frames')
        # if buffer is empty then this checkpoint does nothing
        # (important for idempotency)
        if buffer_frames == 0:
            return self.set_status(200)
        total_frames = stream_frames + buffer_frames
        base_name = 'state.xml.gz.b64'
        # HACK: write checkpoint as a new state
        checkpoint_path = os.path.join(streams_folder, stream_id,
                                       str(total_frames)+'_'+base_name)
        with open(checkpoint_path, 'wb') as handle:
            handle.write(checkpoint_bytes)
        # rename buffer.xtc to [total_frames]_frameset.xtc
        frames_path = os.path.join(streams_folder, stream_id,
                                   str(total_frames)+'_frames.xtc')
        os.rename(buffer_path, frames_path)
        # clear the old buffer
        with open(buffer_path, 'wb'):
            pass
        # delete the old state
        old_state_path = os.path.join(streams_folder, stream_id,
                                      str(stream_frames)+'_'+base_name)
        if stream_frames > 0:
            os.remove(old_state_path)
        stream.hincrby('frames', buffer_frames)
        active_stream.hset('buffer_frames', 0)
        self.set_status(200)


class CoreStopHandler(BaseHandler):
    @authenticate_core
    def put(self, stream_id):
        """ Stop a stream from being ran by a core.

        Request Header:

            Authorization : core_token

        Request Body:
            {
                [optional]
                "error": error_message

                [optional]
                "debug_files": {file1_name: file1_bin_b64,
                                file2_name: file2_bin_b64,
                                ...
                                }
            }

        """
        stream = Stream(stream_id, self.db)
        content = json.loads(self.request.body.decode())
        if 'error' in content:
            stream.hincrby('error_count', 1)
            message = content['error']
            log_path = os.path.join(self.application.streams_folder,
                                    stream_id, 'log.txt')
            with open(log_path, 'a') as handle:
                handle.write(time.strftime("%c")+' | '+message)

        self.set_status(200)
        self.deactivate_stream(stream_id)


class DownloadHandler(BaseHandler):
    def get(self, stream_id):
        """ Download a stream

        This function concatenates the list of frames on the fly by reading
        the files and yielding chunks.

        """
        self.set_status(400)
        stream = Stream(stream_id, self.db)
        if stream:
            streams_folder = self.application.streams_folder
            stream_dir = os.path.join(streams_folder, stream_id)
            if stream.hget('frames') > 0:
                files = [f for f in os.listdir(stream_dir) if 'frames' in f]
                files = sorted(files, key=lambda k: int(k.split('_')[0]))
                buf_size = 4096
                self.set_header('Content-Type', 'application/octet-stream')
                self.set_header('Content-Disposition',
                                'attachment; filename=frames.xtc')
                for sorted_file in files:
                    filename = os.path.join(stream_dir, sorted_file)
                    with open(filename, 'rb') as f:
                        while True:
                            data = f.read(buf_size)
                            if not data:
                                break
                            self.write(data)
                self.set_status(200)
                self.finish()
                return
            else:
                self.write('')
                return self.set_status(200)
        else:
            return self.write(json.dumps({'error': 'bad stream_id'}))


class HeartbeatHandler(BaseHandler):
    @authenticate_core
    def post(self, stream_id):
        ''' Cores POST to this handler to notify the WS that it is still
            alive. WS executes a zadd initially as well'''
        try:
            content = json.loads(self.request.body.decode)
            token_id = content['shared_token']
            increment = tornado.options.options['heartbeat_increment']
            stream_id = ActiveStream.lookup('shared_token', token_id, self.db)
            self.db.zadd('heartbeats', stream_id, time.time()+increment)
            self.set_status(200)
        except KeyError:
            self.set_status(400)


class WorkServer(tornado.web.Application, common.RedisMixin):
    def _cleanup(self):
        # clear active streams (and clear buffer)
        active_streams = ActiveStream.members(self.db)
        if active_streams:
            for stream in active_streams:
                self.deactivate_stream(stream)
        ccs = CommandCenter.members(self.db)
        if ccs:
            for cc_id in ccs:
                CommandCenter.delete(cc_id, self.db)
        self.db.delete('heartbeats')

        # inform the CC gracefully that the WS is dying (ie.expire everything)

    def __init__(self,
                 ws_name,
                 redis_port,
                 url='127.0.0.1',
                 redis_pass=None,
                 ws_ext_http_port=None,
                 ccs=dict(),
                 targets_folder='targets',
                 streams_folder='streams',
                 debug=False):

        """ Initialize the WorkServer.

        """

        self.targets_folder = targets_folder
        self.streams_folder = streams_folder
        self.db = common.init_redis(redis_port, redis_pass)
        if not os.path.exists(self.targets_folder):
            os.makedirs(self.targets_folder)
        if not os.path.exists(self.streams_folder):
            os.makedirs(self.streams_folder)

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        # Notify the command centers that this workserver is starting
        for cc_name in ccs:

            headers = {
                'Authorization': ccs[cc_name]['auth']
            }

            body = {
                'name': ws_name,
                'url': url,
                'http_port': ws_ext_http_port,
                'redis_port': redis_port,
                'redis_pass': redis_pass
            }

            # use the synchronous client
            client = tornado.httpclient.HTTPClient()
            url = ccs[cc_name]['url']
            uri = 'https://'+url+'/register_ws'
            print('URI:', uri)
            try:
                rep = client.fetch(uri, method='PUT', connect_timeout=2,
                                   body=json.dumps(body), headers=headers,
                                   validate_cert=common.is_domain(url))
                if rep.code != 200:
                    print('Warning: not connect to CC '+cc_name)
            except Exception:
                print('Warning: not connect to CC '+cc_name)

        super(WorkServer, self).__init__([
            (r'/streams', PostStreamHandler),
            (r'/streams/delete', DeleteStreamHandler),
            (r'/streams/download/(.*)', DownloadHandler),
            (r'/core/start', CoreStartHandler),
            (r'/core/frame', CoreFrameHandler),
            (r'/core/checkpoint', CoreCheckpointHandler),
            (r'/core/stop', CoreStopHandler),
        ], debug=debug)

    def initialize_pulse(self):
        frequency = tornado.options.options['pulse_frequency_in_ms']
        self.pulse = tornado.ioloop.PeriodicCallback(self.check_heartbeats,
                                                     frequency)
        self.pulse.start()

    def check_heartbeats(self):
        for dead_stream in self.db.zrangebyscore('heartbeats', 0, time.time()):
            self.deactivate_stream(dead_stream)

    @staticmethod
    def activate_stream(target_id, token, db):
        """ Activate and return the highest priority stream belonging to target
        target_id. This is called directly by the CC to start a stream.

        """
        target = Target(target_id, db)
        stream_id = target.zrange('queue', 0, 0)[0]
        if stream_id:
            assert target.zremrangebyrank('queue', 0, 0) == 1
            active_stream = ActiveStream.create(stream_id, db)
            active_stream.hset('buffer_frames', 0)
            active_stream.hset('auth_token', token)
            active_stream.hset('steps', 0)
            active_stream.hset('start_time', time.time())
            increment = tornado.options.options['heartbeat_increment']
            db.zadd('heartbeats', stream_id, time.time() + increment)

        return stream_id

    # deactivates the stream on the workserver
    def deactivate_stream(self, stream_id):
        ActiveStream(stream_id, self.db).delete()
        self.db.zrem('heartbeats', stream_id)
        buffer_path = os.path.join(self.streams_folder,
                                   stream_id, 'buffer.xtc')
        if os.path.exists(buffer_path):
            with open(buffer_path, 'w'):
                pass
        # push this stream back into queue
        stream = Stream(stream_id, self.db)
        frames_completed = stream.hget('frames')
        target = Target(stream.hget('target'), self.db)
        # TODO: to a check to make sure the stream's status is OK. Check the
        # error count, if it's too high, then the stream is stopped
        target.zadd('queue', stream_id, frames_completed)

    def push_stream_to_cc(stream_id):
        pass


#########################
# Defined once globally #
#########################

tornado.options.define('heartbeat_increment', default=900, type=int)
tornado.options.define('pulse_frequency_in_ms', default=3000, type=int)


def start():

    #######################
    # WS Specific Options #
    #######################
    tornado.options.define('name', type=str)
    tornado.options.define('redis_port', type=int)
    tornado.options.define('redis_pass', type=str)
    tornado.options.define('url', type=str)
    tornado.options.define('internal_http_port', type=int)
    tornado.options.define('external_http_port', type=int)
    tornado.options.define('command_centers', type=dict)
    tornado.options.parse_config_file('ws.conf')

    options = tornado.options.options

    ws_name = options.name
    redis_port = options.redis_port
    redis_pass = options.redis_pass
    url = options.url
    internal_http_port = options.internal_http_port
    external_http_port = options.external_http_port
    command_centers = options.command_centers

    ws_instance = WorkServer(ws_name=ws_name,
                             url=url,
                             redis_port=redis_port,
                             redis_pass=redis_pass,
                             ccs=command_centers,
                             ws_ext_http_port=external_http_port)

    ws_server = tornado.httpserver.HTTPServer(ws_instance, ssl_options={
        'certfile': 'certs/ws.crt', 'keyfile': 'certs/ws.key'})

    ws_server.bind(internal_http_port)
    ws_server.start(0)
    ws_instance.initialize_pulse()
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    start()
