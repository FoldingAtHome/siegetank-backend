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

import base64
import gzip
import functools

import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httputil
import tornado.httpserver
import tornado.httpclient
import tornado.options
import tornado.process

from server.common import RedisMixin, init_redis, is_domain
from server.apollo import Entity, zset, relate

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

# POST x.com/streams  - add a new stream
# PUT x.com/streams/delete  - delete a stream
# GET x.com/streams/stream_id  - download a stream
# POST x.com/streams/activate - activate a stream

##################
# CORE Interface #
##################

# GET x.com/core/start  - start a stream (given an auth token)
# PUT x.com/core/frame  - add a frame to a stream (idempotent)
# PUT x.com/core/stop  - stop a stream
# PUT x.com/core/checkpoint  - send a checkpoint file corresponding
#                              to the last frame received
# POST x.com/core/heartbeat  - send a heartbeat

####################
# PUBLIC Interface #
####################

# GET x.com/targets/streams/:target_id
# GET x.com/active_streams - get all the active streams

# In general, we should try and use GETs/PUTs whenever possible. Idempotency
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

# When the WS dies, we can recreate the entire redis database using data from
# the disk! This implies we don't actually need to save an rdb.


class Stream(Entity):
    prefix = 'stream'
    fields = {'frames': int,            # total number of frames completed
              'status': str,            # 'OK', 'DISABLED'
              'error_count': int,       # number of consecutive errors
              }


class ActiveStream(Entity):
    prefix = 'active_stream'
    fields = {'total_frames': int,  # total frames completed.
              'buffer_frames': int,  # number of frames in buffer.xtc
              'auth_token': str,  # used by core to send requests
              'donor': str,  # the donor assigned ? support lookup?
              'steps': int,  # number of steps completed
              'start_time': float,  # time we started at
              'frame_hash': str,  # md5sum of the received frame
              'buffer_files': {str},  # set of frame files sent
              }


class Target(Entity):
    prefix = 'target'
    fields = {'queue': zset(str),  # queue of inactive streams
              'stream_files': {str},  # set of filenames for the stream
              'target_files': {str},  # set of filenames for the target
              }


class CommandCenter(Entity):
    prefix = 'cc'
    fields = {'ip': str,        # ip of the command center
              'http_port': str  # http port
              }

#Target.add_lookup('owner', injective=False)
ActiveStream.add_lookup('auth_token')
ActiveStream.add_lookup('donor', injective=False)
relate(Target, 'streams', {Stream}, 'target')
relate(Target, 'active_streams', {ActiveStream})


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


class TargetStreamsHandler(BaseHandler):
    def get(self, target_id):
        """ Get a list of streams and their simple status
        Reply:
            {
                'stream_id': {
                    'status': OK,
                    'frames': 253,
                }
                ...
            }

        """
        self.set_status(400)
        target = Target(target_id, self.db)
        body = {}
        for stream_id in target.smembers('streams'):
            stream = Stream(stream_id, self.db)
            body[stream_id] = {}
            body[stream_id]['status'] = stream.hget('status')
            body[stream_id]['frames'] = stream.hget('frames')
        self.set_status(200)
        self.write(json.dumps(body))


class ActivateStreamHandler(BaseHandler):
    def post(self):
        """ Activate and return the highest priority stream on a target.
            If no streams can be activated, then return status code 400

            Request:
                {
                    "target_id": target_id

                    [optional]
                    "donor_id": donor_id
                }

            Reply:
                {
                    "token": token
                }

        """

        self.set_status(400)
        content = json.loads(self.request.body.decode())
        target_id = content["target_id"]
        target = Target(target_id, self.db)
        stream_id = target.zrevpop('queue')
        token = str(uuid.uuid4())
        if stream_id:
            active_stream = ActiveStream.create(stream_id, self.db)
            active_stream.hset('buffer_frames', 0)
            active_stream.hset('total_frames', 0)
            active_stream.hset('auth_token', token)
            active_stream.hset('steps', 0)
            active_stream.hset('start_time', time.time())
            if 'donor_id' in content:
                donor_id = content['donor_id']
                active_stream.hset('donor', donor_id)
            increment = tornado.options.options['heartbeat_increment']
            self.db.zadd('heartbeats', stream_id, time.time() + increment)

            reply = {}
            reply["token"] = token
            self.set_status(200)
            return self.write(json.dumps(reply))
        else:
            return


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

        # TODO: stream creation needs to be pipelined and atomic

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

        Request Header:

            Authorization - shared_token

        Reply:

            {
                'frame': frame_count,
                'stream_id': uuid,
                'target_id': uuid,
                'stream_files': {file1_name: file1.b64,
                                 file2_name: file2.b64,
                                 ...
                                 }
                'target_files': {file1_name: file1.b64,
                                 file2_name: file2.b64,
                                 ...
                                 }
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
        filename = 'state.xml.gz.b64'
        if frame_count > 0:
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
        """ Append a new frame.

        If the core posts to this method, then the WS assumes that the frame is
        good. The data received is stored in a buffer until a checkpoint is
        received.

        There are four interval :

        fwi = frame_write_interval (PG Controlled)
        fsi = frame_send_interval (Core Controlled)
        cwi = checkpoint_write_interval (Core Controlled)
        csi = checkpoint_send_interval (Donor Controlled)

        Where: fwi < k*fsi = cwi < j*csi | k, j are integers

        When a set of frames is sent, the core is guaranteed to write a
        corresponding checkpoint, so that the next checkpoint received is
        guaranteed to correspond to the head of the buffered files.

        OpenMM:

        fwi = fsi = cwi = 50000
        sci = 2x per day

        Terachem:

        fwi = 2
        fsi = cwf = 100
        sci = 2x per day

        Request Header:

            Authorization - core_token

        Request Body:
            {

                [optional]
                "frames": 25,  # number of frames this contains

                "files" : {
                    "filename.xtc.b64": file.b64,
                    "frames.xtc.b64": file.b64,
                    "coords.xyz.b64": file.b64,
                    "log.txt.gz.b64": file.gz.b64
                }


            }

        If the filename ends in b64, it is b64 decoded. If the next suffix ends
        in gz, it is gunzipped. Afterwards, the written to disk with the name
        buffer_[filename], with the b64/gz suffixes stripped.

        Reply:

            200 - OK

        """
        self.set_status(400)
        active_stream = ActiveStream(stream_id, self.db)
        frame_hash = hashlib.md5(self.request.body).hexdigest()
        if active_stream.hget('frame_hash') == frame_hash:
            return self.set_status(200)
        active_stream.hset('frame_hash', frame_hash)
        content = json.loads(self.request.body.decode())
        if 'frames' in content:
            frame_count = content['frames']
            if frame_count < 1:
                self.set_status(400)
                return self.write(json.dumps({'error': 'frames < 1'}))
        else:
            frame_count = 1
        files = content['files']
        streams_folder = self.application.streams_folder

        # empty the set
        active_stream.sremall('buffer_files')
        for filename, filedata in files.items():
            filedata = filedata.encode()
            f_root, f_ext = os.path.splitext(filename)
            if f_ext == '.b64':
                filename = f_root
                filedata = base64.b64decode(filedata)
                f_root, f_ext = os.path.splitext(filename)
                if f_ext == '.gz':
                    filename = f_root
                    filedata = gzip.decompress(filedata)
            buffer_filename = os.path.join(streams_folder, stream_id,
                                           'buffer_'+filename)
            with open(buffer_filename, 'ab') as buffer_handle:
                buffer_handle.write(filedata)
            active_stream.sadd('buffer_files', filename)
        active_stream.hincrby('buffer_frames', frame_count)

        return self.set_status(200)


class CoreCheckpointHandler(BaseHandler):
    @authenticate_core
    def put(self, stream_id):
        """ Add a checkpoint. Invoking this handler renames the buffered files
        into a valid filenames prefixed with the corresponding frame count.

        Suppose we have the following files:

        active_stream.hset('buffer_frames', 0)

        total_frames = stream.hget('frames') +
                       active_stream.hget('buffer_frames')
                     = 9

        5_state.xml.gz.b64
        5_frames.xtc            buffer_frames.xtc
        5_log.txt               buffer_log.txt

        1. 1) write checkpoint: 9_state.xml.gz.b64 is written
        2. 2) rename buffered files: buffer_frames.xtc -> 9_frames.xtc
        3. 3) delete old checkpoint: 5_state.xml.gz.b64 is deleted

        Request Header:

            Authorization: core_token

        Request Body:
            {
                "files": {
                    "state.xml.gz.b64" : state.xml.gz.b64
                }
            }

        Reply:

            200 - OK

        """
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        stream = Stream(stream_id, self.db)
        active_stream = ActiveStream(stream_id, self.db)
        stream_frames = stream.hget('frames')
        buffer_frames = active_stream.hget('buffer_frames')

        # important check for idempotency
        if buffer_frames == 0:
            return self.set_status(200)
        streams_folder = self.application.streams_folder
        buffers_folder = os.path.join(streams_folder, stream_id)

        # 1) write checkpoint
        for filename, bytes in content['files'].items():
            checkpoint_bytes = content['files'][filename].encode()
            total_frames = stream_frames + buffer_frames
            checkpoint_path = os.path.join(streams_folder, stream_id,
                                           str(total_frames)+'_'+filename)
            with open(checkpoint_path, 'wb') as handle:
                handle.write(checkpoint_bytes)

        # 2) rename buffered files
        for filename in active_stream.smembers('buffer_files'):
            dst = os.path.join(buffers_folder, str(total_frames)+'_'+filename)
            src = os.path.join(buffers_folder, 'buffer_'+filename)
            os.rename(src, dst)

        # 3) delete old checkpoint
        for filename, bytes in content['files'].items():
            src = os.path.join(streams_folder, stream_id,
                               str(stream_frames)+'_'+filename)
            if stream_frames > 0:
                os.remove(src)

        stream.hincrby('frames', buffer_frames)
        active_stream.hincrby('total_frames', buffer_frames)
        active_stream.hset('buffer_frames', 0)
        self.set_status(200)


class CoreStopHandler(BaseHandler):
    @authenticate_core
    def put(self, stream_id):
        """ Stop a stream from being ran by a core.

        To do: add marker denoting if stream should be finished

        Request Header:

            Authorization: core_token

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


class ActiveStreamsHandler(BaseHandler):
    def get(self):
        """ Display statistics about active streams on the workserver. The list
        of streams are not sorted, so the client must sort them.

        Reply:

        {
            target_id: {
                    stream_id_1: {
                        donor_id: None,
                        start_time: time,
                        total_frames: frames
                    }
                    ...
            }
            ...
        }

        """
        reply = dict()
        for target in Target.members(self.db):
            # Hardcoded
            streams_key = Target.prefix+':'+target+':streams'
            good_streams = self.db.sinter('active_streams', streams_key)
            if len(good_streams) > 0:
                reply[target] = dict()
            for stream_id in good_streams:
                reply[target][stream_id] = dict()
                active_stream = ActiveStream(stream_id, self.db)
                donor = active_stream.hget('donor')
                start_time = active_stream.hget('start_time')
                total_frames = active_stream.hget('total_frames')
                reply[target][stream_id]['donor'] = donor
                reply[target][stream_id]['start_time'] = start_time
                reply[target][stream_id]['total_frames'] = total_frames
        self.write(json.dumps(reply))

class DownloadHandler(BaseHandler):
    def get(self, stream_id, filename):
        """ Download the file filename from stream streamid

        This function concatenates the list of frames on the fly by reading
        the files and yielding chunks.

        """
        self.set_status(400)
        stream = Stream(stream_id, self.db)
        if stream:
            streams_folder = self.application.streams_folder
            stream_dir = os.path.join(streams_folder, stream_id)
            if stream.hget('frames') > 0:
                files = [f for f in os.listdir(stream_dir)
                         if (filename in f and 'buffer_' not in f)]
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


class CoreHeartbeatHandler(BaseHandler):
    @authenticate_core
    def post(self, stream_id):
        ''' Cores POST to this handler to notify the WS that it is still
        alive.

        Request Header:

            Authorization: token

        Request:
            {}

        '''
        increment = tornado.options.options['heartbeat_increment']
        self.db.zadd('heartbeats', stream_id, time.time()+increment)
        self.set_status(200)


class WorkServer(tornado.web.Application, RedisMixin):
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
                 debug=False,
                 appendonly=False):

        """ Initialize the WorkServer.

        """

        self.targets_folder = targets_folder
        self.streams_folder = streams_folder
        self.db = init_redis(redis_port, redis_pass,
                             appendonly=appendonly,
                             appendfilename='aof_'+ws_name)
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
            try:
                rep = client.fetch(uri, method='PUT', connect_timeout=2,
                                   body=json.dumps(body), headers=headers,
                                   validate_cert=is_domain(url))
                if rep.code != 200:
                    print('Warning: not connect to CC '+cc_name)
            except Exception as e:
                print(e.code)
                print('Warning: not connect to CC '+cc_name)

        super(WorkServer, self).__init__([
            (r'/active_streams', ActiveStreamsHandler),
            (r'/streams/activate', ActivateStreamHandler),
            (r'/streams', PostStreamHandler),
            (r'/streams/delete', DeleteStreamHandler),
            (r'/streams/(.*)/(.*)', DownloadHandler),
            (r'/targets/streams/(.*)', TargetStreamsHandler),
            (r'/core/start', CoreStartHandler),
            (r'/core/frame', CoreFrameHandler),
            (r'/core/checkpoint', CoreCheckpointHandler),
            (r'/core/stop', CoreStopHandler),
            (r'/core/heartbeat', CoreHeartbeatHandler)
        ], debug=debug)

    def initialize_pulse(self):
        # check for heartbeats only on the 0th process.
        if tornado.process.task_id() == 0:
            frequency = tornado.options.options['pulse_frequency_in_ms']
            self.pulse = tornado.ioloop.PeriodicCallback(self.check_heartbeats,
                                                         frequency)
            self.pulse.start()

    # we really don't need all 8 processes to constantly check this...
    def check_heartbeats(self):
        for dead_stream in self.db.zrangebyscore('heartbeats', 0, time.time()):
            self.deactivate_stream(dead_stream)

    # deactivates the stream on the workserver
    def deactivate_stream(self, stream_id):
        active_stream = ActiveStream(stream_id, self.db)

        self.db.zrem('heartbeats', stream_id)
        buffer_files = active_stream.smembers('buffer_files')
        for fname in buffer_files:
            fname = 'buffer_'+fname
            buffer_path = os.path.join(self.streams_folder, stream_id, fname)
            if os.path.exists(buffer_path):
                os.remove(buffer_path)

        active_stream.delete()
        # push this stream back into queue
        stream = Stream(stream_id, self.db)
        frames_completed = stream.hget('frames')
        target = Target(stream.hget('target'), self.db)
        # TODO: to a check to make sure the stream's status is OK. Check the
        # error count, if it's too high, then the stream is stopped
        target.zadd('queue', stream_id, frames_completed)

#########################
# Defined once globally #
#########################

tornado.options.define('heartbeat_increment', default=900, type=int)
tornado.options.define('pulse_frequency_in_ms', default=3000, type=int)


def start(*args, **kwargs):

    #######################
    # WS Specific Options #
    #######################
    tornado.options.define('name', type=str)
    tornado.options.define('redis_port', type=int)
    tornado.options.define('redis_pass', type=str)
    tornado.options.define('url', type=str)
    tornado.options.define('internal_http_port', type=int)
    tornado.options.define('external_http_port', type=int)
    tornado.options.define('ssl_certfile', type=str)
    tornado.options.define('ssl_key', type=str)
    tornado.options.define('ssl_ca_certs', type=str)
    tornado.options.define('command_centers', type=dict)
    conf_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', 'ws.conf')
    tornado.options.define('config_file', default=conf_path, type=str)

    tornado.options.parse_command_line()
    options = tornado.options.options
    tornado.options.parse_config_file(options.config_file)
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
                             ws_ext_http_port=external_http_port,
                             appendonly=True)

    cert_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', options.ssl_certfile)
    key_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            '..', options.ssl_key)
    ca_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           '..', options.ssl_ca_certs)

    ws_server = tornado.httpserver.HTTPServer(ws_instance, ssl_options={
        'certfile': cert_path, 'keyfile': key_path, 'ca_certs': ca_path})

    ws_server.bind(internal_http_port)
    ws_server.start(0)
    ws_instance.initialize_pulse()
    tornado.ioloop.IOLoop.instance().start()
