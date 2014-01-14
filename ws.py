import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httputil
import tornado.httpserver
import tornado.httpclient
import io
import tarfile
import signal
import uuid
import os
import json
import sys
import time
import traceback
import shutil
import configparser
import hashlib
import common
import apollo
import base64

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

################
# PG Interface #
################

# POST x.com/streams              - add a new stream
# PUT x.com/streams/delete        - delete a stream
# GET x.com/streams/stream_id     - download a stream
# POST x.com/targets              - add a new target

##################
# CORE Interface #
##################

# GET x.com/core/start            - start a stream (given an auth token)
# PUT x.com/core/frame            - add a frame to a stream (idempotent)
# PUT x.com/core/stop             - stop a stream
# POST x.com/core/heartbeat       - send a heartbeat

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
              'steps_per_frame': int,   # number of steps per frame
              'files': {str},           # set of filenames: fn1 fn2 fn3
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
    fields = {'queue': apollo.zset(str),  # queue of inactive streams
              'files': {str},               # list of filenames,
              'cc': str                   # which cc the target belongs to
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


# General WS config
# Block ALL ports except port 80
# Redis port is only available to CC's IP on the intranet
class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

    @property
    def deactivate_stream(self):
        return self.application.deactivate_stream


class FrameHandler(BaseHandler):
    def initialize(self, max_error_count=10):
        self._max_error_count = max_error_count

    def post(self):
        ''' Post a frame to a stream

        Request Header:

            Authorization - core_token

        Request Body:
        {
            [required]
            'status' : ['OK' | 'Error'],

            [required if status == 'OK]
            'frame' : frame.xtc (b64 encoded)

            [required if status == 'Error']
            'message' : error_message

            [optional]
            'checkpoint' : checkpoint.xtc (b64 encoded)
            }

        '''

        try:
            token = self.request.headers['shared_token']
            stream_id = ActiveStream.lookup('shared_token', token, self.db)
            if not stream_id:
                self.set_status(400)
                return
            stream = Stream.instance(stream_id, self.db)
            active_stream = ActiveStream.instance(stream_id, self.db)
            if stream['status'] != 'OK':
                self.set_status(400)
                return self.write('Stream status not OK')
            if 'error_code' in self.request.headers:
                self.set_status(400)
                error_count = stream.hincrby('error_count',1)
                #if error_count > self._max_error_count:
                #   set status to bad
                self.deactivate_stream(stream_id)
                return self.write('Bad state.. terminating')
            stream['error_count'] = 0
            tar_string = io.BytesIO(self.request.body)
            with tarfile.open(mode='r', fileobj=tar_string) as tarball:
                # Extract the frame
                frame_member = tarball.getmember('frame.xtc')
                frame_binary = tarball.extractfile(frame_member).read()
                buffer_path = os.path.join('streams',stream_id,'buffer.xtc')
                with open(buffer_path,'ab') as buffer_file:
                    buffer_file.write(frame_binary)
                # Increment buffer frames by 1
                active_stream.hincrby('buffer_frames',1)
                # TODO: Check to make sure the frame is valid 
                # valid in both md5 hash integrity and xtc header integrity
                # make sure time step has increased?

                # See if checkpoint state is present, if so, the buffer.xtc is
                # appended to the frames.xtc
                try:
                    chkpt_member = tarball.getmember('state.xml.gz')
                    state        = tarball.extractfile(chkpt_member).read()  
                    state_path   = os.path.join('streams',
                                                stream_id,'state.xml.gz')
                    with open(state_path,'wb') as chkpt_file:
                        chkpt_file.write(state)
                    frames_path = os.path.join('streams', stream_id, 
                                               'frames.xtc')
                    with open(buffer_path,'rb') as src:
                        with open(frames_path,'ab') as dest:
                            while True:
                                chars = src.read(4096)
                                if not chars:
                                    break
                                dest.write(chars)
                    # this need not be done atomically since no other client 
                    # will modify the active_stream key except this ws
                    stream.hincrby('frames',active_stream['buffer_frames'])
                    active_stream['buffer_frames'] = 0
                    # clear the buffer
                    with open(buffer_path,'w') as buffer_file:
                        pass
                except KeyError as e:
                    pass
        except KeyError as e:
            print(repr(e))
            ex_type, ex, tb = sys.exc_info()
            traceback.print_tb(tb)
            self.set_status(400)
            return self.write('Bad Request')


class PostStreamHandler(BaseHandler):
    def post(self):
        ''' Accessible by CC only.

            Add a new stream to WS. The POST method on this URI
            can only be accessed by known CCs (IP restricted)

            Request:
                {
                    'target_id': target_id

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

        '''
        #if not CommandCenter.lookup('ip', self.request.remote_ip, self.db):
        #    return self.set_status(401)
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        target_id = content['target_id']
        stream_files = content['stream_files']

        if not Target.exists(target_id, self.db):
            target_files = content['target_files']
            target_dir = os.path.join('targets', target_id)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            target = Target.create(target_id, self.db)
            for filename, binary in target_files.items():
                target_file = os.path.join(target_dir, filename)
                with open(target_file, 'w') as handle:
                    handle.write(binary)
                target.sadd('files', filename)

        stream_id = str(uuid.uuid4())
        stream_dir = os.path.join('streams', stream_id)
        if not os.path.exists(stream_dir):
            os.makedirs(stream_dir)

        stream = Stream.create(stream_id, self.db)
        for filename, binary in stream_files.items():
            with open(os.path.join(stream_dir, filename), 'w') as handle:
                handle.write(binary)
            stream.sadd('files', filename)

        target = Target(target_id, self.db)
        target.zadd('queue', stream_id, 0)

        stream.hset('status', 'OK')
        stream.hset('error_count', 0)
        stream.hset('target', target)

        response = {'stream_id': stream_id}

        self.set_status(200)
        self.write(json.dumps(response))


class DeleteStreamHandler(BaseHandler):
    def put(self):
        """ Accessible by CC only.

        Request
        {
            'id': stream_id
        }

        """
        stream_id = json.loads(self.request.body.decode())['id']
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
        shutil.rmtree(os.path.join('streams', stream_id))

        target = Target(target_id, self.db)
        if target.scard('streams') == 0:
            target.delete()
            shutil.rmtree(os.path.join('targets', target_id))

        self.set_status(200)

    # def get(self):
    #     ''' PRIVATE - Download a stream.
    #         The CC first creates a token given to the Core for identification
    #         The token and WS's IP is then sent back to the ST interface
    #         Parameters:
    #         download_token: download_token automatically maps to the right
    #                         stream_id
    #         RESPONDS with the appropriate frames.

    #         TODO: Record the file size
    #     '''
    #     self.set_status(400)
    #     try:
    #         token = self.request.headers['download_token']
    #         stream_id = Stream.lookup('download_token',token,self.db)
    #         if stream_id:
    #             filename = os.path.join('streams',stream_id,'frames.xtc')
    #             buf_size = 4096
    #             self.set_header('Content-Type', 'application/octet-stream')
    #             self.set_header('Content-Disposition',
    #                             'attachment; filename=' + filename)
    #             with open(filename, 'r') as f:
    #                 while True:
    #                     data = f.read(buf_size)
    #                     if not data:
    #                         break
    #                     self.write(data)
    #             self.finish()
    #         else:
    #             self.set_status(400)
    #     except Exception as e:
    #         print(repr(e))


class CoreStartHandler(BaseHandler):
    def get(self):
        ''' The core first goes to the CC to get an authorization token. The CC
        activates a stream, and maps the authorization token to the stream.

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
                'steps_per_frame': int,
                'stream_id': str,
                'target_id': str
            }

        We need to be extremely careful about checkpoints and frames, as
        it is important we avoid writing duplicate frames on the first
        step for the core. We use the follow scheme:

              ------------------------------------------------------------
              |c       core 1      |c|              core 2           |c|
              ---                  --|--                             -----
        frame x |1 2 3 4 5 6 7 8 9 10| |11 12 13 14 15 16 17 18 19 20| |21
                ---------------------| ------------------------------- ---

        When a core fetches a checkpoint, it makes sure to NOT write the
        first frame (equivalent to the frame of fetched state.xml file).
        On every subsequent checkpoint, both the frame and the checkpoint
        are sent back to the workserver.

        '''
        shared_token = self.request.headers['Authorization']
        stream_id = ActiveStream.lookup('auth_token', shared_token, self.db)
        if stream_id is None:
            self.set_status(401)
            return self.write('Unknown token')
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        target = Target(target_id, self.db)
        # a core should NEVER be able to get a non OK stream
        assert stream.hget('status') == 'OK'

        reply = dict()

        reply['stream_files'] = dict()
        for filename in stream.smembers('files'):
            file_path = os.path.join('streams', stream_id, filename)
            with open(file_path, 'r') as handle:
                reply['stream_files'][filename] = handle.read()

        reply['target_files'] = dict()
        for filename in target.smembers('files'):
            file_path = os.path.join('targets', target_id, filename)
            with open(file_path, 'r') as handle:
                reply['target_files'][filename] = handle.read()

        reply['stream_id'] = stream_id
        reply['target_id'] = target_id

        return self.write(json.dumps(reply))


class CoreFrameHandler(BaseHandler):
    def put(self):
        """ Add a new frame. If the core posts to this method, then the WS
        assumes that the frame is good. NaNs, and other bad things are sent
        the /core/stop URI

        Request Header:

            Authorization - core_token

        Request Body:
            {
                [required]
                'frame' : frame.xtc (b64 encoded)

                [optional]
                'checkpoint' : checkpoint.xtc (b64 encoded)
            }

        Reply:

            200 - OK

        """

        token = self.request.headers['Authorization']
        stream_id = ActiveStream.lookup('auth_token', token, self.db)
        if not stream_id:
            return self.set_status(400)
        stream = Stream(stream_id, self.db)
        active_stream = ActiveStream(stream_id, self.db)
        content = json.loads(self.request.body.decode())

        frame_bytes = base64.b64decode(content['frame'])
        # see if this frame has been submitted before
        frame_hash = hashlib.md5(frame_bytes).hexdigest()
        if active_stream.hget('last_frame_md5') == frame_hash:
            return self.set_status(200)
        active_stream.hset('last_frame_md5', frame_hash)
        buffer_path = os.path.join('streams', stream_id, 'buffer.xtc')
        with open(buffer_path, 'ab') as buffer_file:
            buffer_file.write(frame_bytes)
        buffer_frames_count = active_stream.hincrby('buffer_frames', 1)
        if 'checkpoint' in content:
            checkpoint_bytes = base64.b64decode(content['checkpoint'])
            # hard-coded checkpoint name to overwrite old state
            checkpoint_path = os.path.join('streams', stream_id,
                                           'state.xml.gz.b64')
            with open(checkpoint_path, 'wb') as handle:
                handle.write(checkpoint_bytes)
            # flush buffer.xtc to frames.xtc
            frames_path = os.path.join('streams', stream_id, 'frames.xtc')
            with open(buffer_path, 'rb') as src:
                with open(frames_path, 'ab') as dest:
                    while True:
                        chars = src.read(4096)
                        if not chars:
                            break
                        dest.write(chars)
            with open(buffer_path, 'wb'):
                pass
            stream.hincrby('frames', buffer_frames_count)
            active_stream.hset('buffer_frames', 0)
        # elif content['status'] == 'Error':
        #     stream.hincrby('error_count', 1)
        #     active_stream.hset('buffer_frames', 0)
        #     message = content['message']
        #     log_path = os.path.join('streams', stream_id, 'log.txt')
        #     with open(log_path, 'a') as handle:
        #         handle.write(time.strftime("%c")+' | '+message)
        #     #self.deactivate_stream()

        return self.set_status(200)


class CoreStopHandler(BaseHandler):
    def put(self):
        """ Stop a stream from being ran by a core.

        Request Header:

        Authorization - core_token

        Request Body:
            {
                [optional]
                'error': error_message

                [optional]
                'debug_files': {file1_name: file1_bin_b64,
                                file2_name: file2_bin_b64,
                                ...
                                }
            }

        """
        token = self.request.headers['Authorization']
        stream_id = ActiveStream.lookup('auth_token', token, self.db)
        if not stream_id:
            return self.set_status(400)
        stream = Stream(stream_id, self.db)
        content = json.loads(self.request.body.decode())

        if 'error' in content:
            stream.hincrby('error_count', 1)
            message = content['error']
            log_path = os.path.join('streams', stream_id, 'log.txt')
            with open(log_path, 'a') as handle:
                handle.write(time.strftime("%c")+' | '+message)

        self.set_status(200)
        self.deactivate_stream(stream_id)


class HeartbeatHandler(BaseHandler):
    def initialize(self, increment=30*60):
        ''' Each heartbeat received by the core increments the timer by
            increment amount. Defaults to once every 30 minutes '''
        self._increment = increment

    def get(self):
        self.set_status(200)
        return self.write('OK')

    def post(self):
        ''' Cores POST to this handler to notify the WS that it is still 
            alive. WS executes a zadd initially as well'''
        try:
            content = json.loads(self.request.body.decode)
            token_id = content['shared_token']
            stream_id = ActiveStream.lookup('shared_token',token_id,self.db)
            self.db.zadd('heartbeats', stream_id, time.time()+self._increment)
            self.set_status(200)
        except KeyError:
            self.set_status(400)


#def deactivate_stream(stream_id, db):
#    ''' Deactivate and add stream back to the queue for processing  '''
#    ActiveStream(stream_id, db).delete()
#    self.assertEqual()


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
                 redis_pass=None,
                 ccs=None,
                 increment=600):

        self.db = common.init_redis(redis_port, redis_pass)
        if not os.path.exists('streams'):
            os.makedirs('streams')
        if not os.path.exists('targets'):
            os.makedirs('targets')

        #self._cleanup()

        # ccs is a list of tuples, where
        # 0th-index is name
        # 1st-index is ip
        # 2nd-index is port
        # if ccs:
        #     for cc in ccs:
        #         cc_name = cc[0]
        #         cc_ip = cc[1]
        #         cc_port = cc[2]
        #         cc_instance = CommandCenter.create(cc_name,self.db)
        #         cc_instance['ip'] = cc_ip
        #         cc_instance['http_port'] = cc_port
        # else:
        #     print('WARNING: No CCs were specified for this WS')

        # check_stream_freq_in_ms = 60000
        # pcb = tornado.ioloop.PeriodicCallback(self.check_heartbeats,
        #         check_stream_freq_in_ms,tornado.ioloop.IOLoop.instance())
        # pcb.start()

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        super(WorkServer, self).__init__([
            #(r'/frame', FrameHandler),
            (r'/streams', PostStreamHandler),
            (r'/streams/delete', DeleteStreamHandler),
            (r'/core/start', CoreStartHandler),
            (r'/core/frame', CoreFrameHandler),
            (r'/core/stop', CoreStopHandler),
            #(r'/heartbeat', HeartbeatHandler, dict(increment=increment))
        ])

    def shutdown(self, signal_number=None, stack_frame=None):
        self.shutdown_redis()
        print('shutting down tornado...')
        tornado.ioloop.IOLoop.instance().stop()
        sys.exit(0)

    def check_heartbeats(self):
        dead_streams = self.db.zrangebyscore('heartbeats', 0, time.time())
        if dead_streams:
            for dead_stream in dead_streams:
                self.deactivate_stream(dead_stream)

    @staticmethod
    def activate_stream(target_id, token, db, increment):
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
            db.zadd('heartbeats', stream_id, time.time() + increment)

        return stream_id

    # clears the buffer so it must be executed on WS side.
    def deactivate_stream(self, stream_id):
        ActiveStream(stream_id, self.db).delete()
        self.db.zrem('heartbeats', stream_id)
        buffer_path = os.path.join('streams', stream_id, 'buffer.xtc')
        if os.path.exists(buffer_path):
            with open(buffer_path, 'w'):
                pass
        # push this stream back into queue
        stream = Stream(stream_id, self.db)
        frames_completed = stream.hget('frames')
        if frames_completed is None:
            frames_completed = 0
        target = Target(stream.hget('target'), self.db)
        target.zadd('queue', stream_id, frames_completed)

    def push_stream_to_cc(stream_id):
        pass

def verifyRegistration(resp):
    if resp.code != 200:
        raise ValueError('Did not register successfully with all CCs')

def start():
    config_file = 'ws_conf'
    Config = ConfigParser.ConfigParser() 
    Config.read(config_file)

    ws_name       = Config.get('WS','name')
    ws_redis_port = Config.getint('WS','redis_port')
    ws_redis_pass = Config.get('WS','redis_pass')
    int_http_port = Config.getint('WS','int_http_port')
    ext_http_port = Config.getint('WS','ext_http_port')

    cc_str        = Config.get('WS','cc_names').split(',')
    ccs = []
    for cc in cc_str:
        cc_ip   = Config.get(cc,'ip')
        cc_port = Config.getint(cc,'http_port')
        ccs.append((cc,cc_ip,cc_port))

    ws_instance = WorkServer(ws_name,ws_redis_port,ws_redis_pass,ccs)
    ws_server = tornado.httpserver.HTTPServer(ws_instance,ssl_options={
                    'certfile' : 'ws.crt','keyfile'  : 'ws.key'})
    #ws_server = tornado.httpserver.HTTPServer(ws_instance)
    ws_server.listen(int_http_port)

    sync_client = tornado.httpclient.HTTPClient()
    for cc in cc_str:
        ip   = Config.get(cc,'ip')
        auth_port = Config.get(cc,'auth_port')
        auth_pass = Config.get(cc,'auth_pass')
        msg = {
            'name'       : ws_name,
            'http_port'  : ext_http_port,
            'redis_port' : ws_redis_port,
            'redis_pass' : ws_redis_pass,
            'auth_pass'  : auth_pass
        }
        uri = "http://"+ip+":"+auth_port+'/register_ws'
        try:
            resp = sync_client.fetch(uri,method='POST',body=json.dumps(msg))
        except tornado.httpclient.HTTPError as e: 
            print(repr(e))
            print('Could not connect to CC')
            ws_instance.shutdown()
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    start()
    '''
    application = tornado.web.Application()

    # inform the CCs that the WS is now online and ready for work
    ws_uuid = 'firebat'
    try:
        for server_address, secret_key in CCs.iteritems():
            payload = {'cc_key' : secret_key, 'ws_id' : ws_uuid, \
                    'http_port' : http_port, 'redis_port' : ws_port}
            r = requests.post('http://'+server_address+':80/add_ws', 
                              json.dumps(payload))
            print 'r.text', r.text
    except:
        print 'cc is down'
'''