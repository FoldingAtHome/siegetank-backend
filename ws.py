import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httputil
import redis
import cStringIO
import tarfile
import signal
import uuid
import os
import json
import requests
import sys
import subprocess
import hashlib
import time
import traceback
import shutil

# [ STREAMS ]

# SET   KEY     'streams'               | set of streams owned by this ws     
# HASH  KEY     'stream:'+id    
#       FIELD   'frames'                | number of frames in frames.xtc        
#       FIELD   'status'                | 'OK', 'DISABLED'
#       FIELD   'error_count'           | number of consecutive errors
#       FIELD   'system_hash'           | hash for system.xml.gz
#       FIELD   'integrator_hash'       | hash for integrator.xml.gz
#       FIELD   'download_token'        | used if download desired
#       FIELD   'steps_per_frame'       | (optional)

# SET   KEY     'active_streams'        | active streams owned by the ws 
# HASH  KEY     'active_stream:'+id     | 
#       FIELD   'buffer_frames'         | number of frames in buffer.xtc
#       FIELD   'shared_token'          | each update must include this token 
#       FIELD   'donor'                 | which donor the stream belongs to  
#       FIELD   'steps'                 | steps completed thus far

# Not implemented yet
# LIST  KEY     'stat:'+id+':donor'     | donor statistics
# LIST  KEY     'stat:'+id+':frames'    | cardinality equal to above

# [ MISC ]

# ZSET  KEY     'heartbeats'                   | { stream_id : expire_time }
# SET   KEY     'download_token:'+id+':stream' | stream the token maps to   
# STRNG KEY     'shared_token:'+id+':stream'   | reverse mapping
# SET   KEY     'file_hashes'                  | files that exist in /files

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

# General WS config
# Block ALL ports except port 80
# Redis port is only available to CC's IP on the intranet

CCs = {'127.0.0.1' : 'PROTOSS_IS_FOR_NOOBS'}

ws_redis = None

def deactivate_stream(dead_stream):
    shared_token = ws_redis.hget('active_stream:'+dead_stream,'shared_token')
    ws_redis.delete('shared_token:'+shared_token+':stream')
    ws_redis.delete('active_stream:'+dead_stream)
    ws_redis.srem('active_streams', dead_stream)
    buffer_path = os.path.join('streams',dead_stream,'buffer.xtc')
    if os.path.exists(buffer_path):
        with open(buffer_path,'w') as buffer_file:
            pass

def push_stream_to_cc(stream_id):
    pass

class FrameHandler(tornado.web.RequestHandler):
    def initialize(self, max_error_count=10):
        ''' Each heartbeat received by the core increments the timer by
            increment amount. Defaults to once every 30 minutes '''
        self._max_error_count = max_error_count

    def post(self):
        ''' CORE - Used by the core to add a frame

            REQ Parameters:
        
            HEADER { 'token_id'      : unique_id } 
            HEADER { 'error_state'   : status_code }    # optional
            HEADER { 'error_message' : error_message }  # optional
            
            Valid values for error:
                - 'InitError'      | failed to initialize
                - 'FailedRefCheck' | failed check against reference
                - 'BadState'       | bad state (Bad Forces, NaNs, Etc)

            BODY   binary_tar # a binary tar file containing filenames:
                              # 'frame.xtc', a single xtc frame
                              # state.xml.gz', checkpoint file
            
            REPLY:
            200 - OK 
            400 - Bad Request
        '''
        try:
            token = self.request.headers['shared_token']
            stream_id = ws_redis.get('shared_token:'+token+':stream')
            if not stream_id:
                self.set_status(400)
                return
            if ws_redis.hget('stream:'+stream_id,'status') != 'OK':
                self.set_status(400)
                return self.write('Stream status not OK')
            if 'error_code' in self.request.headers:
                self.set_status(400)
                errors = ws_redis.hincrby('stream:'+stream_id,'error_count',1)
                #if errors > self._max_error_count:
                deactivate_stream(stream_id)
                return self.write('Bad state.. terminating')
            ws_redis.hset('stream:'+stream_id,'error_count',0)
            tar_string = cStringIO.StringIO(self.request.body)
            with tarfile.open(mode='r', fileobj=tar_string) as tarball:
                # Extract the frame
                frame_member = tarball.getmember('frame.xtc')
                frame_binary = tarball.extractfile(frame_member).read()
                buffer_path = os.path.join('streams',stream_id,'buffer.xtc')
                with open(buffer_path,'ab') as buffer_file:
                    buffer_file.write(frame_binary)
                # Increment buffer frames by 1
                ws_redis.hincrby('active_stream:'+stream_id, 
                                 'buffer_frames',1)
                # TODO: Check to make sure the frame is valid 
                # valid in both md5 hash integrity and xtc header integrity
                # make sure time step has increased?
                # If the stream NaNs 

                # See if state is present, if so, the buffer.xtc is
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
                    # will modify the active_streams key except this ws
                    buf_frames = ws_redis.hget('active_stream:'+stream_id,
                                               'buffer_frames')    
                    ws_redis.hincrby('stream:'+stream_id,'frames',buf_frames)
                    ws_redis.hset('active_stream:'+stream_id,
                                  'buffer_frames',0)
                    # clear the buffer
                    with open(buffer_path,'w') as buffer_file:
                        pass
                except KeyError as e:
                    pass
        except KeyError as e:
            print repr(e)
            ex_type, ex, tb = sys.exc_info()
            traceback.print_tb(tb)
            self.set_status(400)
            return self.write('Bad Request')

    def get(self):
        ''' CORE - The core calls this method to retrieve a frame only after
            first going to CC to get a shared_token. The CC initializes the
            necessary redis internals, starts a heartbeat, and sets the hash.
            'active_stream'+stream_id.

            REQUEST:
            shared_token: token # used for Core identification

            REPLY:
            200 - send binaries state/system/integrator

            We need to be extremely careful about checkpoints and frames, as 
            it is important we avoid writing duplicate frames on the first 
            step for the core. We use the follow scheme:

                  ------------------------------------------------------------
                  |c       core 1      |c|              core 2           |c|
                  ---                  --|--                             -----
            frame: x|1 2 3 4 5 6 7 8 9 10| |11 12 13 14 15 16 17 18 19 20| |21
                    ---------------------| ------------------------------- ---
        
            When a core fetches a checkpoint, it makes sure to NOT write the
            first frame (equivalent to the frame of fetched state.xml file).
            On every subsequent checkpoint, both the frame and the checkpoint 
            are sent back to the workserver.
        '''
        try:
            shared_token = self.request.headers['shared_token']
            if not ws_redis.exists('shared_token:'+shared_token+':stream'):
                self.set_status(401)
                return self.write('Unknown token')
            stream_id  = ws_redis.get('shared_token:'+shared_token+':stream')
            # return if stream is stopped by PG user or NaN'd
            if ws_redis.hget('stream:'+stream_id,'status') != 'OK':
                self.set_status(400)
                return self.write('Stream Disabled')
            sys_hash   = ws_redis.hget('stream:'+stream_id,'system_hash')
            intg_hash  = ws_redis.hget('stream:'+stream_id,'integrator_hash')
            sys_file   = os.path.join('files',sys_hash)
            intg_file  = os.path.join('files',intg_hash)
            state_file = os.path.join('streams',stream_id,'state.xml.gz')
            # Make a tarball in memory and send directly
            c = cStringIO.StringIO()
            tarball = tarfile.open(mode='w', fileobj=c)
            tarball.add(sys_file, arcname='system.xml.gz')
            tarball.add(intg_file, arcname='integrator.xml.gz')
            tarball.add(state_file, arcname='state.xml.gz')
            tarball.close()
            self.set_header('Content-Type', 'application/octet-stream')
            self.set_status(200)
            return self.write(c.getvalue())
        except Exception as e:
            print repr(e)
            ex_type, ex, tb = sys.exc_info()
            traceback.print_tb(tb)
            self.set_status(400)
            return self.write('Bad Request')

class StreamHandler(tornado.web.RequestHandler):
    def post(self):       
        ''' PRIVATE - Add new stream(s) to WS. The POST method on this URI
            can only be accessed by known CCs (ip restricted)

            While JSON would be nice, file transfers are handled differently
            in order to avoid the base64 overhead. The encoding should use a
            multi-part/form-data. This is inspired by Amazon AWS S3's method
            of POSTing objects. 

            The required files are: system, state, and integrator. The CC can
            query the redis db 'file_hashes' to see if some of files exist. CC
            can choose send in either 'system_hash', or 'system_bin'. The CC
            queries the WS's own list of hashes to make sure. 

            The WS checks to see if this request is valid, and generates a 
            unique stream_id if so, and returns the id back to the CC.

            Ex1. Three binaries
            files = {
                'state_bin' : state_bin,
                'system_bin' : system_bin,
                'integrator_bin' : integrator_bin
            }
            Ex2. System hash and integrator hash, (note State must be bin)
            files = {
                'state_bin' : state_bin,
                'system_hash' : system_hash,
                'integrator_bin' : integrator_bin
            }
            prep = requests.Request('POST','http://url',files=files).prepare()
            resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)

            Response code: 200 - OK
                           400 - BAD REQUEST
                           401 - UNAUTHORIZED
            '''
        if not self.request.remote_ip in CCs:
            print self.request.remote_ip
            self.set_status(401)
            return self.write('not authorized')
        # Assume request is bad by default
        self.set_status(400)
        try:
            # Step 1. Check if request is valid.
            state_bin = self.request.files['state_bin'][0]['body']
            required_strings = ['system','integrator']
            redis_pipe = ws_redis.pipeline()
            file_hashes = {}
            file_buffer = {}
            for s in required_strings:
                if s+'_bin' in self.request.files:
                    binary = self.request.files[s+'_bin'][0]['body']
                    bin_hash = hashlib.md5(binary).hexdigest()
                    if not ws_redis.sismember('file_hashes', bin_hash):
                        ws_redis.sadd('file_hashes', bin_hash)
                        file_buffer[bin_hash] = binary
                    else:
                        pass
                elif s+'_hash' in self.request.files: 
                    bin_hash = self.request.files[s+'_hash'][0]['body']
                    if not ws_redis.sismember('file_hashes', bin_hash):
                        return self.write('Gave me a hash for a file not \
                                           in files directory')
                else:
                    return self.write('missing content: '+s+'_bin/hash')
                file_hashes[s+'_hash'] = bin_hash
                
            # Step 2. Valid Request. Generate uuid and write to disk
            stream_id = str(uuid.uuid4())
            stream_folder = os.path.join('streams',stream_id)
            if not os.path.exists(stream_folder):
                os.makedirs(stream_folder)
            # Write the initial state
            path = os.path.join(stream_folder,'state.xml.gz')
            open(path,'w').write(state_bin)
            for f_hash,f_bin in file_buffer.iteritems():
                open(os.path.join('files',f_hash),'w').write(f_bin)
            redis_pipe.sadd('streams',stream_id)
            redis_pipe.hset('stream:'+stream_id, 'frames', 0)
            redis_pipe.hset('stream:'+stream_id, 'status', 'OK')
            for k,v in file_hashes.iteritems():
                redis_pipe.hset('stream:'+stream_id, k, v)
            redis_pipe.execute()
            self.set_status(200)
            return self.write(stream_id)
        except KeyError as e:
            print repr(e)
            ex_type, ex, tb = sys.exc_info()
            traceback.print_tb(tb)
            self.set_status(400)
            return self.write('Bad Request')

    def get(self):
        ''' PRIVATE - Download a stream. 
            The CC first creates a token given to the Core for identification.
            The token and WS's IP is then sent back to the ST interface
            Parameters:
            download_token: download_token automatically maps to the right
                            stream_id
            RESPONDS with the appropriate frames.
        '''
        self.set_status(400)
        try:
            token = self.request.headers['download_token']
            stream_id = ws_redis.get('download_token:'+token+':stream')
            if stream_id:
                filename = os.path.join('streams',stream_id,'frames.xtc')
                buf_size = 4096
                self.set_header('Content-Type', 'application/octet-stream')
                self.set_header('Content-Disposition', 
                                'attachment; filename=' + filename)
                with open(filename, 'r') as f:
                    while True:
                        data = f.read(buf_size)
                        if not data:
                            break
                        self.write(data)
                self.finish()
            else:
                self.set_status(400)
        except Exception as e:
            print e

    def delete(self):
        ''' PRIVATE - Delete a stream. '''
        if not self.request.remote_ip in CCs:
            print self.request.remote_ip
            self.set_status(401)
            return self.write('not authorized')

        try:
            if 'stream_id' in self.request.headers['stream_id']:
                stream_id = self.request.headers['stream_id']
                if ws_redis.sismember('streams', stream_id):

                    # remove temporary
                    deactivate_stream(stream_id)

                    ws_redis.delete('stream:'+stream_id)
                    ws_redis.srem('streams',stream_id)
                    
                else:
                    self.set_status(400)
                    print 'FATAL: Tried deleting a non existing stream on WS'
                    return self.write('stream not found')


        except Exception as e:
            self.set_status(400)
            return

        print 'foo'

class HeartbeatHandler(tornado.web.RequestHandler):
    def initialize(self, increment=30*60):
        ''' Each heartbeat received by the core increments the timer by
            increment amount. Defaults to once every 30 minutes '''
        self._increment = increment

    def post(self):
        ''' Cores POST to this handler to notify the WS that it is still 
            alive. WS executes a zadd initially as well'''

        try:
            content = json.loads(self.request.body)
            token_id = content['shared_token']
            stream_id = ws_redis.get('shared_token:'+token_id+':stream')
            ws_redis.zadd('heartbeats',stream_id,
                          time.time()+self._increment)
            self.set_status(200)
        except KeyError:
            self.set_status(400)

def init_redis(redis_port):
    ''' Initializes the global redis client used by the ws and returns a ref
        to the client (eg. used by test cases) '''
    global ws_redis
    args = ("redis/src/redis-server", "--port", redis_port)
    redis_process = subprocess.Popen(args)
    if redis_process.poll() is not None:
        print 'COULD NOT START REDIS-SERVER, aborting'
        sys.exit(0)
    ws_redis = redis.Redis(host='localhost', port=int(redis_port))
    # wait until redis is alive
    alive = False
    while not alive:
        try:
            alive = ws_redis.ping() 
        except:
            pass
    return ws_redis

def check_heartbeats():
    ''' Queries heartbeats to find dead streams. Streams that have died are
        removed from the active_streams key and the hash is removed. 
        CC is then notified of the dead_streams and pushes them back
        into the appropriate queue '''
    dead_streams = ws_redis.zrangebyscore('heartbeats', 0, time.time())
    if dead_streams:
        for dead_stream in dead_streams:
            deactivate_stream(dead_stream)

def clean_exit(signal, frame):
    print 'shutting down redis...'
    ws_redis.shutdown()
    print 'shutting down tornado...'
    tornado.ioloop.IOLoop.instance().stop()
    sys.exit(0)

if __name__ == "__main__":
    
    application = tornado.web.Application([
        (r'/frame', FrameHandler),
        (r'/stream', StreamHandler),
        (r'/heartbeat', HeartbeatHandler)
    ])

    redis_port = sys.argv[1]
    http_port = sys.argv[2]

    init_redis(redis_port)

    signal.signal(signal.SIGINT, clean_exit)

    if not os.path.exists('files'):
        os.makedirs('files')
    if not os.path.exists('streams'):
        os.makedirs('streams')

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

    # clear db
    ws_redis.flushdb()

    application.listen(http_port, '0.0.0.0')
    pcb = tornado.ioloop.PeriodicCallback(check_heartbeats, 10000, 
          tornado.ioloop.IOLoop.instance())
    pcb.start()
    tornado.ioloop.IOLoop.instance().start()