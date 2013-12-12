import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httputil
import tornado.httpserver
import tornado.httpclient
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
import ConfigParser
import common

# [ STREAMS ]                           | persist on restart

# SET   KEY     'streams'               | set of streams owned by this ws     
# HASH  KEY     'stream:'+id            |
#       FIELD   'frames'                | total number of frames completed       
#       FIELD   'status'                | 'OK', 'DISABLED'
#       FIELD   'error_count'           | number of consecutive errors
#       FIELD   'system_hash'           | hash for system.xml.gz
#       FIELD   'integrator_hash'       | hash for integrator.xml.gz
#       FIELD   'download_token'        | used if download desired
#       FIELD   'cc_id'                 | which cc the stream came from
#       FIELD   'steps_per_frame'       | (optional)

# [ ACTIVE STREAMS ]                    | deleted on restart

# SET   KEY     'active_streams'        | active streams owned by the ws 
# HASH  KEY     'active_stream:'+id     | 
#       FIELD   'buffer_frames'         | number of frames in buffer.xtc
#       FIELD   'shared_token'          | each update must include this token 
#       FIELD   'donor'                 | which donor stream belongs to
#       FIELD   'steps'                 | checkpointed frames completed
#       FIELD   'start_time'            | time started (server time)

# On expiration, the donor/step stats are sent directly to the stats server on
# the CC

# [ COMMAND CENTER ]                    | reconfigured on restart

# SET   KEY     'ccs'                   | set of command center ids
# HASH  KEY     'cc:'+id                | 
#       FIELD   'ip'                    | ip of the CC
#       FIELD   'http_port'             | http_port of the CC
# STRNG KEY     'cc_ip:'+ip+':id'       | id given CC's ip
# Note: passphrase is explicitly not stored in db

# [ MISC ]

# ZSET  KEY     'heartbeats'                   | { stream_id : expire_time }
# STRNG KEY     'download_token:'+id+':stream' | reverse mapping   
# STRNG KEY     'shared_token:'+id+':stream'   | reverse mapping
# SET   KEY     'file_hashes'                  | files that exist in /files

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

class StreamHS(common.HashSet):
    _prefix = 'stream'
    _fields = {'frames'          : int,
               'status'          : str,
               'error_count'     : int,
               'system_hash'     : str,
               'integrator_hash' : str,
               'download_token'  : str,
               'cc_id'           : str,
               'steps_per_frame' : int
              }
    _rmaps  = {'download_token'}

class ActiveStreamHS(common.HashSet):
    _prefix = 'active_stream'
    _fields = {'buffer_frames'  : int,
               'shared_token'   : str,
               'donor'          : str,
               'steps'          : int,
               'start_time'     : float,
              }
    _rmaps  = {'shared_token'}

class CommandCenterHS(common.HashSet):
    _prefix = 'cc'
    _fields = {'ip'         : str,
               'http_port'  : str 
              }
    _rmaps  = {'ip'}

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
        ''' Each heartbeat received by the core increments the timer by
            increment amount. Defaults to once every 30 minutes '''
        self._max_error_count = max_error_count

    def post(self):
        ''' Used by the core to post a frame to the existing frames file

            Request parameters:
        
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
            stream_id = ActiveStreamHS.rmap('shared_token',token)
            if not stream_id:
                self.set_status(400)
                return
            stream = StreamHS.instance(stream_id)
            active_stream = ActiveStreamHS.instance(stream_id)
            if stream.status != 'OK':
                self.set_status(400)
                return self.write('Stream status not OK')
            if 'error_code' in self.request.headers:
                self.set_status(400)
                error_count = stream.hincrby('error_count',1)

                #self.db.hincrby('stream:'+stream_id,'error_count',1)
                #if error_count > self._max_error_count:
                self.deactivate_stream(stream_id)
                return self.write('Bad state.. terminating')
            stream.error_count = 0
            tar_string = cStringIO.StringIO(self.request.body)
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
                    # will modify the active_stream key except this ws
                    stream.hincrby('frames',active_stream.buffer_frames)
                    active_stream.buffer_frames = 0
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
            stream_id = ActiveStreamHS.rmap('shared_token',shared_token)
            if stream_id is None:
                self.set_status(401)
                return self.write('Unknown token')
            stream = StreamHS.instance(stream_id)
            # return if stream is stopped by PG user or NaN'd
            if self.db.hget('stream:'+stream_id,'status') != 'OK':
                self.set_status(400)
                return self.write('Stream Disabled')
            sys_file   = os.path.join('files',stream.system_hash)
            intg_file  = os.path.join('files',stream.integrator_hash)
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

class StreamHandler(BaseHandler):
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
        if not self.db.exists('cc_ip:'+self.request.remote_ip+':id'):
            print self.request.remote_ip
            self.set_status(401)
            return self.write('not authorized')
        # Assume request is bad by default
        self.set_status(400)
        try:
            # Step 1. Check if request is valid.
            state_bin = self.request.files['state_bin'][0]['body']
            required_strings = ['system','integrator']

            file_hashes = {}
            file_buffer = {}
            for s in required_strings:
                if s+'_bin' in self.request.files:
                    binary = self.request.files[s+'_bin'][0]['body']
                    bin_hash = hashlib.md5(binary).hexdigest()
                    if not self.db.sismember('file_hashes', bin_hash):
                        self.db.sadd('file_hashes', bin_hash)
                        file_buffer[bin_hash] = binary
                    else:
                        pass
                elif s+'_hash' in self.request.files: 
                    bin_hash = self.request.files[s+'_hash'][0]['body']
                    if not self.db.sismember('file_hashes', bin_hash):
                        return self.write('Gave me a hash for a file not \
                                           in files directory')
                else:
                    return self.write('missing content: '+s+'_bin/hash')
                file_hashes[s+'_hash'] = bin_hash
                
            # Step 2. Valid Request, generate uuid and write to disk
            stream_id = str(uuid.uuid4())
            stream_folder = os.path.join('streams',stream_id)
            if not os.path.exists(stream_folder):
                os.makedirs(stream_folder)
            # Write the initial state
            path = os.path.join(stream_folder,'state.xml.gz')
            open(path,'w').write(state_bin)
            for f_hash,f_bin in file_buffer.iteritems():
                open(os.path.join('files',f_hash),'w').write(f_bin)
            redis_pipe = self.db.pipeline()
            StreamHS.create(stream_id)
            stream = StreamHS.instance(stream_id)
            stream.frames = 0
            stream.status = 'OK'
            for k,v in file_hashes.iteritems():
                setattr(stream, k, v)
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
            stream_id = self.db.get('download_token:'+token+':stream')
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
        self.set_status(400)
        ''' PRIVATE - Delete a stream. '''
        if not self.db.exists('cc_ip:'+self.request.remote_ip+':id'):
            print self.request.remote_ip
            self.set_status(401)
            return self.write('not authorized')
        try:
            if 'stream_id' in self.request.headers:
                stream_id = self.request.headers['stream_id']
                stream = StreamHS.instance(stream_id)
                if stream:
                    # remove stream from memory
                    self.deactivate_stream(stream_id)
                    print 'FOO'
                    StreamHS.delete(stream_id)
                    self.db.delete('download_token:'+stream_id+':stream')
                    self.db.delete('stream:'+stream_id)
                    self.db.srem('streams',stream_id)
                    # remove stream from disk
                    shutil.rmtree(os.path.join('streams',stream_id))
                    self.set_status(200)
                    return
                else:
                    print 'FATAL: tried to delete a non existing stream on WS'
                    return self.write('stream not found')
            else:
                return self.write('Missing data')
        except Exception as e:
            print e
            return

class HeartbeatHandler(BaseHandler):
    def initialize(self, increment=30*60):
        ''' Each heartbeat received by the core increments the timer by
            increment amount. Defaults to once every 30 minutes '''
        self._increment = increment

    def get(self):
        print 'GOT REQUEST'
        self.set_status(200)
        return self.write('OK')

    def post(self):
        ''' Cores POST to this handler to notify the WS that it is still 
            alive. WS executes a zadd initially as well'''
        try:
            content = json.loads(self.request.body)
            token_id = content['shared_token']
            stream_id = self.db.get('shared_token:'+token_id+':stream')
            self.db.zadd('heartbeats',stream_id,
                          time.time()+self._increment)
            self.set_status(200)
        except KeyError:
            self.set_status(400)

class WorkServer(tornado.web.Application, common.RedisMixin):
    def _cleanup(self):
        # clear active streams
        if self.db.smembers('active_streams'):
            for stream in self.db.smembers('active_streams'):
                # deactivate this stream
                self.deactivate_stream(stream)
            # delete all keys in this
            self.db.delete('active_streams')
        # clear command centers 
        if self.db.smembers('ccs'):
            for cc_id in self.db.smembers('ccs'):
                self.db.delete('cc:'+cc_id)
        self.db.delete('ccs')
        cc_ips = self.db.keys('cc_ip:*')
        if cc_ips:
            for cc_ip in cc_ips:
                self.db.delete('cc_ip:'+cc_ip+':id')
        # remove heartbeats
        self.db.delete('heartbeats')
        # remove tokens
        #for expression in ['download_token:*','shared_token:*']:
        for expression in ['shared_token:*']:
            keys = self.db.keys(expression)
            if keys:
                self.db.delete(*keys)

        # inform the CC gracefully that the WS is dying (ie.expire everything)

    def __init__(self,ws_name,redis_port,redis_pass=None,ccs=None,increment=600):
        print 'Initialization redis server on port: ', redis_port
        self.db = self.init_redis(redis_port,redis_pass)
        
        StreamHS.set_redis(self.db)
        ActiveStreamHS.set_redis(self.db)

        if not os.path.exists('files'):
            os.makedirs('files')
        if not os.path.exists('streams'):
            os.makedirs('streams')
        self._cleanup()

        # ccs is a list of tuples, where
        # 0th-index is name
        # 1st-index is ip
        # 2nd-index is port
        if ccs:
            for cc in ccs:
                cc_name = cc[0]
                cc_ip   = cc[1]
                cc_port = cc[2]
                self.db.sadd('ccs',cc_name)
                self.db.hset('cc:'+cc_name,'ip',cc_ip)
                self.db.hset('cc:'+cc_name,'http_port',cc_port)
                self.db.set('cc_ip:'+cc_ip+':id',cc_name)
                # inform the CCs that we are alive. 
        else:
            print 'WARNING: No CCs were specified for this WS'

        check_stream_freq_in_ms = 60000
        pcb = tornado.ioloop.PeriodicCallback(self.check_heartbeats, 
                check_stream_freq_in_ms,tornado.ioloop.IOLoop.instance())
        pcb.start()
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        super(WorkServer, self).__init__([
            (r'/frame', FrameHandler),
            (r'/stream', StreamHandler),
            (r'/heartbeat', HeartbeatHandler, dict(increment=increment))
        ])

    def shutdown(self, signal_number=None, stack_frame=None):
        self.shutdown_redis()       
        print 'shutting down tornado...'
        tornado.ioloop.IOLoop.instance().stop()
        sys.exit(0)

    def check_heartbeats(self):
        ''' Queries heartbeats to find dead streams. Streams that have died are
        removed from the active_streams key and the hash is removed. 
        CC is then notified of the dead_streams and pushes them back
        into the appropriate queue 
        '''
        dead_streams = self.db.zrangebyscore('heartbeats', 0, time.time())
        if dead_streams:
            for dead_stream in dead_streams:
                self.deactivate_stream(dead_stream)

    def deactivate_stream(self, dead_stream_id):
        ActiveStreamHS.delete(dead_stream_id)
        buffer_path = os.path.join('streams',dead_stream_id,'buffer.xtc')
        if os.path.exists(buffer_path):
            with open(buffer_path,'w') as buffer_file:
                pass

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

    '''
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
        print uri
        print json.dumps(msg)
        try:
            resp = sync_client.fetch(uri,method='POST',body=json.dumps(msg))
        except tornado.httpclient.HTTPError as e: 
            print e
            print 'Could not connect to CC'
            ws_instance.shutdown()
     '''        

    print 'starting IO loop'

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