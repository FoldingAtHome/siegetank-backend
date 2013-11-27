import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httputil
import redis

import signal
import uuid
import random
import threading
import os
import json
import requests
import sys
import subprocess
import hashlib
import time
import traceback

CCs = {'127.0.0.1' : 'PROTOSS_IS_FOR_NOOBS'}

# [ STREAMS ]

# SET   KEY     'streams'               | set of streams owned by this ws     
# HASH  KEY     'stream:'+id    
#       FIELD   'frames'                | frame count of the stream          
#       FIELD   'state'                 | 0 - OK, 1 - disabled, 2 - error    
#       FIELD   'download_token'        | (optional) used for file transfers
#       FIELD   'system_hash'           | hash for system.xml.tar.gz
#       FIELD   'integrator_hash'       | hash for integrator.xml.tar.gz

# SET   KEY     'active_streams'        | active streams owned by the ws 
# HASH  KEY     'active_stream:'+id     | expirable 
#       FIELD   'shared_token'          | each update must include this token 
#       FIELD   'donor'                 | which donor the stream belongs to 
#       FIELD   'start_time'            | elapsed time in seconds  
#       FIELD   'steps'                 | steps completed thus far

# [ MISC ]

# ZSET  KEY     'heartbeats'                | { stream_id : expire_time }
# STRNG KEY     'shared_token:'+id+':stream'| reverse mapping
# SET   KEY     'file_hashes'               | files that exist in /files

# Expiration mechanism:
# hearbeats is a sorted set. A POST to ws/update extends expire_time in 
# heartbeats. A checker callback is passed into ioloop.PeriodicCallback(), 
# which checks for expired streams against the current time. Streams that 
# expire can be obtained by: redis.zrangebyscore('heartbeat',0,current_time)

ws_redis = None

class FrameHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PUBLIC - Used by Core to add a frame
            POST parameters:

            target_id: uuid #
            stream_id: uuid #
            frame_bin: frame.pb # protocol buffered frame
        '''
        print 'foo'

class StreamHandler(tornado.web.RequestHandler):
    def post(self):       
        ''' PRIVATE - Add new stream(s) to WS. The POST method on this URI
            can only be accessed by known CCs (ip restricted) 
            Parameters:

            While JSON would be nice, file transfers are handled differently
            in order to avoid the base64 overhead. The encoding should use a
            multi-part/form-data. This is inspired by Amazon AWS S3's method
            of POSTing objects. 

            Note: Base64 incurs a 33 percent size overhead.

             Instead we encode using 
            multipart/form-data encoding to directly transfer the binaries

            The required files are: system, state, and integrator. The CC can
            query the redis db 'file_hashes' to see if some of files exist. CC
            can choose send in either 'system_hash', or 'system_bin'. The CC
            queries the WS's own list of hashes to make sure.

            The WS checks to see if this request is valid, and generates a 
            unique stream_id if so, and returns the id back to the CC.
            '''
        if not self.request.remote_ip in CCs:
            print self.request.remote_ip
            self.set_status(401)
            return self.write('not authorized')

        # Assume request is bad by default
        self.set_status(400)

        try:
            # Step 1. Check if request is valid.
            content = json.loads(self.request.files['json'][0]['body'])
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
                elif s+'_hash' in content: 
                    bin_hash = content[s+'_hash']
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
            path = os.path.join(stream_folder,'state.xml.tar.gz')
            open(path,'w').write(state_bin)
            for f_hash,f_bin in file_buffer.iteritems():
                open(os.path.join('files',f_hash),'w').write(f_bin)
            redis_pipe.sadd('streams',stream_id)
            redis_pipe.hset('stream:'+stream_id, 'frames', 0)
            redis_pipe.hset('stream:'+stream_id, 'state', 0)
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


        #return

    def get(self):
        ''' PRIVATE - Download a stream. 
            The CC creates a token given to the Core for identification
            purposes.

            Parameters:

            stream_id: uuid #

            RESPONDS with a state.xml '''

    def delete(self):
        ''' PRIVATE - Delete a stream. '''

class HeartbeatHandler(tornado.web.RequestHandler):
    def initialize(self, redis_client=ws_redis, increment=30*60):
        ''' Each heartbeat received by the core increments the timer by
            increment amount. Defaults to once every 30 minutes '''
        self._increment = increment

    def post(self):
        ''' Cores POST to this handler to notify the WS that it is still 
            alive. '''

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
        ws_redis.srem('active_streams', *dead_streams)
        ws_redis.delete(*('active_stream:'+s for s in dead_streams))

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