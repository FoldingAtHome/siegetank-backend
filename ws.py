import tornado.escape
import tornado.ioloop
import tornado.web
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

CCs = {'127.0.0.1' : 'PROTOSS_IS_FOR_NOOBS'}

# [ STREAMS ]

# SET   KEY     'ws:'+id+':streams'     | set of streams owned by the ws     
# HASH  KEY     'stream:'+id    
#       FIELD   'frames'                | frame count of the stream          
#       FIELD   'state'                 | 0 - OK, 1 - disabled, 2 - error    
#       FIELD   'target'                | target the stream belongs to       

# SET   KEY     'active_streams'        | active streams owned by the ws 
# HASH  KEY     'active_stream:'+id     | expirable 
#       FIELD   'shared_token'          | each update must include this token 
#       FIELD   'donor'                 | which donor the stream belongs to 
#       FIELD   'start_time'            | elapsed time in seconds  
#       FIELD   'steps'                 | steps completed thus far

# [ MISC ]

# ZSET  KEY     'heartbeats'                | { stream_id : expire_time }
# STRNG KEY     'shared_token:'+id+':stream'| reverse mapping

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

            REQUIRED:
            stream_ids: uuid1 #
            state_bin: state.xml.tar.gz  #
            system_hash or system_bin #

            stored as:
            /targets/target_id/stream_id/state.xml.tar.gz
        '''

        if not self.request.remote_ip in CCs:
            print self.request.remote_ip
            self.set_status(401)
            return self.write('not authorized')

        self.set_status(400)
        
        try:
            content = json.loads(self.request.body)
            stream_id = content['stream_id']
            state_bin = content['state_bin']
            open(path+'/'+'state.xml.tar.gz','w').write(state_bin)
            required_strings = ['system','integrator']
            for s in required_strings:
                if s+'_bin' in content:
                    binary = content[s+'_bin']
                    bin_hash = hashlib.md5(binary).hexdigest()
                    if len(bin_hash) == 0:
                        return self.write('binary and bin_hash empty')
                    if not os.path.exists('files/'+bin_hash):
                        open('files/'+bin_hash, 'w').write(binary)
                    else:
                        print 'found duplicate hash'
                elif s+'_hash' in content: 
                    bin_hash = content[s+'_hash']
                    if len(bin_hash) == 0:
                        return self.write('binary and bin_hash empty')
                    if not os.path.exists('files/'+bin_hash):
                        return self.write('Gave me a hash for a file not \
                                           in files directory')
                else:
                    return self.write('missing content: '+s+'_bin/hash')
                ws_redis.hset('stream:'+stream_id, s, bin_hash)

            ws_redis.hset('stream:'+stream_id, 'owner', content['owner'])
            ws_redis.hset('stream:'+stream_id, 'frames', 0)
            self.set_status(200)

        except Exception as e:
            print e
            return self.write('bad request')

        return

    def get(self):
        ''' PRIVATE - Assign a job. 
            The CC creates a token given to the Core for identification
            purposes.

            Parameters:

            target_id: uuid #
            stream_id: uuid #

            RESPONDS with a state.xml '''

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
            token_id = content['core_token']
            print 'TOKEN', token_id
            stream_id = ws_redis.get('core_token:'+token_id+':stream')
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
        removed from the active_streams key and the hash is removed. '''
    dead_streams = ws_redis.zrangebyscore('heartbeats', 0, time.time())
    if dead_streams:
        ws_redis.srem('active_streams', dead_streams)
        ws_redis.delete('active_streams:'+s for s in dead_streams)

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