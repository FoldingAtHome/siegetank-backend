import tornado.escape
import tornado.ioloop
import tornado.web

import datetime
import hashlib
import json
import os
import uuid
import random
import requests

import redis

CC_WS_KEY = 'PROTOSS_IS_FOR_NOOBS'

cc_redis = redis.Redis(host='localhost', port=6379)
ws_redis_clients = {}

def remove_ws(ws_id):
    streams = cc_redis.smembers('ws:'+ws_id+':streams')
    for stream in streams:
        target_id = cc_redis.get('stream:'+stream+':target')
        cc_redis.zrem('queue:'+target_id, stream)
    ws_redis_clients.pop(ws_id, None)

def test_ws(ws_id):
    ''' If WS is up, returns True.
        If WS is down, remove_ws() is called. Returns False.
    '''
    # test redis
    try:
        ws_redis_clients(ws_id).ping()
    except:
        remove_ws(ws_id)

def get_idle_ws():
    n_available_ws = cc_redis.card('workservers')
    while n_available_ws > 0:
        ws_id = cc_redis.srandmember('workservers')

        ws = test_ws(ws_id) 

        n_available_ws = cc_redis.card(ws_id)

    if n_available_ws == 0:
        return None

    return ws_id, ws_ip, redis_client
        # test and see if this WS is still alive

# Redis memory usage:
# http://nosql.mypopescu.com/post/1010844204/redis-memory-usage

# [ WS ]
#
# SET   KEY     'active_ws'             [ set of active ws ids ]
# HASH  KEY     'ws:'+id     
#       FIELD   'ip'                    [ ip address ]
#       FIELD   'http_port'             [ port of the ws's webserver ]
#       FIELD   'redis_port'            [ port of the ws's redis-server ]
# SET KEY 'ws:'+ws_id+':streams'        [ list of all the streams owned by the ws ]

# [ STREAMS ]
#
# SET   KEY     'streams'               [ set of all the streams ]
# HASH  KEY     'stream:'+id
#       FIELD   'active'                [ if the stream is active or stopped ]
#       FIELD   'ws'                    [ id of the ws the stream is on ]
#       FIELD   'target'                [ target the stream belongs to ]

# [ TARGETS ]
#       
# SET   KEY     'targets'               [ set of all the targets ]
# HASH  KEY     'target:'+id
#       FIELD   'description'           [ description of the target ]
#       FIELD   'owner'                 [ owner of the target ]
#       FIELD   'system'                [ md5sum of the system file ]
#       FIELD   'integrator'            [ md5sum of the integrator file ]
#       FIELD   'date'                  [ creation date of in seconds since 1/1/70 ]    
# SET   KEY     'target:'+id+':streams' [ list of all streams of the target ]
# OSET  KEY     'queue:'+id             [ priority queue of streams ]

# WS Connect:
# -sadd ws_id to active_ws
# -(re)configure hash ws:ws_id, as ip and ports may have changed
# -for stream in 'ws:'+ws_id+':streams' 
#      add stream to 'queue:'+hget('stream:'+id,target)

# WS Clean Disconnect:
# -srem ws_id from active_ws
# -remove HASH key 'ws:'+ws_id
# -for each stream in 'ws:'+ws_id+':streams', find its target and remove the stream from priority queue

# CC Initialization
# -figure out which ws are still active by looking through cached active_ws, and 'ws:'+id
# -for each stream in streams, see if the ws_id it belongs to is alive using hash 'ws:'+ws_id

class WSHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PGI: Called by WS for registration '''
        content = json.loads(self.request.body)
        ip = self.request.remote_ip
        try:
            test_key = content['cc_key']
            if test_key != CC_WS_KEY:
                self.set_status(401)
                return self.write('Bad CC_WS_KEY')

            ws_id = content['ws_id']
            cc_redis.add('workservers',ws_id)

            require_strings = ['ip','redis_port','http_port']
            for string in require_strings:
                item = content[string]
                cc_redis.hset('ws:'+ws_id, string, item)

            ws_redis_clients[ws_id] = redis.Redis(host=content['ip'], 
                                                  port=int(content['redis_port']))

            # see if this ws existed in the past
            existing_streams = cc_redis.smembers('ws:'+ws_id+':streams') > 0:
            if existing_streams:
                for stream in existing_streams:
                    
                    # ONLY DO THIS IF STREAM IS ACTIVE
                    target = cc_redis.get('stream:'+stream+':target')
                    priority_queue

        except Exception as e:
            print str(e)
            self.set_status(400)
            return self.write('bad request')

        self.write('REGISTERED')

class TargetHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PGI - Post a new target '''
        self.set_status(400)
        content = json.loads(self.request.body)
        try:
            system = content['system']
            integrator = content['integrator']
            
            if len(system) == 0 or len(integrator) == 0:
                return self.write('bad request')

            system_sha = hashlib.sha256(system).hexdigest()
            path = './files/'+system_sha
            if not os.path.isfile(path):
                open(path,'w').write(system)
 
            integrator_sha = hashlib.sha256(integrator).hexdigest()
            path = './files/'+integrator_sha
            if not os.path.isfile(path):
                open(path,'w').write(integrator)
            description = content['description']
            creation_time = cc_redis.time()[0]
            target_id = hashlib.sha256(str(uuid.uuid4())).hexdigest()
            owner = 'yutong'

            # store target details into redis
            cc_redis.hset('target:'+target_id,'system',system_sha)
            cc_redis.hset('target:'+target_id,'integrator',integrator_sha)
            cc_redis.hset('target:'+target_id,'description',description)
            cc_redis.hset('target:'+target_id,'date',creation_time)
            cc_redis.hset('target:'+target_id,'owner',owner)

            # add target_id to the owner's list of targets
            cc_redis.sadd(owner+':targets',target_id)

            # add target_id to list of targets managed by this WS
            cc_redis.sadd('targets',target_id)

        except Exception as e:
            print str(e)
            return self.write('bad request')

        self.set_status(200)
        return self.write('OK')

    def get(self):
        ''' PGI - Fetch details on a target'''
        user = 'yutong'
        response = []
        targets = cc_redis.smembers(user+':targets')
        for target_id in targets:
            prop = {}

            stamp = datetime.datetime.fromtimestamp(float(cc_redis.hget(target_id,'date')))
            prop['date'] = stamp.strftime('%m/%d/%Y, %H:%M:%S')
            prop['description'] = cc_redis.hget(target_id,'description')
            prop['frames'] = random.randint(0,200)
            response.append(prop)

        return self.write(json.dumps(response,indent=4, separators=(',', ': ')))

    def delete(self):
        # delete all streams

        # delete the project
        return

class StreamHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PGI: Add new streams to an existing target. The input must be compressed
            state.xml files encoded as base64. Streams are routed directly to the WS via 
            redis using a pubsub mechanism - (Request Forwarded to WS) '''
        content = json.loads(self.request.body)
        try:
            target_id = content['target_id']
        except Exception as e:
            print str(e)
            return self.write('bad request')

        # shove stream to random workserver
        random_ws_id = cc_redis.srandmember('wss')
        # record the WSs used by this target
        cc_redis.sadd('target:'+target_id+':wss', random_ws_id)

        random_ws_ip = cc.redis.get('ws:'+target_id+':ip')

        response = requests.post('')

    def delete(self):
        ''' PGI: Delete a particular stream - (Request Forwarded to WS) '''
        print self.request.body

class JobHandler(tornado.web.RequestHandler):
    def get(self):
        ''' DI: Fetch a job from a random WS for the core
            Returns system.xml, integrator.xml, token, ws:ip

            notes: when popping off priority queue, need to make sure that the WS 
            is still ALIVE. 

            '''

        # for a random target
        target_id = random.choice()

        # pick a random ws from the list of targets

        client = random.choice(ws_clients)
        pipe = client.pipeline()
        pipe.zrevrange('target:'+target_id+':queue',0,0)
        pipe.zremrangebyrank('target:'+target_id+':queue',-1,-1)
        result = pipe.execute()
        head = result[0]

        if head:
            stream_id = head[0]
            client.set('expire:'+stream_id,0)
            client.expire('expire:'+stream_id,5)
            return self.write('Popped stream_id:' + stream_id)
        else:
            return self.write('No jobs available!')

application = tornado.web.Application([
    (r'/target', TargetHandler),
    (r'/stream', StreamHandler),
    (r'/job', JobHandler),
    (r'/add_ws', WSHandler)
])
 
if __name__ == "__main__":
    cc_redis.flushdb()

    # when CC starts, we need to:
    # rebuild all priority queues. 

    application.listen(8888, '0.0.0.0')
    if not os.path.exists('files'):
        os.makedirs('files')
    tornado.ioloop.IOLoop.instance().start()