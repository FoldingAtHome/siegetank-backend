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

# All WS must authenticate themselves with the CC_WS_KEY
CC_WS_KEY = 'PROTOSS_IS_FOR_NOOBS'

cc_redis = redis.Redis(host='localhost', port=6379)
ws_redis = redis.Redis(host='localhost', port=6380)
ws2_redis = redis.Redis(host='localhost', port=6381)

ws_clients = [ws_redis, ws2_redis]

class WSManager:

    def __init__(self):
        self.redis_clients = {}

    def add_ws(self, ws_id, ip, redis_port):
        cc_redis.sadd('wss', ws_id)
        cc_redis.set('ws:'+ws_id+':ip', ip)
        cc_redis.set('ws:'+ws_id+':redis_port', redis_port)
        ws_redis = redis.Redis(host=ip, port=redis_port)
        self.redis_clients[ws_id] = ws_redis

    def get_redis_client(self, ws_id):
        return self.redis_clients[ws_id]

    def get_idle_ws(self):
        n_available_ws = cc_redis.card('wss')
        while n_available_ws > 0:
            ws_id = cc_redis.srandmember('wss')





            n_available_ws = cc_redis.card(ws_id)

        if n_available_ws == 0:
            return None

        return ws_id, ws_ip, redis_client
            # test and see if this WS is still alive

WSManager Workservers;

class WSHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PGI: Called by WS to register CC

        '''
        content = json.loads(self.request.body)
        ip = self.request.remote_ip
        try:
            test_key = content['cc_key']
            if test_key != CC_WS_KEY:
                return self.write('Bad CC_WS_KEY')

            ws_id = content['ws_id']
            redis_port = content['redis_port']

            Workservers.add_ws(ws_id, ip, redis_port)
        except Exception as e:
            print str(e)
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
            cc_redis.incr('target:'+target_id+':count')
        except Exception as e:
            print str(e)
            return self.write('bad request')

        # shove stream to random workserver
        random_ws_id = cc_redis.srandmember('wss')
        # record the WSs used by this target
        cc_redis.sadd('target:'+target_id+':wss', random_ws_id)

        random_ws_ip = cc.redis.get('ws:'+target_id+':ip')

        # forward the request to the WS

        response = requests.post('')

    def delete(self):
        ''' PGI: Delete a particular stream - (Request Forwarded to WS) '''
        print self.request.body

class JobHandler(tornado.web.RequestHandler):
    def get(self):
        ''' DI: Fetch a job from a random WS for the core
            Returns system.xml, integrator.xml, token, ws:ip'''

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
    application.listen(8888, '0.0.0.0')
    if not os.path.exists('files'):
        os.makedirs('files')
    tornado.ioloop.IOLoop.instance().start()