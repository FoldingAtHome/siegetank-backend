import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver

import datetime
import hashlib
import json
import os
import uuid
import random
import requests
import redis
import signal
import sys
import apollo

import common

# The command center manages several work servers in addition to managing 
# the stats system for each work server. 

# CC uses Antirez's redis extensively as NoSQL store mainly because of 
# its blazing fast speed. Binary blobs such as states, systems, and 
# frames are written directly to disk.

# For more information on redis memory usage, visit:
# http://nosql.mypopescu.com/post/1010844204/redis-memory-usage

# On average, we expect the load of the CC to be significantly lower 
# than that of the WS. If each stream takes about 4 hours until 
# termination, then each stream makes about 6 requests per day. 
# A single CC is expected to handle 500,000 streams. This is 
# about 3 million requests per day - translating to about 35 requests 
# per second. Each request needs to be handled by the CC in 30 ms. 


class WorkServer(apollo.Entity):
    prefix = 'ws'
    fields = {'ip': str,  # ip address
              'http_port': int,   # ws http port
              'redis_port': int,  # ws redis port
              'redis_pass': str,  # ws password1
              }

WorkServerDB = {}  # 'name' : redis_client


class Targets(apollo.Entity):
    prefix = 'target'
    fields = {'description': str,  # description of the target
              'owner': str,  # owner of the target
              'system_md5': str,  # md5 of the system.xml file
              'integrator_md5': str,  # md5 of the integrator.xml file
              'creation_date': str,  # creation of the target
              'stage': str,  # disabled, beta, release
              'workservers': {WorkServer},  # set of WSs that own streams
              }


class User(apollo.Entity):
    prefix = 'user'



# WS Clean Disconnect:
# -for each stream in 'ws:'+ws_id+':streams', find its target and remove the stream from priority queue
# -srem ws_id from active_ws
# -remove HASH KEY 'ws:'+ws_id

# CC Initialization (assumes RDB or AOF file is present)
# -figure out which ws are still active by looking through saved active_ws, and 'ws:'+id
# -for each stream in streams, see if the ws_id it belongs to is alive using hash 'ws:'+ws_id

# PG Interface
# PUT x.com/targets 
# DELETE x.com/targets/:target_id
# GET x.com/targets/:target_id
#   Returns a list of streams and the CC it's on. 

# Core Interface
# POST x.com/assign
#   assign a stream on a particular WS to the core
#   If successful:
#       Return an HTTP Code 302 - with URL

# WS Interface
# POST x.com/streams/stop


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db


class RegisterWSHandler(BaseHandler):
    def put(self):
        ''' Called by WS for registration '''
        content = json.loads(self.request.body.decode())
        auth = content['auth']
        if auth != self.application.cc_pass:
            return self.set_status(401)
        content = json.loads(self.request.body.decode())
        ip = self.request.remote_ip
        name = content['name']
        http_port = content['http_port']
        redis_port = content['redis_port']
        redis_pass = content['redis_pass']
        client = redis.Redis(host=ip, port=redis_port, password=redis_pass,
                             decode_responses=True)
        client.ping()
        if not WorkServer.exists(name, self.db):
            ws = WorkServer.create(name, self.db)
        else:
            ws = WorkServer(name, self.db)
        ws.hset('ip', ip)
        ws.hset('http_port', http_port)
        ws.hset('redis_port', redis_port)
        ws.hset('redis_pass', redis_pass)

        WorkServerDB[name] = redis
        self.set_status(200)


class TargetHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PGI - Post a new target




        '''


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
            return self.write('bad request')

        # shove stream to random workserver
        random_ws_id = cc_redis.srandmember('wss')
        # record the WSs used by this target
        cc_redis.sadd('target:'+target_id+':wss', random_ws_id)
        random_ws_ip = cc.redis.get('ws:'+target_id+':ip')
        response = requests.post('')

    def put(self):
        ''' PGI: Enable/Disable a particular stream
            A stream is Enable/Disabled by sending a WS redis request directly 
            However, if stream's error_count is > 10 then it cannot be ENABLED

            '''
        pass


    def delete(self):
        ''' PGI: Delete a particular stream - (Request Forwarded to WS) '''
        pass

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


CC_WS_KEY = 'PROTOSS_IS_FOR_NOOBS'

cc_redis = redis.Redis(host='localhost', port=6379)
ws_redis_clients = {}


class CommandCenter(tornado.web.Application, common.RedisMixin):
    def __init__(self, cc_name, redis_port, cc_pass):
        print('Starting up Command Center ', cc_name)
        self.cc_pass = cc_pass
        self.name = cc_name
        self.db = common.init_redis(redis_port)
        self.ws_dbs = {}
        if not os.path.exists('files'):
            os.makedirs('files')
        signal.signal(signal.SIGINT, self.shutdown)
        super(CommandCenter, self).__init__([
            #(r'/target', TargetHandler),
            #(r'/stream', StreamHandler),
            #(r'/job', JobHandler),
            (r'/register_ws', RegisterWSHandler)
            ])

    def shutdown(self, signal_number, stack_frame):
        self.shutdown_redis()
        print('shutting down command center...')
        tornado.ioloop.IOLoop.instance().stop()
        sys.exit(0)

def start():
    config_file = 'cc_conf'
    Config = ConfigParser.ConfigParser()
    Config.read(config_file)
    # read config file
    cc_name      = Config.get('CC','name')
    redis_port   = Config.getint('CC','redis_port')
    cc_http_port = Config.getint('CC','http_port')
    auth_port = Config.getint('CC','auth_port')
    auth_pass = Config.get('CC','auth_pass')
    cc_instance  = CommandCenter(cc_name,redis_port)
    ws_registrar = tornado.web.Application([
        (r"/register_ws",RegisterWSHandler,
         dict(cc=cc_instance, cc_auth_pass=auth_pass))
                                          ])
    tornado.httpserver.HTTPServer(cc_instance).listen(cc_http_port)
    tornado.httpserver.HTTPServer(ws_registrar).listen(auth_port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    start()

    #cc_redis.flushdb()

    # when CC starts, we need to:
    # rebuild all priority queues. 

    #application.listen(8888, '0.0.0.0')
    #if not os.path.exists('files'):
    #    os.makedirs('files')
    #tornado.ioloop.IOLoop.instance().start()