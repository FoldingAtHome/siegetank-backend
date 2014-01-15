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
import redis
import signal
import sys
import apollo
import time

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
              'http_port': int,  # ws http port
              'redis_port': int,  # ws redis port
              'redis_pass': str,  # ws password
              }

WorkServerDB = {}  # 'name' : redis_client


class Target(apollo.Entity):
    prefix = 'target'
    fields = {'description': str,  # description of the target
              'owner': str,  # owner of the target,
              'steps_per_frame': int,  # number of steps per frame
              'files': {str},  # list of files needed by this target
              'creation_date': float,  # in linux time.time()
              'stage': str,  # disabled, beta, release
              'allowed_ws': {str},  # ws to allow striation on
              'engine': str,  # openmm or gromacs
              'engine_versions': {str},  # allowed core_versions
              }

apollo.relate(Target, 'workservers', {WorkServer})

################
# PG Interface #
################

# POST x.com/targets/add  - add a target
# PUT x.com/target/update_stage -  change the stage of the target
# PUT x.com/targets/delete  - delete a target and all associated streams
# POST x.com/streams/add  - add a stream to a given target
# PUT x.com/streams/delete  - delete a stream
# GET x.com/targets/all  - retrieve a list of all targets
# GET x.com/targets/  - retrieve info about a specific target

##################
# Core Interface #
##################

# POST x.com/jobs/job - get a stream to work on

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


class PostTargetHandler(BaseHandler):
    def post(self):
        ''' POST a new target to the server
            Request:
                {

                    [required]
                    "description": description,
                    "files": {"file1_name": file1_bin_b64,
                              "file2_name": file2_bin_b64,
                              ...
                              }
                    "steps_per_frame": 100000,
                    "engine": "openmm",
                    "engine_versions": ["6.0", "5.5", "5.2"]

                    [optional]
                    # if present, a list of workservers to striate on.
                    "allowed_ws": ["mengsk", "arcturus"]
                    "stage": beta,

                }

            Reply:
                {
                    "target_id": target_id,
                }

        '''
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        files = content['files']

        if content['engine'] != 'openmm':
            return self.write(json.dumps({'error': 'engine must be openmm'}))
        if content['engine_versions'] != ['6.0']:
            return self.write(json.dumps({'error': 'version must be 6.0'}))

        #----------------#
        # verify request #
        #----------------#
        engine = content['engine']
        if content['engine'] == 'openmm':
            required_files = {'system.xml.gz.b64', 'integrator.xml.gz.b64'}
            given_files = set()
            for filename, filebin in files.items():
                given_files.add(filename)
            if given_files != required_files:
                return self.write(json.dumps({'error': 'missing file'}))
        else:
            return self.write(json.dumps({'error': 'unsupported engine'}))

        if 'allowed_ws' in content:
            for ws_name in content['allowed_ws']:
                if not WorkServer.exists(ws_name, self.db):
                    err_msg = {'error': ws_name+' is not connected to this cc'}
                    return self.write(json.dumps(err_msg))
        description = content['description']
        steps_per_frame = content['steps_per_frame']

        if steps_per_frame < 5000:
            return self.write(json.dumps({'error': 'steps_per_frame < 5000'}))

        #------------#
        # write data #
        #------------#
        target_id = str(uuid.uuid4())
        target = Target.create(target_id, self.db)
        target.hset('description', description)
        target.hset('steps_per_frame', steps_per_frame)
        target.hset('creation_date', time.time())
        target.hset('engine', engine)
        target.hset('stage', 'disabled')
        for allowed_version in content['engine_versions']:
            target.sadd('engine_versions', allowed_version)
        if 'allowed_ws' in content:
            for ws_name in content['allowed_ws']:
                target.sadd('allowed_ws', ws_name)

        target_dir = os.path.join('targets', target_id)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        for filename, filebin in files.items():
            file_path = os.path.join(target_dir, filename)
            target.sadd('files', filename)
            with open(file_path, 'wb') as handle:
                handle.write(filebin.encode())

        self.set_status(200)
        response = {'target_id': target_id}

        return self.write(json.dumps(response))

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
            (r'/register_ws', RegisterWSHandler),
            (r'/targets/create', PostTargetHandler)
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