import tornado.escape
import tornado.gen
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.httpclient

import json
import os
import uuid
import random
import redis
import signal
import apollo
import time
import ws
import ipaddress
import functools

import common
import psycopg2

# The command center manages several work servers in addition to managing the
# stats system for each work server.

# CC uses Antirez's redis extensively as NoSQL store mainly because of its
# blazing fast speed. Binary blobs such as states, systems, and frames are
# written directly to disk.

# For more information on redis memory usage, visit:
# http://nosql.mypopescu.com/post/1010844204/redis-memory-usage

# On average, we expect the load of the CC to be significantly lower
# than that of the WS. If each stream takes about 4 hours until
# termination, then each stream makes about 6 requests per day.
# A single CC is expected to handle 500,000 streams. This is
# about 3 million requests per day - translating to about 35 requests
# per second. Each request needs to be handled by the CC in 30 ms.

#################
#   Interface   #
#################

# [A] Requires authentication
# [P] Publicly accessible (possibly limited info)

# [A] POST x.com/targets - add a target
# [P] GET x.com/targets - if Authenticated, retrieves User's targets
#                       - if Public, retrieves list of all targets on server
# [P] GET x.com/targets/info/:target_id - get info about a specific target
# [A] PUT x.com/targets/stage/:target_id - change stage from beta->adv->full
# [A] PUT x.com/targets/delete/:target_id - delete target and its streams
# [A] PUT x.com/targets/stop/:target_id - stop the target and its streams
# [A] GET x.com/targets/streams/:target_id - get the streams for the target

# [A] POST x.com/streams - add a stream
# [P] GET x.com/streams/info/:stream_id - get information about specific stream
# [A] PUT x.com/streams/delete/:stream_id - delete a stream
# [A] PUT x.com/streams/stop/:stream_id - stop a stream

##################
# Core Interface #
##################

# POST x.com/core/assign - get a stream to work on

class WorkServer(apollo.Entity):
    prefix = 'ws'
    fields = {'url': str,  # http request url (verify based on if IP or not)
              'http_port': int,  # ws http port
              'redis_port': int,  # ws redis port
              'redis_pass': str,  # ws password
              }


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


#class Pilot(apollo.Entity):
#    prefix = 'pilot'
#    fields = {'token': str}

# each token maps to at most one user, old token is deleted
# Pilot.add_lookup('token', injective=True)
# apollo.relate(Target, 'striated_ws', {WorkServer})

def authenticated(method):
    """ Decorate methods with this that require users to login. Based off of
    tornado's authenticated method.

    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        # disable authentication requirements if in debug mode (eg. unit tests)
        if self.application.settings.get('debug'):
            return method(self, *args, **kwargs)
        token = self.request.headers['Authorization']
        
        token):
            # check against redis first
            return method(self, *args, **kwargs)
        else:
            return self.set_status(401)
    return wrapper


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db


class RegisterWSHandler(BaseHandler):
    def put(self):
        ''' Called by WS for registration
        Request:
            {
                'name': unique_name_of_the_cc
                'url': url for requests (IP or DNS)
                'http_port': http port
                'redis_pass': redis password
                'redis_port': redis port
            }

        Response:
            {
                200 - OK
            }
        '''
        content = json.loads(self.request.body.decode())
        auth = content['auth']
        if auth != self.application.cc_pass:
            return self.set_status(401)
        content = json.loads(self.request.body.decode())
        name = content['name']
        url = content['url']
        http_port = content['http_port']
        redis_port = content['redis_port']
        redis_pass = content['redis_pass']
        self.application.add_ws(name, url, http_port, redis_port, redis_pass)
        self.set_status(200)


def _is_domain(url):
    ''' returns True if url is a domain '''
    try:
        ipaddress.ip_address(url)
        return False
    except Exception:
        return True


class PostStreamHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        ''' POST a new stream to the server
            Request:
                {
                    [required]
                    "target_id": target_id,
                    "files": {"file1_name": file1_bin_b64,
                              "file2_name": file2_bin_b64,
                              ...
                              }
                }

            Reply:
                {
                    "stream_id": stream_id
                }

        '''

        self.set_status(400)
        content = json.loads(self.request.body.decode())
        target_id = content['target_id']
        target = Target(target_id, self.db)
        files = content['files']
        for filename in files:
            if target.hget('engine') == 'openmm':
                if filename != 'state.xml.gz.b64':
                    return self.write(json.dumps({'error': 'bad_filename'}))

        # TODO: ensure the WS we're POSTing to is up
        allowed_workservers = target.smembers('allowed_ws')
        if not allowed_workservers:
            allowed_workservers = WorkServer.members(self.db)

        # randomly pick from available workservers
        ws_id = random.sample(allowed_workservers, 1)[0]
        target.sadd('striated_ws', ws_id)
        picked_ws = WorkServer(ws_id, self.db)

        ws_url = picked_ws.hget('url')
        ws_http_port = picked_ws.hget('http_port')

        body = {
            'target_id': target_id,
        }

        body['stream_files'] = {}

        for filename, filebin in files.items():
            body['stream_files'][filename] = filebin

        if not ws.Target.exists(target_id,
                                self.application.WorkServerDB[picked_ws.id]):
            target_files = target.smembers('files')
            body['target_files'] = {}
            for filename in target_files:
                file_path = os.path.join(self.application.targets_folder,
                                         target_id, filename)
                with open(file_path, 'rb') as handle:
                    body['target_files'][filename] = handle.read().decode()

        client = tornado.httpclient.AsyncHTTPClient()
        rep = yield client.fetch('https://'+str(ws_url)+':'+str(ws_http_port)
                                 +'/streams',
                                 method='POST',
                                 body=json.dumps(body),
                                 validate_cert=_is_domain(ws_url))

        # client = tornado.httpclient.HTTPClient()
        # rep = client.fetch('https://'+str(ws_ip)+':'+str(ws_http_port)
        #                    +'/streams', method='POST', validate_cert=False)
        # client.close()

        self.set_status(rep.code)
        return self.write(rep.body)


class GetTargetHandler(BaseHandler):
    def get(self, target_id):
        ''' Retrieve details regarding this target

        Reply:
        {
            'description': description,
            'owner': owner,
            'steps_per_frame': steps_per_frame,
            'creation_date': creation_date,
            'stage': disabled, beta, release
            'allowed_ws': workservers allowed
            'striated_ws': workservers used
            'engine': engine_type (usually openmm)
            'engine_versions': engine_versions
        }

        '''
        self.set_status(400)
        target = Target(target_id, self.db)

        # get a list of streams

        body = {
            'description': target.hget('description'),
            'owner': target.hget('owner'),
            'steps_per_frame': target.hget('steps_per_frame'),
            'creation_date': target.hget('creation_date'),
            'stage': target.hget('stage'),
            'allowed_ws': list(target.smembers('allowed_ws')),
            'striated_ws': list(target.smembers('striated_ws')),
            'engine': target.hget('engine'),
            'engine_versions': list(target.smembers('engine_versions'))
            }
        self.set_status(200)
        self.write(json.dumps(body))


class ListStreamsHandler(BaseHandler):
    def get(self, target_id):
        ''' Return a list of streams for the specified target

        Reply:
            {
                'stream1_id': [status, n_frames, ws_name],
                'stream2_id': [status, n_frames, ws_name],
                ...
            }

        '''
        self.set_status(400)
        target = Target(target_id, self.db)
        striated_ws = target.smembers('striated_ws')

        body = {}

        for ws_name in striated_ws:
            ws_db = self.application.WorkServerDB[ws_name]
            ws_target = ws.Target(target_id, ws_db)
            stream_ids = ws_target.smembers('streams')
            for stream_id in stream_ids:
                body[stream_id] = []
                stream = ws.Stream(stream_id, ws_db)
                body[stream_id].append(stream.hget('status'))
                body[stream_id].append(stream.hget('frames'))
                body[stream_id].append(ws_name)

        self.set_status(200)
        self.write(json.dumps(body))


class TargetHandler(BaseHandler):
    def get(self):
        ''' Return a list of targets on the CC depending on context

        Reply:
            {
                'targets': ['target_id1', 'target_id2', '...']
            }

        If Authorized by a F@h user, it will only return list of targets
        owned by the user.

        '''
        streams = Target.members(self.db)
        self.write(json.dumps({'targets': list(streams)}))

    @authenticated
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
        target.hset('owner', 'NotImplemented')
        for allowed_version in content['engine_versions']:
            target.sadd('engine_versions', allowed_version)
        if 'allowed_ws' in content:
            for ws_name in content['allowed_ws']:
                target.sadd('allowed_ws', ws_name)

        target_dir = os.path.join(self.application.targets_folder, target_id)
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

    # def get(self):
    #     ''' PGI - Fetch details on a target'''
    #     user = 'yutong'
    #     response = []
    #     targets = cc_redis.smembers(user+':targets')
    #     for target_id in targets:
    #         prop = {}

    #         stamp = datetime.datetime.fromtimestamp(float(cc_redis.hget(target_id,'date')))
    #         prop['date'] = stamp.strftime('%m/%d/%Y, %H:%M:%S')
    #         prop['description'] = cc_redis.hget(target_id,'description')
    #         prop['frames'] = random.randint(0,200)
    #         response.append(prop)

    #     return self.write(json.dumps(response,indent=4, separators=(',', ': ')))

    # def delete(self):
    #     # delete all streams

    #     # delete the project
    #     return

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


class CommandCenter(tornado.web.Application, common.RedisMixin):
    def __init__(self, cc_name, redis_port,
                 cc_pass=None, targets_folder='targets', debug=False):
        print('Starting up Command Center:', cc_name)
        self.cc_pass = cc_pass
        self.name = cc_name
        self.db = common.init_redis(redis_port, cc_pass)
        self.ws_dbs = {}
        self.targets_folder = targets_folder
        if not os.path.exists('files'):
            os.makedirs('files')
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        if debug:
            self.stats_conn = psycopg2.connect("dbname='fahdb' user='postgres'\
                              password='testpass' host='localhost'")

        super(CommandCenter, self).__init__([
            (r'/targets/info/(.*)', GetTargetHandler),
            (r'/targets/streams/(.*)', ListStreamsHandler),
            (r'/register_ws', RegisterWSHandler),
            (r'/targets', TargetHandler),
            (r'/streams', PostStreamHandler)
            ], debug=debug)



        self.WorkServerDB = {}

    def add_ws(self, name, url, http_port, redis_port, redis_pass=None):
        client = redis.Redis(host=url, port=redis_port, password=redis_pass,
                             decode_responses=True)
        client.ping()

        if not WorkServer.exists(name, self.db):
            ws = WorkServer.create(name, self.db)
        else:
            ws = WorkServer(name, self.db)

        ws.hset('url', url)
        ws.hset('http_port', http_port)
        ws.hset('redis_port', redis_port)
        ws.hset('redis_pass', redis_pass)
        self.WorkServerDB[name] = client

    def cleanup_ws_dbs(self):
        for db in self.WorkServerDB:
            del db

    def shutdown(self, **kwargs):
        super(CommandCenter, self).shutdown(**kwargs)
        for db_name, db_client in self.WorkServerDB.items():
            db_client.connection_pool.disconnect()

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