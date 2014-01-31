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
import bcrypt
import common
import pymongo


# The command center manages several work servers in addition to managing the
# stats system for each work server. A single command center manages a single
# engine type (eg. OpenMM). 

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

# [P] POST x.com/auth - Authenticate the user, returning an authorization token

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
              'online': bool,  # denotes if the server is online or not
              }

# note, some of these options are created lazily, ie. they don't take up space
# until created. (yay for noSQL)

class Target(apollo.Entity):
    prefix = 'target'
    fields = {'description': str,  # description of the target
              'owner': str,  # owner of the target,
              'steps_per_frame': int,  # number of steps per frame
              'files': {str},  # files shared by all streams
              'creation_date': float,  # in linux time.time()
              'stage': str,  # disabled, beta, release
              'allowed_ws': {str},  # ws to allow striation on
              'engine': str,  # openmm or terachem
              'engine_versions': {str},  # allowed core_versions
              }

# allow queries to find which targets have a matching engine version
Target.add_lookup('engine_versions', injective=False)
apollo.relate(Target, 'striated_ws', {WorkServer})


def authenticated(method):
    """ Decorate handlers with this that require managers to login. Based off
    of tornado's authenticated method.

    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            self.request.headers['Authorization']
        except:
            self.write(json.dumps({'error': 'missing Authorization header'}))
            return self.set_status(401)

        if self.get_current_user():
            return method(self, *args, **kwargs)
        else:
            return self.set_status(401)

    return wrapper


class AuthHandler(tornado.web.RequestHandler):
    def post(self):
        """ Generate a new authorization token for the user

        Request: {
            'email': proteneer@gmail.com,
            'password': password
        }

        Reply: {
            token: token
        }

        """
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        password = content['password']
        email = content['email']
        mdb = self.application.mdb
        query = mdb.managers.find_one({'_id': email},
                                      fields=['password_hash'])
        stored_hash = query['password_hash']
        if stored_hash == bcrypt.hashpw(password.encode(), stored_hash):
            new_token = str(uuid.uuid4())
            mdb.managers.update({'_id': email}, {'$set': {'token': new_token}})
        else:
            return self.status(401)
        self.set_status(200)
        self.write(json.dumps({'token': new_token}))


class AddManagerHandler(tornado.web.RequestHandler):
    def post(self):
        """ Add a PG member as a Manager.

        Request: {
            'email': proteneer@gmail.com,
            'password': password,
        }

        Reply: {
            token: token
        }

        """
        if self.request.remote_ip != '127.0.0.1':
            return self.set_status(401)
        content = json.loads(self.request.body.decode())
        token = str(uuid.uuid4())
        password = content['password']
        hash_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt())

        db_body = {'_id': content['email'],
                   'password_hash': hash_password,
                   'token': token
                   }

        mdb = self.application.mdb
        managers = mdb.managers
        managers.insert(db_body)

        self.write(json.dumps({'token': token}))


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

    def get_current_user(self):
        header_token = self.request.headers['Authorization']
        mdb = self.application.mdb
        query = mdb.managers.find_one({'token': header_token},
                                      fields=['_id'])
        try:
            return query['_id']
        except:
            return None


class AssignHandler(BaseHandler):
    def post(self):
        """ Get a job assignment.

        Request:
            {
                #"core_id": core_id,
                "engine": "openmm", (lowercase)
                "engine_version": "5.2"
            }

        Reply:
            {
                "token": "6lk2j5-tpoi2p6-poipoi23",
                "url": "https://raynor.stanford.edu:1234/core/start",
                "steps_per_frame": 50000
            }

        Matching algorithm:

        1. "engine" type must match that of the CC's engine type.
        2. A list of targets matching "engine_versions" is determined.

        A cdf function is built with weights of each target. A random target
        is picked from this cdf, and one of its streams is activated. The set
        of striated_ws for this target is found, and a random WS is picked.

        The stream is then activated on this WS.

        """

        self.set_status(400)
        #core_id = self.request.body['core_id']
        content = json.loads(self.request.body.decode())
        engine = content['engine']
        if engine != 'openmm':
            return self.write(json.dumps({'error': 'engine must be openmm'}))
        engine_version = content['engine_version']

        available_targets = Target.lookup('engine_versions', engine_version,
                                          self.db)
        if not available_targets:
            self.write(json.dumps({'error': 'no jobs match engine version'}))
            return

        attempts = 0
        while True and attempts < 3:
            attempts += 1
            # pick a random target from available targets
            target_id = random.sample(available_targets, 1)[0]
            target = Target(target_id, self.db)
            steps_per_frame = target.hget('steps_per_frame')
            # pick a random ws the target is striating over
            ws_name = target.srandmember('striated_ws')
            ws_db = self.application.get_ws_db(ws_name)
            token = str(uuid.uuid4())
            stream_id = ws.WorkServer.activate_stream(target_id, token, ws_db)
            if stream_id:
                workserver = WorkServer(ws_name, self.db)
                ws_url = workserver.hget('url')
                ws_port = workserver.hget('http_port')
                body = {
                    'token': token,
                    'uri': 'https://'+ws_url+':'+str(ws_port)+'/core/start',
                    'steps_per_frame': steps_per_frame
                }
                self.write(json.dumps(body))
                return self.set_status(200)
            else:
                pass

        self.write(json.dumps({'error': 'could not get assignment'}))


class RegisterWSHandler(BaseHandler):
    def put(self):
        """ Called by WS for registration

        Header:
            'Authorization': cc_pass

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

        """
        content = json.loads(self.request.body.decode())
        auth = self.request.headers['Authorization']
        if auth != self.application.cc_pass:
            return self.set_status(401)
        content = json.loads(self.request.body.decode())
        name = content['name']
        url = content['url']
        http_port = content['http_port']
        redis_port = content['redis_port']
        redis_pass = content['redis_pass']
        self.application.add_ws(name, url, http_port, redis_port, redis_pass)
        print('WS '+content['name']+' is now connected')
        self.set_status(200)


class DeleteStreamHandler(BaseHandler):
    @authenticated
    @tornado.gen.coroutine
    def put(self, stream_id):
        """ Deletes a stream from the server

        Request:
            {
                'stream_id': stream_id,
            }

        """
        for ws_name in WorkServer.members(self.db):
            db_client = self.application.get_ws_db(ws_name)
            if ws.Stream.exists(stream_id, db_client):
                picked_ws = WorkServer(ws_name, self.db)
                ws_url = picked_ws.hget('url')
                ws_http_port = picked_ws.hget('http_port')
                client = tornado.httpclient.AsyncHTTPClient()
                body = {
                    'stream_id': stream_id
                }
                rep = yield client.fetch('https://'+str(ws_url)+':'
                                         +str(ws_http_port)+'/streams/delete',
                                         method='PUT', body=json.dumps(body),
                                         validate_cert=common.is_domain(ws_url))
                return self.set_status(rep.code)
            else:
                self.write(json.dumps({'error': 'stream not found'}))
        self.set_status(400)


class PostStreamHandler(BaseHandler):
    @authenticated
    @tornado.gen.coroutine
    def post(self):
        """ POST a new stream to the server

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

        """
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        target_id = content['target_id']
        target = Target(target_id, self.db)
        if target.hget('owner') != self.get_current_user():
            self.set_status(401)
            return self.write(json.dumps({'error': 'target not owned by you'}))
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
        ws_db = self.application.get_ws_db(ws_id)

        body = {
            'target_id': target_id,
        }

        body['stream_files'] = {}

        for filename, filebin in files.items():
            body['stream_files'][filename] = filebin

        if not ws.Target.exists(target_id, ws_db):
            target_files = target.smembers('files')
            body['target_files'] = {}
            for filename in target_files:
                file_path = os.path.join(self.application.targets_folder,
                                         target_id, filename)
                with open(file_path, 'rb') as handle:
                    body['target_files'][filename] = handle.read().decode()

        client = tornado.httpclient.AsyncHTTPClient()
        rep = yield client.fetch('https://'+str(ws_url)+':'+str(ws_http_port)
                                 + '/streams', method='POST',
                                 body=json.dumps(body),
                                 validate_cert=common.is_domain(ws_url))

        self.set_status(rep.code)
        return self.write(rep.body)


class GetTargetHandler(BaseHandler):
    def get(self, target_id):
        """ Retrieve details regarding this target

        Reply:
        {
            "description": description,
            "owner": owner,
            "steps_per_frame": steps_per_frame,
            "creation_date": creation_date,
            "stage": disabled, beta, release
            "allowed_ws": workservers allowed
            "striated_ws": workservers used
            "engine": engine type ('openmm' for now)
            "engine_versions": engine_versions
        }

        """
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
        """ Return a list of streams for the specified target

        Reply:
            {
                'stream1_id': [status, n_frames, ws_name]
                'stream2_id': [status, n_frames, ws_name],
                ...
            }

        """
        self.set_status(400)
        target = Target(target_id, self.db)
        striated_ws = target.smembers('striated_ws')

        body = {}

        for ws_name in striated_ws:
            ws_db = self.application.get_ws_db(ws_name)
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
        """ Return a list of targets on the CC depending on context

        Reply:
            {
                'targets': ['target_id1', 'target_id2', '...']
            }

        If Authorized by a F@h user, it will only return list of targets
        owned by the user.

        """
        streams = Target.members(self.db)
        self.write(json.dumps({'targets': list(streams)}))

    @authenticated
    def post(self):
        """ POST a new target to the server

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

        """
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
        target.hset('owner', self.get_current_user())
        for allowed_version in content['engine_versions']:
            target.sadd('engine_versions', allowed_version)
        if 'allowed_ws' in content:
            for ws_name in content['allowed_ws']:
                target.sadd('allowed_ws', ws_name)

        mdb = self.application.mdb
        cc_id = self.application.name
        mdb.managers.update({'_id': self.get_current_user()},
                            {'$push': {'targets.'+cc_id: target_id}})

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
    #     """ PGI - Fetch details on a target"""
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


class CommandCenter(tornado.web.Application, common.RedisMixin):
    def __init__(self, cc_name, redis_port, redis_pass=None,
                 cc_pass=None, targets_folder='targets', debug=False,
                 mdb_host='localhost', mdb_port=27017, mdb_password=None,
                 appendonly=False):
        print('Starting up Command Center:', cc_name)
        self.cc_pass = cc_pass
        self.name = cc_name
        self.db = common.init_redis(redis_port, redis_pass,
                                    appendonly=appendonly,
                                    appendfilename='aof_'+self.name)
        self.ws_dbs = {}
        self.mdb = pymongo.MongoClient(host=mdb_host, port=mdb_port).users

        self.targets_folder = targets_folder
        #if not os.path.exists('files'):
        #    os.makedirs('files')
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        super(CommandCenter, self).__init__([
            (r'/core/assign', AssignHandler),
            (r'/auth', AuthHandler),
            (r'/managers', AddManagerHandler),
            (r'/targets', TargetHandler),
            (r'/targets/info/(.*)', GetTargetHandler),
            (r'/targets/streams/(.*)', ListStreamsHandler),
            (r'/register_ws', RegisterWSHandler),
            (r'/streams', PostStreamHandler),
            (r'/streams/delete/(.*)', DeleteStreamHandler)
            ], debug=debug)

        self.WorkServerDB = {}

    def _guarded_get(self, name):
        """ Returns a workserver's db client with name if and only if the
        client is still alive.

        """
        ws = WorkServer(name, self.db)
        if ws.hget('online'):
            try:
                # the ws is supposed to be working fine
                self.WorkServerDB[name].ping()  # raise exception otherwise
                return self.WorkServerDB[name]
            except Exception:
                # oh noes, the ws is dead
                ws.hset('online', False)
                return None
        else:
            return None

    def get_ws_db(self, name):
        """ When pre-forking with tornado, a register_ws request gets
        sent to a single process, meaning that the other processes did
        not end up actually calling add_ws() to register the ws.

        However, we can do lazy connects, since all the information pertaining
        to the ws is contained entirely in redis! So we can reconstruct the
        client after the fact, and each process constructs at most one time.

        Returns None if the WS is not available.

        """
        if name in self.WorkServerDB:
            # the workserver has already been added in this process
            return self._guarded_get(name)
        else:
            # see if this WS exists but hasn't been added to the db
            if WorkServer.exists(name, self.db):
                ws = WorkServer(name, self.db)
                url = ws.hget('url')
                redis_port = ws.hget('redis_port')
                redis_pass = ws.hget('redis_pass')
                client = redis.Redis(host=url,
                                     port=redis_port,
                                     password=redis_pass,
                                     decode_responses=True)

                client.ping()
                self.WorkServerDB[name] = client
                return self._guarded_get(name)
            else:
                return None

    def add_ws(self, name, url, http_port, redis_port, redis_pass=None):
        try:
            # make sure the client is alive
            client = redis.Redis(host=url,
                                 port=redis_port,
                                 password=redis_pass,
                                 decode_responses=True)
            client.ping()

            if not WorkServer.exists(name, self.db):
                ws = WorkServer.create(name, self.db)
            else:
                ws = WorkServer(name, self.db)

            ws.hset('url', url)
            ws.hset('http_port', http_port)
            ws.hset('redis_port', redis_port)
            ws.hset('online', True)
            if redis_pass:
                ws.hset('redis_pass', redis_pass)
            self.WorkServerDB[name] = client
        except Exception as e:
            print(e)


def start():

    #######################
    # CC Specific Options #
    #######################
    tornado.options.define('name', type=str)
    tornado.options.define('redis_port', type=int)
    tornado.options.define('redis_pass', type=str)
    tornado.options.define('url', type=str)
    tornado.options.define('internal_http_port', type=int)
    tornado.options.define('external_http_port', type=int)
    tornado.options.define('cc_pass', type=str)
    tornado.options.parse_config_file('cc.conf')

    options = tornado.options.options

    cc_name = options.name
    redis_port = options.redis_port
    redis_pass = options.redis_pass
    internal_http_port = options.internal_http_port
    cc_pass = options.cc_pass

    cc_instance = CommandCenter(cc_name=cc_name,
                                cc_pass=cc_pass,
                                redis_port=redis_port,
                                redis_pass=redis_pass,
                                appendonly=True)

    cc_server = tornado.httpserver.HTTPServer(cc_instance, ssl_options={
        'certfile': 'certs/ws.crt', 'keyfile': 'certs/ws.key'})

    cc_server.bind(internal_http_port)
    cc_server.start(0)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    start()
