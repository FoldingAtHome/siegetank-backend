# Tornado-powered command center backend.
#
# Authors: Yutong Zhao <proteneer@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

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
import time
import functools
import bcrypt
import pymongo

from server.common import RedisMixin, init_redis, is_domain
from server.apollo import Entity, relate
import server.ws

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

# [P] POST x.com/managers/auth - Authenticate the F@h manager
# [P] POST x.com/donors/auth - Authenticate the F@h donor

# [A] POST x.com/targets - add a target
# [P] GET x.com/targets - if Authenticated, retrieves User's targets
#                       - if Public, retrieves list of all targets on server
# [P] GET x.com/targets/info/:target_id - get info about a specific target
# [A] PUT x.com/targets/stage/:target_id - change stage from beta->adv->full
# [A] PUT x.com/targets/delete/:target_id - delete target and its streams
# [A] PUT x.com/targets/stop/:target_id - stop the target and its streams
# [A] GET x.com/targets/streams/:target_id - get the streams for target
# [A] PUT x.com/targets/update_stage/:target_id

# [A] POST x.com/streams - add a stream
# [P] GET x.com/streams/info/:stream_id - get information about specific stream
# [A] PUT x.com/streams/delete/:stream_id - delete a stream
# [A] PUT x.com/streams/stop/:stream_id - stop a stream

##################
# Core Interface #
##################

# POST x.com/core/assign - get a stream to work on


class WorkServer(Entity):
    prefix = 'ws'
    fields = {'url': str,  # http request url (verify based on if IP or not)
              'http_port': int,  # ws http port
              'online': bool,  # denotes if the server is online or not
              }


# note, some of these options are created lazily, ie. they don't take up space
# until created. (yay for noSQL)
class Target(Entity):
    prefix = 'target'
    fields = {'description': str,  # description of the target
              'owner': str,  # owner of the target,
              'steps_per_frame': int,  # number of steps per frame
              'files': {str},  # files shared by all streams
              'creation_date': float,  # in linux time.time()
              'stage': str,  # private, beta, public
              'allowed_ws': {str},  # ws to allow striation on
              'engine': str,  # openmm or terachem
              'engine_versions': {str},  # allowed core_versions
              }

# allow queries to find which targets have a matching engine version
Target.add_lookup('engine_versions', injective=False)
relate(Target, 'striated_ws', {WorkServer})


def authenticated(method):
    """ Decorator for handlers that require manager authentication. Based off
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


class AuthDonorHandler(tornado.web.RequestHandler):
    def post(self):
        """ Generate a new authorization token for the donor

        Request: {
            "username": userid,
            "password": password
        }

        Reply: {
            "token": token
        }

        """
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        username = content['username']
        password = content['password']
        mdb = self.application.mdb
        query = mdb.donors.find_one({'_id': username},
                                    fields=['password_hash'])
        stored_hash = query['password_hash']
        if stored_hash == bcrypt.hashpw(password.encode(), stored_hash):
            new_token = str(uuid.uuid4())
            mdb.donors.update({'_id': username},
                              {'$set': {'token': new_token}})
        else:
            return self.status(401)
        self.set_status(200)
        self.write(json.dumps({'token': new_token}))


class AddDonorHandler(tornado.web.RequestHandler):
    def post(self):
        """ Add a F@H Donor

        Request: {
            "username": jesse_v,
            "password": jesse's password
            "email": 'jv@gmail.com'
        }

        reply: {
            "token": token;
        }

        The donor can optionally choose to use token as a commandline arg to
        when starting the cores. This way, all work is then associated with
        the donor.

        """
        self.set_status(400)
        if self.request.remote_ip != '127.0.0.1':
            return self.set_status(401)
        content = json.loads(self.request.body.decode())
        username = content['username']
        password = content['password']
        email = content['email']
        donors = self.application.mdb.donors
        # see if email exists:
        query = donors.find_one({'email': email})
        if query:
            return self.write(json.dumps({'error': 'email exists in db!'}))
        hash_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
        token = str(uuid.uuid4())
        db_body = {'_id': username,
                   'password_hash': hash_password,
                   'token': token,
                   'email': email}

        try:
            donors.insert(db_body)
        except:
            return self.write(json.dumps({'error': username+' exists'}))

        self.set_status(200)
        self.write(json.dumps({'token': token}))


class AuthManagerHandler(tornado.web.RequestHandler):
    def post(self):
        """ Generate a new authorization token for the user

        Request: {
            "email": proteneer@gmail.com,
            "password": password
        }

        Reply: {
            "token": token
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
            "email": proteneer@gmail.com,
            "password": password,
        }

        Reply: {
            "token": token
        }

        """
        self.set_status(400)
        if self.request.remote_ip != '127.0.0.1':
            return self.set_status(401)
        content = json.loads(self.request.body.decode())
        token = str(uuid.uuid4())
        email = content['email']
        password = content['password']
        hash_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
        db_body = {'_id': email,
                   'password_hash': hash_password,
                   'token': token
                   }
        managers = self.application.mdb.managers
        try:
            managers.insert(db_body)
        except pymongo.errors.DuplicateKeyError:
            self.write(json.dumps({'error': email+' exists'}))
            return

        self.set_status(200)
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


class UpdateStageHandler(BaseHandler):
    @authenticated
    def put(self, target_id):
        """ Update the status of the target

        Request:
            {
                "stage": private, beta, public
            }

        """


def yates_generator(x):
    for i in range(len(x)-1, -1, -1):
        j = random.randrange(i + 1)
        x[i], x[j] = x[j], x[i]
        yield x[i]


class AssignHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /core/assign

            Initialize a stream assignment from the CC.

            **Example request**

            .. sourcecode:: javascript

                {
                    "engine": "openmm",
                    "engine_version": "6.0",
                    "donor_token": "token",

                    "stage": "beta" // optional, allow access to beta targets
                    "target_id": "target_id" // optional
                }

            If a `target_id` is specified, then the WS will try and activate a
            stream corresponding to the `target_id`. In addition, the stage of
            the target is not taken into consideration. Note that `target_id`
            must be the fully qualified 36 digit uuid.

            Otherwise, we try and find a `target_id` whose:

            Engine version is compatible with the core's engine version and
            `stage` is either "beta" or "public".

            **Example reply**

            .. sourcecode:: javascript

                {
                    "token": "6lk2j5-tpoi2p6-poipoi23",
                    "url": "https://raynor.stanford.edu:1234/core/start",
                    "steps_per_frame": 50000
                }

            :statuscode 200: No error
            :statuscode 400: Bad request

        """

        # The assignment algorithm is then applied:

        # Proposed algorithm:

        # Let:
        # 0 <= target.n_streams
        # 0 <= vijay.weight < 10
        # 1 <= a <= b

        # Then:
        # target.weight = target.n_streams^(2/3)*vijay.weight

        # order and compute the cdf over all targets, then sample x ~ U(0,1]
        # and see which target maps to x.

        self.set_status(400)
        #core_id = self.request.body['core_id']
        content = json.loads(self.request.body.decode())
        if 'donor_token' in content:
            donor_token = content['donor_token']
            mdb = self.application.mdb
            query = mdb.donors.find_one({'token': donor_token},
                                        fields=['_id'])
            if not query:
                return self.write(json.dumps({'error': 'bad donor token'}))
            donor_id = query['_id']
        else:
            donor_id = None

        engine = content['engine']
        if engine != 'openmm':
            return self.write(json.dumps({'error': 'engine must be openmm'}))
        engine_version = content['engine_version']

        available_targets = list(Target.lookup('engine_versions',
                                               engine_version, self.db))

        allowed_stages = ['public']

        if 'stage' in content:
            if content['stage'] == 'beta':
                allowed_stages.append('beta')
            else:
                return self.write(json.dumps({'error': 'stage must be beta'}))

        if 'target_id' in content:
            # make sure the given target_id can be sent to this core
            target_id = content['target_id']
            if not Target.exists(target_id, self.db):
                err = 'given target is not managed by this cc'
                return self.write(json.dumps({'error': err}))
            if not target_id in available_targets:
                err = 'requested target_id not in available targets'
                return self.write(json.dumps({'error': err}))
            target = Target(target_id, self.db)
        else:
            # if no target is specified, then a random target is chosen from a
            # list of available targets for the core's engine version
            if not available_targets:
                err_msg = 'no available targets matching engine version'
                return self.write(json.dumps({'error': err_msg}))

            found = False

            # target_id must be either beta or public
            for target_id in yates_generator(available_targets):
                target = Target(target_id, self.db)
                if target.hget('stage') in allowed_stages:
                    found = True

            # if we reached here then we didn't find a good target
            if not found:
                err = 'no public or beta targets available'
                return self.write(json.dumps({'error': err}))

        target = Target(target_id, self.db)
        steps_per_frame = target.hget('steps_per_frame')

        # shuffle and re-order the list of striated servers
        striated_servers = list(target.smembers('striated_ws'))
        random.shuffle(striated_servers)

        for ws_name in striated_servers:
            workserver = WorkServer(ws_name, self.db)
            ws_url = workserver.hget('url')
            ws_port = workserver.hget('http_port')
            ws_body = {}
            ws_body['target_id'] = target_id
            if donor_id:
                ws_body['donor_id'] = donor_id
            client = tornado.httpclient.AsyncHTTPClient()
            ws_uri = 'https://'+ws_url+':'+str(ws_port)+'/streams/activate'
            try:
                reply = yield client.fetch(ws_uri,
                                           validate_cert=is_domain(ws_url),
                                           method='POST',
                                           body=json.dumps(ws_body))
                if(reply.code == 200):
                    rep_content = json.loads(reply.body.decode())
                    token = rep_content["token"]
                    body = {
                        'token': token,
                        'steps_per_frame': steps_per_frame,
                        'uri': 'https://'+ws_url+':'+str(ws_port)+'/core/start'
                    }
                    self.write(json.dumps(body))
                    return self.set_status(200)
            except tornado.httpclient.HTTPError as e:
                print(e)
                pass

        self.write(json.dumps({'error': 'no free WS available'}))


class RegisterWSHandler(BaseHandler):
    def put(self):
        """ Called by WS for registration

        Header:
            "Authorization": cc_pass

        Request:
            {
                "name": unique_name_of_the_cc
                "url": url for requests (IP or DNS)
                "http_port": http port
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
        """ Deletes a stream from the server. If the stream was found on one of
        the workservers, then status 200 is returned. Otherwise, 400.

        Request:
            {
                "stream_id": stream_id,
            }

        """
        # this is a relatively slow method. Partially because 1) we don't know
        # which server the stream is on, and 2) we don't know its target so
        # we don't know which servers its striating over
        self.set_status(400)
        found = False
        for ws_name in WorkServer.members(self.db):
            picked_ws = WorkServer(ws_name, self.db)
            ws_url = picked_ws.hget('url')
            ws_http_port = picked_ws.hget('http_port')
            body = {
                "stream_id": stream_id
            }
            client = tornado.httpclient.AsyncHTTPClient()
            rep = yield client.fetch('https://'+str(ws_url)+':'
                                     +str(ws_http_port)+'/streams/delete',
                                     method='PUT', body=json.dumps(body),
                                     validate_cert=is_domain(ws_url))
            if rep.code == 200:
                found = True
        if found:
            self.set_status(200)
            self.write(json.dumps({}))
        else:
            self.write(json.dumps({'error': 'stream not found'}))


class PostStreamHandler(BaseHandler):
    @authenticated
    @tornado.gen.coroutine
    def post(self):
        """ POST a new stream to the server

        When a stream is POSTED, we do a check to make sure that the set of
        {stream_files} U {target_files} is sufficient for the engine type.

        It now becomes possible for streams to have different xml files

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
        if not allowed_workservers:
            self.set_status(400)
            self.write(json.dumps({'error': 'no available workserver'}))

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

        # see if this target is striating over the ws
        if ws_id in target.smembers('striated_ws'):
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
                                 validate_cert=is_domain(ws_url))

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
    @tornado.gen.coroutine
    def get(self, target_id):
        """ Return a list of streams for the specified target

        Reply:
            {
                'ws_name': {
                    stream1_id: {
                        'status': OK
                        'frames': 253,
                    }
                    ...
                }
                ...
            }

        """
        self.set_status(400)
        target = Target(target_id, self.db)
        striated_ws = target.smembers('striated_ws')

        body = {}

        for ws_name in striated_ws:
            ws = WorkServer(ws_name, self.db)
            ws_url = ws.hget('url')
            ws_port = ws.hget('http_port')

            client = tornado.httpclient.AsyncHTTPClient()
            ws_uri = 'https://'+ws_url+':'+str(ws_port)+'/targets/streams/'+\
                     target_id
            reply = yield client.fetch(ws_uri, validate_cert=is_domain(ws_url),
                                       method='GET')

            if reply.code == 200:
                body[ws_name] = json.loads(reply.body.decode())

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
        """
        .. http:post:: /targets

            Add a new target to be managed by this command center.

            **Example request**

            .. sourcecode:: javascript

                {
                    "files": {"file1_name": file1_bin_b64,
                              "file2_name": file2_bin_b64,
                              ...
                              }
                    "steps_per_frame": 100000,
                    "engine": "openmm",
                    "engine_versions": ["6.0", "5.5", "5.2"],

                    "allowed_ws": ["mengsk", "arcturus"], // optional
                    "stage": "private", "beta", or "public" // optional
                }

            allowed_ws is a list of workservers to striate over. If no "stage"
            is given, then the target's default stage is private.

            **Example reply**

            .. sourcecode:: javascript

                {
                    "target_id": "uuid4"
                }

            :statuscode 200: No error
            :statuscode 400: Bad request


        """
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        files = content['files']

        if content['engine'] == 'openmm':
            if content['engine_versions'] != ['6.0']:
                return self.write(json.dumps({'error': 'version must be 6.0'}))
        elif content['engine'] == 'terachem':
            if content['engine_versions'] != ['1.0']:
                return self.write(json.dumps({'error': 'version msut be 1.0'}))
        else:
            return self.write(json.dumps({'error': 'unsupported engine'}))

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
        if 'stage' in content:
            if content['stage'] in ['private', 'beta', 'public']:
                target.hset('stage', content['stage'])
        else:
            target.hset('stage', 'private')
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

    # def delete(self):
    #     # delete all streams

    #     # delete the project
    #     return


class CommandCenter(tornado.web.Application, RedisMixin):
    def __init__(self, cc_name, redis_port, redis_pass=None,
                 cc_pass=None, targets_folder='targets', debug=False,
                 mdb_host='localhost', mdb_port=27017, mdb_password=None,
                 appendonly=False):
        print('Starting up Command Center:', cc_name)
        self.cc_pass = cc_pass
        self.name = cc_name
        self.db = init_redis(redis_port, redis_pass,
                             appendonly=appendonly,
                             appendfilename='aof_'+self.name)
        self.ws_dbs = {}
        self.mdb = pymongo.MongoClient(host=mdb_host, port=mdb_port).users

        # set up indexes
        self.mdb.donors.ensure_index("token")

        self.targets_folder = targets_folder
        #if not os.path.exists('files'):
        #    os.makedirs('files')
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        super(CommandCenter, self).__init__([
            (r'/core/assign', AssignHandler),
            (r'/managers/auth', AuthManagerHandler),
            (r'/managers', AddManagerHandler),
            (r'/donors/auth', AuthDonorHandler),
            (r'/donors', AddDonorHandler),
            (r'/targets', TargetHandler),
            (r'/targets/info/(.*)', GetTargetHandler),
            (r'/targets/streams/(.*)', ListStreamsHandler),
            (r'/register_ws', RegisterWSHandler),
            (r'/streams', PostStreamHandler),
            (r'/streams/delete/(.*)', DeleteStreamHandler)
            ], debug=debug)

        self.WorkServerDB = {}

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
            ws.hset('online', True)
        except Exception as e:
            print(e)


def start():

    #######################
    # CC Specific Options #
    #######################
    tornado.options.define('name', type=str)
    tornado.options.define('redis_port', type=int)
    tornado.options.define('redis_pass', type=str)
    tornado.options.define('internal_http_port', type=int)
    tornado.options.define('external_http_port', type=int)
    tornado.options.define('cc_pass', type=str)
    tornado.options.define('ssl_certfile', type=str)
    tornado.options.define('ssl_key', type=str)
    tornado.options.define('ssl_ca_certs', type=str)
    conf_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', 'cc.conf')
    tornado.options.define('config_file', default=conf_path, type=str)

    tornado.options.parse_command_line()
    options = tornado.options.options
    tornado.options.parse_config_file(options.config_file)
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

    cert_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', options.ssl_certfile)
    key_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            '..', options.ssl_key)
    ca_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           '..', options.ssl_ca_certs)

    cc_server = tornado.httpserver.HTTPServer(cc_instance, ssl_options={
        'certfile': cert_path, 'keyfile': key_path, 'ca_certs': ca_path})

    cc_server.bind(internal_http_port)
    cc_server.start(0)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    start()
