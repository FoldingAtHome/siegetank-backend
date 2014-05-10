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
import time
import bcrypt
import pymongo
import io

from server.common import BaseServerMixin, is_domain, configure_options
from server.apollo import Entity


class SCV(Entity):
    prefix = 'scv'
    fields = {'host': str,  # http request url (verify based on if IP or not)
              'fail_count': int,  # number of times a request has failed
              'password': str,
              }


class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")

    def initialize(self):
        self.fetch = self.application.fetch

    def error(self, message, code=400):
        """ Write a message to the output buffer """
        self.set_status(code)
        self.write({'error': message})

    @property
    def db(self):
        return self.application.db

    @property
    def mdb(self):
        return self.application.mdb

    @property
    def motor(self):
        return self.application.motor

    @tornado.gen.coroutine
    def get_current_user(self):
        try:
            header_token = self.request.headers['Authorization']
        except KeyError:
            return None
        managers = self.motor.users.managers
        query = yield managers.find_one({'token': header_token},
                                        fields=['_id'])
        if query:
            return query['_id']
        else:
            return None

    # TODO: refactor to common
    @tornado.gen.coroutine
    def get_user_role(self):
        try:
            token = self.request.headers['Authorization']
        except KeyError:
            if self.request.remote_ip == '127.0.0.1':
                return 'admin'
            else:
                return None
        cursor = self.motor.users.managers
        query = yield cursor.find_one({'token': token}, fields=['role'])
        try:
            return query['role']
        except:
            return None


class DonorAuthHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /managers/auth

            Generate a new authorization token for the donor

            **Example request**

            .. sourcecode:: javascript

                {
                    "username": "JesseV",
                    "password": "some_password"
                }

            **Example reply**

            .. sourcecode:: javascript

                {
                    "token": "uuid_token"
                }

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        username = content['username']
        password = content['password']
        donors = self.motor.community.donors
        query = yield donors.find_one({'_id': username},
                                      fields=['password_hash'])
        stored_hash = query['password_hash']
        if stored_hash == bcrypt.hashpw(password.encode(), stored_hash):
            new_token = str(uuid.uuid4())
            donors.update({'_id': username},
                          {'$set': {'token': new_token}})
        else:
            return self.status(401)
        self.set_status(200)
        self.write({'token': new_token})


class DonorsHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        """ Add a F@H Donor

        Request: {
            "username": "jesse_v",
            "password": "jesse's password"
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
        cursor = self.motor.community.donors
        # see if email exists:
        query = yield cursor.find_one({'email': email})
        if query:
            return self.error('email exists in db!')
        hash_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
        token = str(uuid.uuid4())
        db_body = {'_id': username,
                   'password_hash': hash_password,
                   'token': token,
                   'email': email}

        try:
            yield cursor.insert(db_body)
        except:
            return self.error(username+' exists')

        self.set_status(200)
        self.write({'token': token})


class ManagerVerifyHandler(BaseHandler):
    @tornado.gen.coroutine
    def get(self):
        """
        .. http:post:: /managers/validate

            Validate an authentication token.

            :reqheader Authorization: access token of a manager

            :status 200: OK
            :status 400: Unauthorized

        """
        self.set_status(400)
        current_user = yield self.get_current_user()
        if not current_user:
            return self.error('Bad credentials', code=401)
        else:
            return self.set_status(200)


class ManagerAuthHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /managers/auth

            Generate a new authorization token for the manager

            **Example request**

            .. sourcecode:: javascript

                {
                    "email": "proteneer@gmail.com",
                    "password": "my_password"
                }

            **Example reply**

            .. sourcecode:: javascript

                {
                    "token": "uuid token"
                }

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        password = content['password']
        email = content['email']
        cursor = self.motor.users.managers
        query = yield cursor.find_one({'_id': email}, fields=['password_hash'])
        stored_hash = query['password_hash']
        if stored_hash == bcrypt.hashpw(password.encode(), stored_hash):
            new_token = str(uuid.uuid4())
            cursor.update({'_id': email}, {'$set': {'token': new_token}})
        else:
            return self.status(401)
        self.set_status(200)
        self.write({'token': new_token})


class ManagersHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /managers

            Add a manager.

            :reqheader Authorization: access token of an administrator

            **Example request**

            .. sourcecode:: javascript

                {
                    "email": "proteneer@gmail.com",
                    "password": "password",
                    "role": "admin" or "manager",
                    "weight": 1
                }

            **Example reply**

            .. sourcecode:: javascript

                {
                    "token": "token"
                }

            :status 200: OK
            :status 400: Bad request
            :status 401: Unauthorized

        """
        self.set_status(400)
        if(yield self.get_user_role()) != 'admin':
            return self.set_status(401)
        elif self.request.remote_ip != '127.0.0.1':
            return self.set_status(401)

        content = json.loads(self.request.body.decode())
        token = str(uuid.uuid4())
        email = content['email']
        password = content['password']
        weight = content['weight']
        role = content['role']
        if not role in ['admin', 'manager']:
            return self.error('bad user role')
        hash_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
        db_body = {'_id': email,
                   'password_hash': hash_password,
                   'token': token,
                   'role': role,
                   'weight': weight
                   }

        cursor = self.motor.users.managers
        try:
            yield cursor.insert(db_body)
        except pymongo.errors.DuplicateKeyError:
            return self.error(email+' exists')

        self.set_status(200)
        self.write({'token': token})


class TargetUpdateHandler(BaseHandler):
    @tornado.gen.coroutine
    def put(self, target_id):
        """
        .. http:put:: /targets/update/:target_id

            Update certain fields of ``target_id``

            :reqheader Authorization: Manager’s authorization token

            **Example request**

            .. sourcecode:: javascript

                {
                    "stage": "disabled", private", "beta", "public"  //optional
                    "engines": ["openmm", "openmm_cluster"]  //optional
                    "weight": 2 // optional
                    "options": {
                        "description": "omgwtfbbq", // optional
                        "title": "DHFR",
                        "category": "wow",
                        "steps_per_frame": 50000
                    } // optional
                }

                .. note:: modifying ``stage`` only affects future assignments.
                    If you wish to stop the streams, you must explicitly stop
                    them.

                .. note:: generally you want to avoid manually modifying the
                    ``shards`` field. It should only modified when an SCV is
                    permanently disabled for whatever reason. If you detach
                    an SCV, then you are manually responsible for the cleanup
                    unless you re-attach it.

                .. note:: fields in ``options`` will be updated if present,
                    otherwise a new field will be added. Note that it is not
                    possible to delete fields inside ``options``, so you must
                    take care that the names are correct.

            **Example reply**

            :status 200: OK
            :status 400: Bad request
            :status 401: Unauthorized

        """
        self.set_status(400)
        current_user = yield self.get_current_user()
        if not current_user:
            self.error('Bad credentials', code=401)
        content = json.loads(self.request.body.decode())
        payload = {}
        if 'engines' in content:
            payload['engines'] = content['engines']
        if 'stage' in content:
            if content['stage'] in ['disabled', 'private', 'beta', 'public']:
                payload['stage'] = content['stage']
            else:
                return self.error('invalid stage')
        if 'weight' in content:
            payload['weight'] = max(content['weight'], 0)
        if 'options' in content:
            for key, value in content['options'].items():
                payload['options.'+key] = value
        cursor = self.motor.data.targets
        result = yield cursor.update({'_id': target_id}, {'$set': payload})
        if result['updatedExisting']:
            self.set_status(200)
        else:
            self.error('invalid '+target_id)


def yates_generator(x):
    for i in range(len(x)-1, -1, -1):
        j = random.randrange(i + 1)
        x[i], x[j] = x[j], x[i]
        yield x[i]


class EngineKeysHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /engines/keys

            Add a new core key for the specified engine. The corresponding core
            must identify itself using the token.

            :reqheader Authorization: access token of an administrator

            **Example request**

            .. sourcecode:: javascript

                {
                    "engine": "openmm_60_opencl", // required
                    "description": "some string", // required
                }

            **Example reply**

            .. sourcecode:: javascript

                {
                    "key": "uuid4"
                }

            :status 200: OK
            :status 400: Bad request
            :status 401: Unauthorized

        """
        if(yield self.get_user_role()) != 'admin':
            return self.error('Bad credentials', 401)
        content = json.loads(self.request.body.decode())
        for required_key in ['engine', 'description']:
            if required_key not in content:
                return self.error('missing: '+required_key)
        stored_id = str(uuid.uuid4())
        content['_id'] = stored_id
        content['creation_date'] = time.time()
        cursor = self.motor.engines.keys
        yield cursor.insert(content)
        self.set_status(200)
        self.write({'key': stored_id})

    @tornado.gen.coroutine
    def get(self):
        """
        .. http:get:: /engines/keys

            Retrieve a list of core keys for all the engines.

            :reqheader Authorization: access token of an administrator

            **Example reply**

            .. sourcecode:: javascript

                {
                    "core_key_1": {
                        "engine": "openmm_55_opencl",
                        "description": "all platforms",
                        "creation_date": 874389297.4,
                    },
                    "core_key_1": {
                        "engine": "openmm_60_opencl",
                        "description": "all platforms",
                        "creation_date": 874389291.4,
                    },
                    "core_key_2": {
                        "engine": "openmm_60_cuda",
                        "description": "all platforms",
                        "creation_date": 538929304.4,
                    },
                }

            :status 200: OK
            :status 400: Bad request
            :status 401: Unauthorized

        """
        if(yield self.get_user_role()) != 'admin':
            return self.error('Bad credentials', 401)
        self.set_status(400)
        body = dict()
        cursor = self.motor.engines.keys
        results = cursor.find()
        while(yield results.fetch_next):
            document = results.next_object()
            core_key = document['_id']
            document.pop('_id')
            body[core_key] = document
        self.set_status(200)
        self.write(body)


class EngineKeysDeleteHandler(BaseHandler):
    @tornado.gen.coroutine
    def put(self, core_key):
        """
        .. http:put:: /engines/keys/delete/:key_id

            Delete a specific core key ``key_id``.

            :reqheader Authorization: access token of an administrator

            :status 200: OK
            :status 400: Bad request
            :status 401: Unauthorized

        """
        if(yield self.get_user_role()) != 'admin':
            return self.error('Bad credentials', 401)
        self.set_status(400)
        cursor = self.motor.engines.keys
        result = yield cursor.remove({'_id': core_key})
        if result['n'] > 0:
            self.set_status(200)
        else:
            return self.error('engine key not found')


class CoreAssignHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /core/assign

            Assign a stream from an SCV to a core.

            The assignment algorithm is:

            1. Each user is assigned a weight by an administrator.
            2. The set of users who have targets that match the core's engine
                is determined.
            3. A user is chosen based on his weight relative to other users.
            4. One of the user's targets is chosen based on the target weights

            **Example request**

            .. sourcecode:: javascript

                {
                    "engine": "openmm_50_opencl",
                    "donor_token": "token", // optional
                    "target_id": "target_id" // optional
                }

            .. note:: If ``target_id`` is specified, then the CC will disregard
                the assignment algorithm.

            **Example reply**

            .. sourcecode:: javascript

                {
                    "token": "6lk2j5-tpoi2p6-poipoi23",
                    "url": "https://raynor.stanford.edu:1234/core/start",
                }

            :status 200: OK
            :status 400: Bad request

        """
        # core authentication
        try:
            key = self.request.headers['Authorization']
        except:
            self.write(json.dumps({'error': 'missing Authorization header'}))
            return self.set_status(401)
        content = json.loads(self.request.body.decode())
        core_engine = content['engine']
        query = {'engine': {'$in': [core_engine]}}
        cursor = self.motor.engines.keys
        keys = []
        results = cursor.find(query, {'_id': 1})
        while (yield results.fetch_next):
            document = results.next_object()
            keys.append(document['_id'])
        if key not in keys:
            return self.error('Bad engine key')

        self.set_status(400)
        content = json.loads(self.request.body.decode())
        if 'donor_token' in content:
            donor_token = content['donor_token']
            donors = self.motor.community.donors
            query = yield donors.find_one({'token': donor_token},
                                          fields=['_id'])
            if not query:
                return self.error('bad donor token')
            donor_id = query['_id']
        else:
            donor_id = None
        core_engine = content['engine']
        cursor = self.motor.data.targets
        if 'target_id' in content:
            target_id = content['target_id']
            result = yield cursor.find_one({'_id': target_id},
                                           {'engines': 1,
                                            'shards': 1,
                                            })
            if core_engine not in result['engines']:
                return self.error('core engine not allowed for this target')
            if not result['shards']:
                return self.error('target specified has no shards')
            shards = result['shards']
        else:
            results = cursor.find({'engines': {'$in': [core_engine]},
                                   'stage': 'public'},
                                  {'owner': 1,
                                   '_id': 1,
                                   'weight': 1,
                                   'shards': 1})
            owner_weights = dict()
            target_weights = dict()
            target_owners = dict()
            target_shards = dict()
            while (yield results.fetch_next):
                document = results.next_object()
                if document['shards']:
                    owner_weights[document['owner']] = None
                    target_weights[document['_id']] = document['weight']
                    target_owners[document['_id']] = document['owner']
                    target_shards[document['_id']] = document['shards']
            if not target_weights:
                return self.error('no valid targets could be found')
            cursor = self.motor.users.managers
            results = cursor.find(fields={'_id': 1, 'weight': 1})
            while (yield results.fetch_next):
                document = results.next_object()
                owner_weights[document['_id']] = document['weight']

            def weighted_sample(d):
                keys = list(d.keys())
                values = list(d.values())
                # exclusive prefix sum
                for index, value in enumerate(values):
                    if index > 0:
                        values[index] += values[index-1]
                x = random.uniform(0, values[-1])
                for index, value in enumerate(values):
                    if value > x:
                        break
                return keys[index]

            picked_owner = weighted_sample(owner_weights)
            owner_targets = dict((k, v) for k, v in target_weights.items()
                                 if target_owners[k] == picked_owner)
            target_id = weighted_sample(owner_targets)
            shards = target_shards[target_id]

        def scv_online(scv_name):
            cursor = SCV(scv_name, self.db)
            if cursor.hget('fail_count') < self.application._max_ws_fails:
                return True
            else:
                return False

        available_scvs = list(filter(lambda x: scv_online(x), shards))
        random.shuffle(available_scvs)
        for scv in available_scvs:
            msg = {'target_id': target_id,
                   'engine': core_engine}
            if donor_id:
                msg['donor_id'] = donor_id
            try:
                password = SCV(scv, self.db).hget('password')
                headers = {'Authorization': password}
                reply = yield self.fetch(scv, '/streams/activate',
                                         method='POST', body=json.dumps(msg),
                                         headers=headers)
                if reply.code == 200:
                    token = json.loads(reply.body.decode())["token"]
                    host = SCV(scv, self.db).hget('host')
                    body = {'token': token,
                            'url': 'https://'+host+'/core/start'
                            }
                    self.write(body)
                    return self.set_status(200)
            except tornado.httpclient.HTTPError:
                print('--CAUGHT HTTP ERROR--')
                pass
        return self.error('no streams available for the target')


class SCVStatusHandler(BaseHandler):
    def get(self):
        """
        .. http:put:: /scv/status

            Return the status of all scvs managed by the command center.

            **Example response**

            .. sourcecode:: javascript

                {
                    "raynor": {
                        "host": "raynor.stanford.edu",
                        "online": true,
                    }
                }

        """
        self.set_status(400)
        body = {}
        for scv_name in SCV.members(self.db):
            cursor = SCV(scv_name, self.db)
            body[scv_name] = {}
            body[scv_name]['host'] = cursor.hget('host')
            if cursor.hget('fail_count') < self.application._max_ws_fails:
                body[scv_name]['online'] = True
            else:
                body[scv_name]['online'] = False
        self.set_status(200)
        return self.write(body)


class StreamsHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /streams

            Add a new stream to an existing target.

            :reqheader Authorization: Manager's authorization token

            **Example request**

            .. sourcecode:: javascript

                {
                    "target_id": "target_id",
                    "files": {"file1_name": "file1_bin_b64",
                              "file2_name": "file2_bin_b64",
                              }
                }

            **Example reply**

            .. sourcecode:: javascript

                {
                    "stream_id": "stream uuid4"
                }

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
         # randomly pick from available scvs
        if SCV.members(self.db):
            picked_scv = random.sample(SCV.members(self.db), 1)[0]
            reply = yield self.fetch(picked_scv, '/streams', method='POST',
                                     body=self.request.body,
                                     headers=self.request.headers)
            self.set_status(reply.code)
            self.write(reply.body)
        else:
            self.error('no scvs available')


class TargetInfoHandler(BaseHandler):
    @tornado.gen.coroutine
    def get(self, target_id):
        """
        .. http:get:: /targets/info/:target_id

            Get detailed information about a target

            **Example reply**

            .. sourcecode:: javascript

                {
                    "owner": "diwakar@gmail.com",
                    "creation_date": 1392784469,
                    "stage": "beta",
                    "shards": ["raynor", "zeratul"],
                    "engines": ["openmm_50_opencl", "openmm_50_cpu"]
                    "options": {
                        "description": "Some secret project",
                        "steps_per_frame": 50000,
                    }
                }

            .. note:: ``creation_date`` is in seconds since epoch 01/01/1970.

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        cursor = self.motor.data.targets
        info = yield cursor.find_one({'_id': target_id})
        self.set_status(200)
        self.write(info)


class TargetDeleteHandler(BaseHandler):
    @tornado.gen.coroutine
    def put(self, target_id):
        """
        .. http:put:: /targets/delete/:target_id

            Delete a target from the Command Center. The target must not have
            any shards in order for this method to succeed.

            This will not affect mongo's community database in order to
            preserve statistics.

            :status 200: OK
            :status 400: Bad request

        """
        if(yield self.get_current_user()) is None:
            return self.error('Bad Manager Credentials', 401)
        self.set_status(400)
        cursor = self.motor.data.targets
        result = yield cursor.remove({'_id': target_id,
                                      'shards': {'$size': 0}})
        if result['n'] > 0:
            return self.set_status(200)
        else:
            return self.error('Could not remove target, make sure it has no\
                               shards and that target_id is correct')


class TargetsHandler(BaseHandler):
    @tornado.gen.coroutine
    def get(self):
        """
        .. http:get:: /targets

            Return a list of all the targets. If a manager is
            authenticated, then only his set of targets will be returned.

            **Example reply**

            .. sourcecode:: javascript

                {
                    'targets': ['target_id1', 'target_id2', '...']
                }

            :status 200: OK
            :status 400: Bad request

        """
        manager = yield self.get_current_user()
        cursor = self.motor.data.targets
        if manager:
            targets = []
            results = cursor.find({'owner': manager}, {'_id': 1})
            while (yield results.fetch_next):
                document = results.next_object()
                targets.append(document['_id'])
            return self.write({'targets': targets})
        else:
            targets = []
            results = cursor.find(field={'_id': 1})
            while (yield results.fetch_next):
                document = results.next_object()
                targets.append(document['_id'])
            return self.write({'targets': targets})

    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /targets

            Add a new target.

            :reqheader Authorization: Manager's authorization token

            **Example request**

            .. sourcecode:: javascript

                {
                    "engines": [
                        "openmm_60_opencl",
                        "openmm_60_cpu",
                        "openmm_60_cuda",
                        "openmm_60_opencl_ps4",
                        "openmm_60_cpu_ps4",
                        "openmm_55_opencl",
                        "openmm_55_cuda"
                    ],
                    "stage": "disabled", "private", or "public"
                    "options": {
                        "title": "Dihydrofolate reductase",
                        "description": "project description",
                        "category": "Benchmark",
                        "steps_per_frame": 50000,
                        "xtc_precision": 3,
                        "discard_water": True
                    }
                    "weight": 1 // weight of the target relative to others
                }

            .. note:: If ``stage`` is not given, then the stage defaults to
                "private".
            .. note:: ``description`` must be a JSON compatible string.
            .. note:: ``options`` are generally core specific. Different cores
                will interpret options differently. Refer to the core guide for
                additional info.

            **Example reply**

            .. sourcecode:: javascript

                {
                    "target_id": "uuid4"
                }

            :status 200: OK
            :status 400: Bad request

        """
        # TODO: shove target descriptions to mongodb
        self.set_status(400)
        current_user = yield self.get_current_user()
        if not current_user:
            self.error('Bad credentials', code=401)
        content = json.loads(self.request.body.decode())
        #----------------#
        # verify request #
        #----------------#
        engines = content['engines']
        if 'stage' in content:
            if content['stage'] in ['disabled', 'private', 'public']:
                stage = content['stage']
            else:
                return self.error('unsupported stage')
        else:
            stage = 'private'
        if 'options' in content:
            options = content['options']
        else:
            options = dict()
        if 'weight' not in content:
            weight = 1
        else:
            weight = max(content['weight'], 0)

        #------------#
        # write data #
        #------------#
        target_id = str(uuid.uuid4())
        owner = yield self.get_current_user()
        payload = {
            '_id': target_id,
            'creation_date': time.time(),
            'engines': engines,
            'owner': owner,
            'stage': stage,
            'options': options,
            'shards': [],
            'weight': weight,
        }
        if 'options' in content:
            payload['options'] = content['options']
        cursor = self.motor.data.targets
        yield cursor.insert(payload)
        self.set_status(200)
        response = {'target_id': target_id}

        return self.write(response)


class CommandCenter(BaseServerMixin, tornado.web.Application):

    _max_ws_fails = 10

    @tornado.gen.coroutine
    def _load_scvs(self):
        """ Load a list of available SCVs from MDB and cache in redis. """
        cursor = self.motor.servers.scvs
        results = cursor.find()
        while(yield results.fetch_next):
            document = results.next_object()
            scv_name = document['_id']
            scv_host = document['host']
            scv_pass = document['password']
            if not SCV.exists(scv_name, self.db):
                fields = {'host': scv_host,
                          'fail_count': 0,
                          'password': scv_pass}
                SCV.create(scv_name, self.db, fields)
            else:
                cursor = SCV(scv_name, self.db)
                pipe = self.db.pipeline()
                cursor.hset('host', scv_host, pipeline=pipe)
                cursor.hset('fail_count', 0, pipeline=pipe)
                cursor.hset('password', scv_pass, pipeline=pipe)
                pipe.execute()

    def __init__(self, name, external_host, redis_options, mongo_options):
        self.base_init(name, redis_options, mongo_options)
        super(CommandCenter, self).__init__([
            (r'/engines/keys', EngineKeysHandler),
            (r'/engines/keys/delete/(.*)', EngineKeysDeleteHandler),
            (r'/core/assign', CoreAssignHandler),
            (r'/managers/verify', ManagerVerifyHandler),
            (r'/managers/auth', ManagerAuthHandler),
            (r'/managers', ManagersHandler),
            (r'/donors/auth', DonorAuthHandler),
            (r'/donors', DonorsHandler),
            (r'/targets', TargetsHandler),
            (r'/targets/delete/(.*)', TargetDeleteHandler),
            (r'/targets/info/(.*)', TargetInfoHandler),
            (r'/targets/update/(.*)', TargetUpdateHandler),
            (r'/scvs/status', SCVStatusHandler),
            (r'/streams', StreamsHandler),
            ])

    @tornado.gen.coroutine
    def fetch(self, scv_id, path, **kwargs):
        """ This is a fairly special method. First, it takes care of boiler
        plate code. Second, it keeps track of how many times a workserver has
        failed. If it has failed one too many times, then the workserver is
        taken offline automatically.

        """
        cursor = SCV(scv_id, self.db)
        host = cursor.hget('host')
        uri = 'https://'+host+path
        client = tornado.httpclient.AsyncHTTPClient()
        try:
            reply = yield client.fetch(uri, validate_cert=is_domain(host),
                                       **kwargs)
            cursor.hset('fail_count', 0)
            return reply
        except (tornado.httpclient.HTTPError, IOError) as e:
            if isinstance(e, tornado.httpclient.HTTPError):
                code = e.code
                if e.response:
                    body = io.BytesIO(e.response.body)
                else:
                    body = io.BytesIO('scv disabled')
                if e.code == 599:
                    cursor.hincrby('fail_count', 1)
                else:
                    cursor.hset('fail_count', 0)
            else:
                code = 503
                body = io.BytesIO(json.dumps({'error': 'scv down'}).encode())
                cursor.hincrby('fail_count', 1)
            dummy = tornado.httpclient.HTTPRequest(uri)
            reply = tornado.httpclient.HTTPResponse(dummy, code, buffer=body)
            return reply

    @tornado.gen.coroutine
    def _check_scvs(self):
        """ Check all SCVs to see if they are alive or not """
        yield self._load_scvs()
        scvs = SCV.members(self.db)
        for scv_name in scvs:
            yield self.fetch(scv_name, '/')


def start():
    extra_options = {'allowed_core_keys': set}
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', 'cc.conf')
    configure_options(config_file, extra_options)
    options = tornado.options.options
    instance = CommandCenter(name=options.name,
                             external_host=options.external_host,
                             redis_options=options.redis_options,
                             mongo_options=options.mongo_options)
    ssl_opts = None
    if options.ssl_certfile or options.ssl_key or options.ssl_ca_certs:
        cert_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 '..', options.ssl_certfile)
        key_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                '..', options.ssl_key)
        ca_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', options.ssl_ca_certs)
        ssl_opts = {
            'certfile': cert_path,
            'keyfile': key_path,
            'ca_certs': ca_path
        }
    cc_server = tornado.httpserver.HTTPServer(instance, ssl_options=ssl_opts)
    cc_server.bind(options.internal_http_port)
    cc_server.start(0)
    instance.initialize_motor()
    if tornado.process.task_id() == 0:
        tornado.ioloop.IOLoop.instance().add_callback(instance._check_scvs)
        pulse = tornado.ioloop.PeriodicCallback(instance._check_scvs, 5000)
        pulse.start()
    tornado.ioloop.IOLoop.instance().start()
