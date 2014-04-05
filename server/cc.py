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

import shutil
import json
import os
import uuid
import random
import time
import bcrypt
import pymongo
import io
import socket

from server.common import BaseServerMixin, is_domain, configure_options
from server.common import authenticate_manager
from server.apollo import Entity


class SCV(Entity):
    prefix = 'scv'
    fields = {'host': str,  # http request url (verify based on if IP or not)
              'fail_count': int,  # number of times a request has failed
              }


class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")

    def initialize(self):
        self.fetch = self.application.fetch

    def error(self, message):
        """ Write a message to the output buffer """
        self.set_status(400)
        self.write({'error': message})

    @property
    def db(self):
        return self.application.db

    @property
    def mdb(self):
        return self.application.mdb

    def get_current_user(self):
        try:
            header_token = self.request.headers['Authorization']
        except KeyError:
            return None
        managers = self.mdb.users.managers
        query = managers.find_one({'token': header_token},
                                  fields=['_id'])
        if query:
            return query['_id']
        else:
            return None

    # TODO: refactor to common
    def get_user_role(self, email):
        mdb = self.mdb.users
        query = mdb.managers.find_one({'_id': email}, fields={'role'})
        try:
            return query['role']
        except:
            return None

    def load_target_options(self, target_id):
        options_path = os.path.join(self.application.targets_folder,
                                    target_id, 'options')

        if os.path.exists(options_path):
            with open(options_path, 'r') as handle:
                options = json.loads(handle.read())
                return options
        else:
            return dict()


class AuthDonorHandler(BaseHandler):
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
        donors = self.mdb.community.donors
        query = donors.find_one({'_id': username},
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


class AddDonorHandler(BaseHandler):
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
        donors = self.mdb.community.donors
        # see if email exists:
        query = donors.find_one({'email': email})
        if query:
            return self.error('email exists in db!')
        hash_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
        token = str(uuid.uuid4())
        db_body = {'_id': username,
                   'password_hash': hash_password,
                   'token': token,
                   'email': email}

        try:
            donors.insert(db_body)
        except:
            return self.error(username+' exists')

        self.set_status(200)
        self.write({'token': token})


class VerifyManagerHandler(BaseHandler):
    @authenticate_manager
    def get(self):
        """
        .. http:post:: /managers/validate

            Validate an authentication token.

            :reqheader Authorization: access token of a manager

            :status 200: OK
            :status 400: Unauthorized

        """
        self.set_status(200)
        return


class AuthManagerHandler(BaseHandler):
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
        managers = self.mdb.users.managers
        query = managers.find_one({'_id': email},
                                  fields=['password_hash'])
        stored_hash = query['password_hash']
        if stored_hash == bcrypt.hashpw(password.encode(), stored_hash):
            new_token = str(uuid.uuid4())
            managers.update({'_id': email}, {'$set': {'token': new_token}})
        else:
            return self.status(401)
        self.set_status(200)
        self.write({'token': new_token})


class AddManagerHandler(BaseHandler):
    def post(self):
        """
        .. http:post:: /managers

            Add a manager. This request can only be made by managers whose
            role is *admin*.

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
        current_user = self.get_current_user()
        if current_user:
            if self.get_user_role(current_user) != 'admin':
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
                   'role': role
                   }

        managers = self.mdb.users.managers
        try:
            managers.insert(db_body)
        except pymongo.errors.DuplicateKeyError:
            return self.error(email+' exists')

        self.set_status(200)
        self.write({'token': token})


class TargetUpdateHandler(BaseHandler):
    @authenticate_manager
    def put(self, target_id):
        """
        .. http:put:: /targets/update/:target_id

            Update certain fields of ``target_id``

            :reqheader Authorization: Manager’s authorization token

            **Example request**

            .. sourcecode:: javascript

                {
                    "stage": "disabled", private", "beta", "public"  //optional
                    "engine_versions":  ["6.0", "6.5"]  //optional
                    "description": "description"  //optional
                    "weight": 2 // optional
                }

                .. note:: ``engine_versions`` will only affect future streams.

                .. note:: modifying ``stage`` only affects future assignments.
                    If you wish to stop the streams, you must explicitly stop
                    them.

            **Example reply**

            :status 200: OK
            :status 400: Bad request
            :status 401: Unauthorized

        """
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        payload = {}
        if 'engine_versions' in content:
            payload['engine_versions'] = content['engine_versions']
        if 'stage' in content:
            if content['stage'] in ['disabled', 'private', 'beta', 'public']:
                payload['stage'] = content['stage']
            else:
                return self.error('invalid stage')
        if 'description' in content:
            payload['description'] = content['description']
        if 'weight' in content:
            payload['weight'] = max(content['weight'], 0)
        cursor = self.mdb.data.targets
        result = cursor.update({'_id': target_id}, {'$set': payload})
        if result['updatedExisting']:
            self.set_status(200)
        else:
            self.error('invalid '+target_id)


def yates_generator(x):
    for i in range(len(x)-1, -1, -1):
        j = random.randrange(i + 1)
        x[i], x[j] = x[j], x[i]
        yield x[i]


def draw_prefix_sum(array):
    """ Draws a """

class CoreAssignHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /core/assign

            Assign a stream from an SCV to a core. The assignment algorithm is:

            1. Each user is assigned a weight by an administrator.
            2. The set of users who have targets that
                a. match the core's "engine"
                c. allow the core's "engine_version"
            3. A user is chosen based on his weight relative to other users
            4. One of the user's targets is chosen based on the target's
                weights

            **Example request**

            .. sourcecode:: javascript

                {
                    "engine": "openmm",
                    "engine_version": "6.0",
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
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        if 'donor_token' in content:
            donor_token = content['donor_token']
            donors = self.mdb.community.donors
            query = donors.find_one({'token': donor_token},
                                    fields=['_id'])
            if not query:
                return self.error('bad donor token')
            donor_id = query['_id']
        else:
            donor_id = None
        engine = content['engine']
        engine_version = content['engine_version']
        cursor = self.mdb.data.targets
        if 'target_id' in content:
            target_id = content['target_id']
            result = cursor.find_one({'_id': target_id},
                                     {'engine': engine,
                                      'engine_versions': engine_version,
                                      'shards': 1,
                                      })
            if result['engine'] != engine:
                return self.error('target engine does not match core engine')
            if engine_version not in result['engine_versions']:
                return self.error('core engine_version not allowed')
            if not result['shards']:
                return self.error('target specified has no shards')
            shards = result['shards']
        else:
            result = cursor.find({'engine': engine,
                                  'engine_versions': {'$in': [engine_version]},
                                  'stage': 'public',
                                  },
                                 {'owner': 1,
                                  '_id': 1,
                                  'weight': 1,
                                  'shards': 1})
            owner_weights = dict()
            target_weights = dict()
            target_owners = dict()
            target_shards = dict()
            for match in result:
                if match['shards']:
                    owner_weights[match['owner']] = None
                    target_weights[match['_id']] = match['weight']
                    target_owners[match['_id']] = match['owner']
                    target_shards[match['_id']] = match['shards']
            if not target_weights:
                return self.error('no valid targets could be found')
            cursor = self.mdb.users.managers
            result = cursor.find({'_id': {'$in': ['test_ws@gmail.com']}},
                                 {'_id': 1, 'weight': 1})
            for match in result:
                owner_weights[match['_id']] = match['weight']

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
            msg = {'target_id': target_id}
            if donor_id:
                msg['donor_id'] = donor_id
            try:
                reply = yield self.fetch(scv, '/streams/activate',
                                         method='POST', body=json.dumps(msg))
                token = json.loads(reply.body.decode())["token"]
                host = SCV(scv, self.db).hget('host')
                body = {'token': token,
                        'url': 'https://'+host+'/core/start'
                        }
                self.write(body)
                return self.set_status(200)
            except tornado.httpclient.HTTPError:
                print('--caught--httperror--')
                pass
        return self.error('no scvs available for the target')


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


class SCVConnectHandler(BaseHandler):
    def put(self):
        """
        .. http:put:: /scv/connect

            Register an SCV as online. SCVs broadcast their state to all CCs
            available in the MDB.

            :reqheader Authorization: Secret password of the CC

            **Example request**

            .. sourcecode:: javascript

                {
                    "name": "some_workserver",
                }

            .. note:: ``url`` corresponds to the workserver's url. This should
                be a fully qualifed domain and *not* an ip address.

                ``http_port`` is the outward facing port.

            **Example response**

            .. sourcecode:: javascript

                {
                    // empty
                }

            :status 200: OK
            :status 400: Bad request
            :status 401: Unauthorized

        """
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        name = content['name']
        scvs = self.application._load_scvs()
        host = scvs[name]
        if(self.request.remote_ip != socket.gethostbyname(host.split(':')[0])):
            self.error('remote_ip does not match given host')
        self.application._cache_scv(name, host)
        self.set_status(200)
        return self.write(dict())


class SCVDisconnectHandler(BaseHandler):
    def put(self):
        """
        .. http:put:: /scv/disconnect

            Disconnect an SCV, setting its status to offline.

            **Example request**

            .. sourcecode:: javascript

                {
                    "name": "some_workserver"
                }

            **Example response**

            .. sourcecode:: javascript

                {
                    //empty
                }

            :status 200: OK
            :status 400: Bad request
            :status 401: Unauthorized

        """
        self.set_status(400)
        scvs = self.application._load_scvs()
        content = json.loads(self.request.body.decode())
        name = content['name']
        host = scvs[name]
        if(self.request.remote_ip != socket.gethostbyname(host.split(':')[0])):
            self.error('remote_ip does not match given host')
        cursor = SCV(name, self.db)
        cursor.hset('fail_count', self.application._max_ws_fails)
        self.set_status(200)
        return self.write(dict())


class RoutedStreamHandler(BaseHandler):
    @authenticate_manager
    @tornado.gen.coroutine
    def put(self, stream_id):
        """
        .. http:put:: /streams/[start,stop,delete]/:stream_id

            Deletes a stream from the server. This method routes to the
            appropriate ws automatically.

            :reqheader Authorization: Manager's authorization token

            **Example request**

            .. sourcecode:: javascript

                {
                    // empty
                }

            :status 200: OK
            :status 400: Bad request
            :status 401: Unauthorized

        """
        self.set_status(400)
        ws_name = stream_id.split(':')[1]
        rep = yield self.fetch(ws_name, self.request.path, method='PUT',
                               body=self.request.body,
                               headers=self.request.headers)
        self.set_status(rep.code)
        self.write(rep.body)


class PostStreamHandler(BaseHandler):
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
        picked_scv = random.sample(SCV.members(self.db), 1)[0]
        reply = yield self.fetch(picked_scv, '/streams', method='POST',
                                 body=self.request.body,
                                 headers=self.request.headers)
        self.set_status(reply.code)
        self.write(reply.body)


class TargetInfoHandler(BaseHandler):
    def get(self, target_id):
        """
        .. http:get:: /targets/info/:target_id

            Get detailed information about a target

            **Example reply**

            .. sourcecode:: javascript

                {
                    "description": "Some secret project",
                    "owner": "diwakar@gmail.com",
                    "creation_date": 1392784469,
                    "stage": "beta",
                    "allowed_ws": ["raynor", "zeratul", "kerrigan"],
                    "striated_ws": ["raynor", "zeratul"],
                    "engine": "openmm"
                    "engine_versions": ["6.0"]
                    "files": ["filename1", "filename2"],
                    "options": {
                        "steps_per_frame": 50000,
                    }
                }

            .. note:: ``creation_date`` is in seconds since epoch 01/01/1970.

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        cursor = self.mdb.data.targets
        info = cursor.find_one({'_id': target_id})
        self.set_status(200)
        self.write(info)


class TargetStreamsHandler(BaseHandler):
    @tornado.gen.coroutine
    def get(self, target_id):
        """
        .. http:get:: /targets/streams/:target_id

            Return a list of streams on each striated workserver for
            ``target_id``.

            **Example reply**

            .. sourcecode:: javascript

                {
                    "ws_firebat": {
                        "stream1_id": {
                            "status": "OK",
                            "frames": 253
                        }
                        ...
                    }
                    ...
                }

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        target = Target(target_id, self.db)
        striated_ws = target.smembers('striated_ws')

        body = {}

        for ws_name in striated_ws:
            reply = yield self.fetch(ws_name, '/targets/streams/'+target_id)
            if reply.code == 200:
                body.update(json.loads(reply.body.decode()))

        self.set_status(200)
        self.write(body)


class TargetDeleteHandler(BaseHandler):
    @authenticate_manager
    @tornado.gen.coroutine
    def put(self, target_id):
        """
        .. http:put:: /targets/delete/:target_id

            Delete a target from the Command Center. There is no undo button
            once you call this. It will erase everything pertaining to the
            target from Command Center and the Workservers.

            This will not affect mongo's community database in order to
            preserve statistics.

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        target = Target(target_id, self.db)
        striated_ws = target.smembers('striated_ws')
        target_dir = os.path.join(self.application.targets_folder, target_id)
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)
        for ws_name in striated_ws:
            reply = yield self.fetch(ws_name, '/targets/delete/'+target_id,
                                     method='PUT', body='')
            if reply.code == 200:
                target.srem('striated_ws', ws_name)
            else:
                self.write(reply.body.decode())
        target.delete()
        targets = self.mdb.data.targets
        targets.remove(spec_or_id=target_id)
        self.set_status(200)


class TargetsHandler(BaseHandler):
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
        cursor = self.mdb.data.targets
        manager = self.get_current_user()
        if manager:
            result = cursor.find({'owner': manager}, {'_id': 1})
            targets = []
            for k in result:
                targets.append(k['_id'])
            return self.write({'targets': targets})
        else:
            result = cursor.find(field={'_id': 1})
            targets = []
            for k in result:
                targets.append(k['_id'])
            return self.write({'targets': targets})

    @authenticate_manager
    def post(self):
        """
        .. http:post:: /targets

            Add a new target.

            :reqheader Authorization: Manager's authorization token

            **Example request**

            .. sourcecode:: javascript

                {
                    "description": "some JSON compatible description",
                    "engine": "openmm",
                    "engine_versions": ["6.0", "5.5", "5.2"],
                    "stage": "disabled", private", "beta", or "public"
                    "options": {
                        "steps_per_frame": 50000,
                        "xtc_precision": 3,
                        "discard_water": True
                    }
                    "weight": 1 // weight of the target relative to others
                }

            .. note:: If ``stage`` is not given, then the stage defaults to
                "private".
            .. note:: ``description`` must be a JSON compatible string. That
                means it must not contain double quotation marks and slashes.
            .. note:: ``options`` pertains to the target as a whole.
                Stream specific options are not available yet.

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
        content = json.loads(self.request.body.decode())

        #----------------#
        # verify request #
        #----------------#
        engine = content['engine']
        engine_versions = content['engine_versions']
        for k in engine_versions:
            if type(k) is not str:
                return self.error('engine version must be a list of strings')
        description = content['description']
        if 'stage' in content:
            if content['stage'] in ['disabled', 'private', 'beta', 'public']:
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
        targets = self.mdb.data.targets
        payload = {
            '_id': target_id,
            'description': description,
            'creation_date': time.time(),
            'engine': engine,
            'engine_versions': engine_versions,
            'owner': self.get_current_user(),
            'stage': stage,
            'options': options,
            'shards': [],
            'weight': weight,
        }

        if 'options' in content:
            payload['options'] = content['options']

        targets.insert(payload)
        self.set_status(200)
        response = {'target_id': target_id}

        return self.write(response)


class CommandCenter(BaseServerMixin, tornado.web.Application):

    _max_ws_fails = 10

    def _register(self, external_host):
        """ Register the CC in MDB. """
        ccs = self.mdb.servers.ccs
        result = ccs.update({'_id': self.name},
                            {'_id': self.name, 'host': external_host},
                            upsert=True)

    def _load_scvs(self):
        """ Load a list of available SCVs from MDB. """
        cursor = self.mdb.servers.scvs
        scvs_info = dict()
        for scv in cursor.find(fields={'_id': 1, 'host': 1}):
            scvs_info[scv['_id']] = scv['host']
        return scvs_info

    def _cache_scv(self, scv_name, host):
        """ Cache a single SCV into redis. """
        if not SCV.exists(scv_name, self.db):
            fields = {'host': host, 'fail_count': 0}
            SCV.create(scv_name, self.db, fields)
        else:
            cursor = SCV(scv_name, self.db)
            pipe = self.db.pipeline()
            cursor.hset('host', host, pipeline=pipe)
            cursor.hset('fail_count', 0, pipeline=pipe)
            pipe.execute()

    def __init__(self, name, external_host, redis_options, mongo_options):
        self.base_init(name, redis_options, mongo_options)
        self._register(external_host)
        super(CommandCenter, self).__init__([
            (r'/core/assign', CoreAssignHandler),
            (r'/managers/verify', VerifyManagerHandler),
            (r'/managers/auth', AuthManagerHandler),
            (r'/managers', AddManagerHandler),
            (r'/donors/auth', AuthDonorHandler),
            (r'/donors', AddDonorHandler),
            (r'/targets', TargetsHandler),
            (r'/targets/delete/(.*)', TargetDeleteHandler),
            (r'/targets/info/(.*)', TargetInfoHandler),
            (r'/targets/streams/(.*)', TargetStreamsHandler),
            (r'/targets/update/(.*)', TargetUpdateHandler),
            (r'/scv/connect', SCVConnectHandler),
            (r'/scv/disconnect', SCVDisconnectHandler),
            (r'/scv/status', SCVStatusHandler),
            (r'/streams', PostStreamHandler),
            (r'/streams/delete/(.*)', RoutedStreamHandler),
            (r'/streams/start/(.*)', RoutedStreamHandler),
            (r'/streams/stop/(.*)', RoutedStreamHandler)
            ])

    @tornado.gen.coroutine
    def fetch(self, ws_id, path, **kwargs):
        """ This is a fairly special method. First, it takes care of boiler
        plate code. Second, it keeps track of how many times a workserver has
        failed. If it has failed one too many times, then the workserver is
        taken offline automatically.

        """
        cursor = SCV(ws_id, self.db)
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
                body = io.BytesIO(e.response.body)
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
        self._load_scvs()
        for scv_name in self.scvs.keys():
            reply = yield self.fetch(scv_name, '/')
            cursor = SCV(scv_name)
            if reply.code == 200:
                cursor.hset('fail_count', 0)
            else:
                cursor.hset('fail_count', self.application._max_ws_fails)

    def ws_online(self, ws_id):
        """ Returns True if the workserver is online, False otherwise """
        ws = WorkServer(ws_id, self.db)
        if ws.hget('fail_count') < self._max_ws_fails:
            return True
        else:
            return False


def start():
    extra_options = {
        'allowed_core_keys': set
    }
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', 'cc.conf')
    configure_options(config_file, extra_options)
    options = tornado.options.options
    instance = CommandCenter(name=options.name,
                             external_host=options.external_host,
                             redis_options=options.redis_options,
                             mongo_options=options.mongo_options)
    cert_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', options.ssl_certfile)
    key_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            '..', options.ssl_key)
    ca_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           '..', options.ssl_ca_certs)
    cc_server = tornado.httpserver.HTTPServer(instance, ssl_options={
        'certfile': cert_path, 'keyfile': key_path, 'ca_certs': ca_path})
    cc_server.bind(options.internal_http_port)
    cc_server.start(0)
    if tornado.process.task_id() == 0:
        tornado.ioloop.IOLoop.instance().add_callback(instance._check_scvs)
        pulse = tornado.ioloop.PeriodicCallback(instance._check_scvs, 5000)
        pulse.start()
    tornado.ioloop.IOLoop.instance().start()
