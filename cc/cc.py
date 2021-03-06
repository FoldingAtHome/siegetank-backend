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
import tornado.process

import json
import os
import uuid
import random
import time
import io
import signal
import sys
import logging

from cc.common import BaseServerMixin, is_domain, configure_options
from cc.common import CommonHandler, kill_children

class BaseHandler(CommonHandler):

    def initialize(self):
        self.fetch = self.application.fetch
        self.scvs = self.application.scvs

    @tornado.gen.coroutine
    def get_target_owner(self, target_id):
        cursor = self.motor.data.targets
        info = yield cursor.find_one({'_id': target_id}, {'owner': 1})
        if info:
            return info['owner']
        else:
            return None


class UserVerifyHandler(BaseHandler):

    @tornado.gen.coroutine
    def get(self):
        """
        .. http:post:: /users/verify

            Validate an user's Authorization token.

            :reqheader Authorization: token

            :status 200: OK
            :status 400: Unauthorized

        """
        self.set_status(400)
        current_user = yield self.get_current_user()
        if not current_user:
            self.error('Bad credentials', code=401)
        else:
            return self.set_status(200)


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
                    "stage": "private",  //optional
                    "engines": ["openmm", "openmm_cluster"],  //optional
                    "weight": 2, // optional
                    "options": {
                        "description": "omgwtfbbq", // optional
                        "title": "DHFR",
                        "category": "wow",
                        "steps_per_frame": 50000
                    } // optional
                }

            .. note:: modifying ``stage`` only affects future assignments.
                If you wish to stop the streams, you must explicitly stop
                them. Allowed ``stages`` are disabled, private, and public

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
        target_owner = yield self.get_target_owner(target_id)
        if target_owner is None:
            self.error('Bad target_id', code=400)
        if target_owner != current_user:
            self.error('User does not own this target', code=401)
        content = json.loads(self.request.body.decode())
        payload = {}
        if 'engines' in content:
            payload['engines'] = content['engines']
        if 'stage' in content:
            if content['stage'] in ['disabled', 'private', 'beta', 'public']:
                payload['stage'] = content['stage']
            else:
                self.error('invalid stage')
        if 'weight' in content:
            payload['weight'] = max(content['weight'], 0)
        if 'options' in content:
            for key, value in content['options'].items():
                payload['options.'+key] = value
        if not payload:
            self.error('Nothing to update')
        cursor = self.motor.data.targets
        result = yield cursor.update({'_id': target_id}, {'$set': payload})
        if result['updatedExisting']:
            self.set_status(200)
        else:
            self.error('invalid '+target_id)


class CoreAssignHandler(BaseHandler):

    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /core/assign

            Assign a stream from an SCV to a core.

            The assignment algorithm proceeds by identifying all compatible
            targets given the core's engine. The managers responsible for these
            targets is identified. A manager is chosen based on his weight
            relative to the weights of other managers. One of his targets is
            chosen based on the target's relative weight.

            :reqheader Authorization: Engine key

            **Example request**

            .. sourcecode:: javascript

                {
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
            :status 400: Error
            :status 401: Unauthorized Core

        """
        # core authentication
        try:
            key = self.request.headers['Authorization']
        except:
            self.error('missing Authorization header', code=401)
            return self.set_status(401)

        try:
            content = json.loads(self.request.body.decode())
        except:
            self.error('Bad POST content', code=401)
        cursor = self.motor.engines.keys
        result = yield cursor.find_one({'_id': key}, fields=['engine'])
        if not result:
            self.error('Bad engine key', code=401)
        core_engine = result['engine']
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        if 'donor_token' in content:
            donor_token = content['donor_token']
            cursor = self.motor.users.all
            query = yield cursor.find_one({'token': donor_token},
                                          fields=['_id'])
            if not query:
                self.error('Bad donor token')
            user = query['_id']
        else:
            user = None
        cursor = self.motor.data.targets
        if 'target_id' in content:
            # a target was specified in the request
            target_id = content['target_id']
            result = yield cursor.find_one({'_id': target_id},
                                           {'engines': 1})
            if core_engine not in result['engines']:
                self.error('Core engine not allowed for this target')
            
            if target_id not in self.application.shards:
                self.error('Target specified has no shards')
            shards = self.application.shards[target_id]
        else:
            # no target was specified
            results = cursor.find({'engines': {'$in': [core_engine]},
                                   'stage': 'public'},
                                  {'owner': 1,
                                   '_id': 1,
                                   'weight': 1})
            owner_weights = dict()
            target_weights = dict()
            target_owners = dict()
            # target_shards = dict()
            while (yield results.fetch_next):
                document = results.next_object()
                if document['_id'] in self.application.shards:
                    owner_weights[document['owner']] = None
                    target_weights[document['_id']] = document['weight']
                    target_owners[document['_id']] = document['owner']
            if not target_weights:
                self.error('no valid targets could be found')
            # get a list of all managers and their ids
            cursor = self.motor.users.managers
            results = cursor.find(fields={'_id': 1, 'weight': 1})
            while (yield results.fetch_next):
                document = results.next_object()
                # add this owner iff he's a matching owner
                if document['_id'] in owner_weights:
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
            shards = self.application.shards[target_id]

        def scv_online(scv_name):
            if self.scvs[scv_name]['fail_count'] < self.application._max_ws_fails:
                return True
            else:
                return False

        available_scvs = list(filter(lambda x: scv_online(x), shards))
        random.shuffle(available_scvs)
        for scv in available_scvs:
            msg = {'target_id': target_id,
                   'engine': core_engine}
            if user:
                msg['user'] = user
            try:
                password = self.scvs[scv]['password'] 
                headers = {'Authorization': password}
                reply = yield self.fetch(scv, '/streams/activate',
                                         method='POST', body=json.dumps(msg),
                                         headers=headers)
                if reply.code == 200:
                    token = json.loads(reply.body.decode())["token"]
                    host = self.scvs[scv]['host']
                    body = {'token': token,
                            'url': 'https://'+host+'/core/start'}
                    self.write(body)
                    return self.set_status(200)
                elif reply.code == 400:
                    message = "Assignment returned 400, target_id: "+target_id+" scv: "+scv
                    logging.getLogger('tornado.application').critical(message)
            except tornado.httpclient.HTTPError as e:
                message = "Assignment failed, target_id: "+target_id+" scv: "+scv
                logging.getLogger('tornado.application').critical(message)
        self.error('no streams available for the target')


class SCVStatusHandler(BaseHandler):

    def get(self):
        """
        .. http:put:: /scvs/status

            Return the status of all SCVs as seen by this CC.

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

        print('scv status request received', self.scvs, self.application.scvs)

        for scv_name, scv_prop in self.scvs.items():
            body[scv_name] = {}
            body[scv_name]['host'] = scv_prop['host']
            if scv_prop['fail_count'] < self.application._max_ws_fails:
                body[scv_name]['online'] = True
            else:
                body[scv_name]['online'] = False
        self.set_status(200)
        return self.write(body)


class TargetInfoHandler(BaseHandler):

    @tornado.gen.coroutine
    def get(self, target_id):
        """
        .. http:get:: /targets/info/:target_id

            Get detailed information about a target.

            **Example reply**

            .. sourcecode:: javascript

                {
                    "owner": "dshukla",
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
        if not info:
            self.error('Invalid target id')
        self.set_status(200)
        # it's possible this target has no shards!
        if target_id in self.application.shards:
            info["shards"] = list(self.application.shards[target_id])
        self.write(info)


class TargetDeleteHandler(BaseHandler):

    @tornado.gen.coroutine
    def put(self, target_id):
        """
        .. http:put:: /targets/delete/:target_id

            Delete a target from the Command Center.

            This will not affect mongo's community database in order to
            preserve statistics.

            :status 200: OK
            :status 400: Bad request

        """
        if(yield self.get_current_user()) is None:
            self.error('Bad Manager Credentials', 401)
        self.set_status(400)

        streams_cursor = self.motor["streams"]
        scv_names = yield streams_cursor.collection_names()
        scv_names.remove('system.indexes')
        shard_copy = {}

        success = True

        headers = {'Authorization': self.request.headers['Authorization']}
        
        for scv_id in scv_names:
            cursor = streams_cursor[scv_id]
            results = cursor.find({'target_id': target_id}, {'_id': 1})
            while (yield results.fetch_next):
                document = results.next_object()
                stream_id = document['_id']

                print("deleting stream", stream_id)

                reply = yield self.fetch(scv_id, '/streams/delete/'+stream_id, method='PUT', headers=headers, body='')
                if reply.code != 200:
                    success = False
                    print('\n\nCANT DELETE TARGET :(\n\n', str(reply.body))
                    self.error("Unable to delete, error:"+str(reply.body))
                else:
                    print('deleted', stream_id)

        if success:
            cursor = self.motor.data.targets
            yield cursor.remove({'_id': target_id})
            return self.set_status(200)

# TODO: Cache?
class TargetStreamsHandler(BaseHandler):

    @tornado.gen.coroutine
    def get(self, target_id):
        """
        .. http:get:: /targets/streams/:target_id

            List all the streams for target target_id.

            :reqheader Authorization: Manager's authorization token (optional)

            **Example reply**

            .. sourcecode:: javascript

                {
                    'streams': ['target_id1', 'target_id2', '...']
                }

            :status 200: OK
            :status 400: Bad request

        """
        # if(yield self.get_current_user()) is None:
        #     self.error('Bad Manager Credentials', 401)
        self.set_status(400)
        streams_cursor = self.motor["streams"]
        scv_names = yield streams_cursor.collection_names()
        streams_result = {
            "streams": []
        }
        if scv_names:
            scv_names.remove('system.indexes')
            for scv_id in scv_names:
                cursor = streams_cursor[scv_id]
                results = cursor.find({'target_id': target_id},
                                      {'_id': 1}) # stream_id

                while (yield results.fetch_next):
                    document = results.next_object()
                    streams_result['streams'].append(document['_id'])
        self.write(streams_result)
        return self.set_status(200)


class TargetsHandler(BaseHandler):

    @tornado.gen.coroutine
    def get(self):
        """
        .. http:get:: /targets

            Return a list of targets. If a manager's Authorization token is
            provided, then only his set of targets will be returned. Otherwise,
            a list of public targets will be returned.

            :reqheader Authorization: Manager's authorization token (optional)

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
            results = cursor.find({'_id': 1, 'stage': 'public'}, {'_id': 1})
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
        is_manager = yield self.is_manager(current_user)
        if not is_manager:
            self.error('Not a manager', code=401)
        content = json.loads(self.request.body.decode())
        #----------------#
        # verify request #
        #----------------#
        engines = content['engines']

        if 'stage' in content:
            if content['stage'] in ['disabled', 'private', 'public']:
                stage = content['stage']
            else:
                self.error('unsupported stage')
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
            'weight': weight,
        }
        if 'options' in content:
            payload['options'] = content['options']
        cursor = self.motor.data.targets
        yield cursor.insert(payload)
        self.set_status(200)
        response = {'target_id': target_id}
        return self.write(response)


class EngineKeysHandler(BaseHandler):

    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /engines/keys

            Add a new core key for the specified engine. The corresponding core
            must identify itself using the token. A single engine can have
            multiple core keys.

            :reqheader Authorization: access token of an administrator.

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
        current_user = yield self.get_current_user()
        if not (yield self.is_admin(current_user)):
            self.error('Bad credentials', 401)
        content = json.loads(self.request.body.decode())
        for required_key in ['engine', 'description']:
            if required_key not in content:
                self.error('missing: '+required_key)
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
        current_user = yield self.get_current_user()
        if not (yield self.is_admin(current_user)):
            self.error('Bad credentials', 401)
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
        current_user = yield self.get_current_user()
        if not (yield self.is_admin(current_user)):
            self.error('Bad credentials', 401)
        self.set_status(400)
        cursor = self.motor.engines.keys
        result = yield cursor.remove({'_id': core_key})
        if result['n'] > 0:
            self.set_status(200)
        else:
            self.error('engine key not found')


class AliveHandler(BaseHandler):

    def get(self):
        """
        .. http:get:: /

            Used to check and see if the server is up.

            :status 200: OK

        """
        self.set_status(200)


class CommandCenter(BaseServerMixin, tornado.web.Application):

    _max_ws_fails = 5

    @tornado.gen.coroutine
    def _load_scvs(self):
        """ Load a list of available SCVs from Mongo """
        cursor = self.motor.servers.scvs
        results = cursor.find()
        while (yield results.fetch_next):
            document = results.next_object()
            scv_name = document['_id']
            scv_host = document['host']
            scv_pass = document['password']

            if scv_name not in self.scvs:
                self.scvs[scv_name] = {
                    'host': scv_host,
                    'password': scv_pass,
                    'fail_count': 0,
                }
            else:
                self.scvs[scv_name]['host'] = scv_host
                self.scvs[scv_name]['password'] = scv_pass

    def __init__(self, name, redis_options, mongo_options):
        self.base_init(name, redis_options, mongo_options)
        self.scvs = {}
        self.shards = {}
        super(CommandCenter, self).__init__([
            (r'/', AliveHandler),
            (r'/engines/keys', EngineKeysHandler),
            (r'/engines/keys/delete/(.*)', EngineKeysDeleteHandler),
            (r'/core/assign', CoreAssignHandler),
            (r'/users/verify', UserVerifyHandler),
            (r'/targets', TargetsHandler),
            (r'/targets/delete/(.*)', TargetDeleteHandler),
            (r'/targets/info/(.*)', TargetInfoHandler),
            (r'/targets/update/(.*)', TargetUpdateHandler),
            (r'/scvs/status', SCVStatusHandler),
            (r'/targets/streams/(.*)', TargetStreamsHandler)
            ])

    @tornado.gen.coroutine
    def fetch(self, scv_id, path, **kwargs):
        """ Make a request to a particular SCV and keep track of whether or not
        it is alive. 

        """
        host = self.scvs[scv_id]['host']
        uri = 'https://'+host+path
        client = tornado.httpclient.AsyncHTTPClient()
        try:
            reply = yield client.fetch(uri, validate_cert=is_domain(host),
                                       **kwargs)
            self.scvs[scv_id]['fail_count'] = 0
            return reply
        except (tornado.httpclient.HTTPError, IOError) as e:
            if isinstance(e, tornado.httpclient.HTTPError):
                if e.response:
                    body = io.BytesIO(e.response.body)
                else:
                    body = io.BytesIO(b'scv disabled')
                if e.code == 599:
                    self.scvs[scv_id]['fail_count'] += 1
                else:
                    self.scvs[scv_id]['fail_count'] = 0
            else:
                body = io.BytesIO(json.dumps({'error': 'scv down'}).encode())
                self.scvs[scv_id]['fail_count'] += 1
            dummy = tornado.httpclient.HTTPRequest(uri)
            reply = tornado.httpclient.HTTPResponse(dummy, 400, buffer=body)
            return reply

    @tornado.gen.coroutine
    def _cache_shards(self):
        streams_cursor = self.motor["streams"]
        scv_names = yield streams_cursor.collection_names()
        shard_copy = {}
        if scv_names:
            scv_names.remove('system.indexes')
            for scv_id in scv_names:
                cursor = streams_cursor[scv_id]
                results = cursor.find({'status': 'enabled'},
                                      {'target_id': 1}) # stream_id

                while (yield results.fetch_next):
                    document = results.next_object()
                    tid = document['target_id']
                    if tid not in shard_copy:
                        shard_copy[tid] = set()
                    shard_copy[tid].add(scv_id)

        self.shards = shard_copy

    @tornado.gen.coroutine
    def _check_scvs(self):
        """ Check all SCVs to see if they are alive or not """
        yield self._load_scvs()
        for key, scv_name in self.scvs.items():
            yield self.fetch(scv_name, '/')

def stop_parent(sig, frame):
    kill_children()


def stop_children(sig, frame):
    print('-> stopping children', tornado.process.task_id())
    # stop accepting new requests
    tornado.ioloop.IOLoop.instance().add_callback_from_signal(server.stop)
    tornado.ioloop.IOLoop.instance().add_callback_from_signal(app.shutdown)


def start():

    global server
    global app

    print('starting CC on pid', os.getpid())

    extra_options = {'allowed_core_keys': set}
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', 'cc.conf')
    configure_options(config_file, extra_options)
    options = tornado.options.options
    app = CommandCenter(name=options.name,
                        redis_options=options.redis_options,
                        mongo_options=options.mongo_options)
    ssl_opts = None
    if options.ssl_certfile or options.ssl_key or options.ssl_ca_certs:
        print("Enabling SSL ...")
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

    sockets = tornado.netutil.bind_sockets(options.internal_http_port)

    signal.signal(signal.SIGINT, stop_parent)
    signal.signal(signal.SIGTERM, stop_parent)

    try:
        # num_processes defaults to 0 if not specified
        tornado.process.fork_processes(options.num_processes)

        signal.signal(signal.SIGTERM, stop_children)

        server = tornado.httpserver.HTTPServer(app, ssl_options=ssl_opts)
        server.add_sockets(sockets)

        app.initialize_motor()

        tornado.ioloop.IOLoop.instance().add_callback(app._check_scvs)
        tornado.ioloop.IOLoop.instance().add_callback(app._cache_shards)
        pulse = tornado.ioloop.PeriodicCallback(app._check_scvs, 2000)
        pulse.start()
        # update target shards every minute
        pulse2 = tornado.ioloop.PeriodicCallback(app._cache_shards, 60000)
        pulse2.start()
        tornado.ioloop.IOLoop.instance().start()
    except SystemExit as e:
        print('! parent is shutting down ...')
        # app.db.shutdown()
        sys.exit(0)
