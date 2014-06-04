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

from server.common import BaseServerMixin, is_domain, configure_options
from server.common import CommonHandler
from server.apollo import Entity


class SCV(Entity):
    prefix = 'scv'
    fields = {'host': str,  # http request url (verify based on if IP or not)
              'fail_count': int,  # number of times a request has failed
              'password': str,
              }


class BaseHandler(CommonHandler):

    def initialize(self):
        self.fetch = self.application.fetch

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

        content = json.loads(self.request.body.decode())
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
            target_id = content['target_id']
            result = yield cursor.find_one({'_id': target_id},
                                           {'engines': 1,
                                            'shards': 1,
                                            })
            if core_engine not in result['engines']:
                self.error('Core engine not allowed for this target')
            if not result['shards']:
                self.error('Target specified has no shards')
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
            if user:
                msg['user'] = user
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
        self.error('no streams available for the target')


class SCVStatusHandler(BaseHandler):

    def get(self):
        """
        .. http:put:: /scv/status

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
        self.set_status(200)
        self.write(info)


class TargetDeleteHandler(BaseHandler):

    @tornado.gen.coroutine
    def put(self, target_id):
        """
        .. http:put:: /targets/delete/:target_id

            Delete a target from the Command Center. You must delete all the
            streams from the target to succeed.

            This will not affect mongo's community database in order to
            preserve statistics.

            :status 200: OK
            :status 400: Bad request

        """
        if(yield self.get_current_user()) is None:
            self.error('Bad Manager Credentials', 401)
        self.set_status(400)
        cursor = self.motor.data.targets
        result = yield cursor.remove({'_id': target_id,
                                      'shards': {'$size': 0}})
        if result['n'] > 0:
            return self.set_status(200)
        else:
            self.error('Cannot remove target all streams are removed')


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

    def __init__(self, name, redis_options, mongo_options):
        self.base_init(name, redis_options, mongo_options)
        super(CommandCenter, self).__init__([
            (r'/engines/keys', EngineKeysHandler),
            (r'/engines/keys/delete/(.*)', EngineKeysDeleteHandler),
            (r'/core/assign', CoreAssignHandler),
            (r'/users/verify', UserVerifyHandler),
            (r'/targets', TargetsHandler),
            (r'/targets/delete/(.*)', TargetDeleteHandler),
            (r'/targets/info/(.*)', TargetInfoHandler),
            (r'/targets/update/(.*)', TargetUpdateHandler),
            (r'/scvs/status', SCVStatusHandler),
            ])

    @tornado.gen.coroutine
    def fetch(self, scv_id, path, **kwargs):
        """ Make a request to a particular SCV and keep track of whether or not
        it is alive.

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


def stop_parent(sig, frame):
    pass


def stop_children(sig, frame):
    print('-> stopping children', tornado.process.task_id())
    # stop accepting new requests
    tornado.ioloop.IOLoop.instance().add_callback_from_signal(server.stop)
    app.shutdown()


def start():

    global server
    global app

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
        tornado.process.fork_processes(0)

        signal.signal(signal.SIGINT, stop_children)
        signal.signal(signal.SIGTERM, stop_children)

        server = tornado.httpserver.HTTPServer(app, ssl_options=ssl_opts)
        server.add_sockets(sockets)

        app.initialize_motor()
        if tornado.process.task_id() == 0:
            tornado.ioloop.IOLoop.instance().add_callback(app._check_scvs)
            pulse = tornado.ioloop.PeriodicCallback(app._check_scvs, 5000)
            pulse.start()
        tornado.ioloop.IOLoop.instance().start()
    except SystemExit as e:
        print('! parent is shutting down ...')
        app.db.shutdown()
        sys.exit(0)
