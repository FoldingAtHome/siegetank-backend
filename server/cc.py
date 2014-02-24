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
import time
import functools
import bcrypt
import pymongo
import io
import socket

from server.common import BaseServerMixin, is_domain, configure_options
from server.apollo import Entity, relate

# The Command Center manages several work servers in addition to managing the
# stats system for each work server.

# CC uses Antirez's redis extensively as NoSQL store mainly because of its
# blazing fast speed, atomicity across different proccesses, and database
# recovery features. Note: binary blobs such as states, systems, and frames are
# written directly to disk instead

# For more information on redis memory usage, visit:
# http://nosql.mypopescu.com/post/1010844204/redis-memory-usage

###################
# Fault tolerance #
###################

# There are three failure modes that could happen:

# 1. CC Up - WS Down: When the WS restarts, it notifies all the WS that it is
#                     alive via RegisterWSHandler (which resets the fail_count)

# 2. CC Down - WS Up: When the CC restarts, it pings all of the WS in its redis
#                     databases to see if they are alive.

# 3. CC Up - WS Up - Network Down: The CC will see that the WS is down, but the
#                                  WS won't explicitly restart since it may be
#                                  slaves to multiple CCs. So the only way is
#                                  for the CC to periodically ping the downed
#                                  WSs via a periodic callback.


class WorkServer(Entity):
    prefix = 'ws'
    fields = {'url': str,  # http request url (verify based on if IP or not)
              'ip': str,  # ip address of the workserver
              'http_port': int,  # ws http port
              'fail_count': int,  # number of times a request has failed
              }

WorkServer.add_lookup('ip', injective=True)


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


def authenticate_manager(method):
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


class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

    def get_current_user(self):
        try:
            header_token = self.request.headers['Authorization']
            mdb = self.application.mdb
            query = mdb.managers.find_one({'token': header_token},
                                          fields=['_id'])
            return query['_id']
        except:
            return None

    def get_user_role(self, email):
        mdb = self.application.mdb
        query = mdb.managers.find_one({'_id': email}, fields={'role'})
        try:
            return query['role']
        except:
            return None

    @tornado.gen.coroutine
    def fetch(self, ws_id, path, **kwargs):
        """
        This is a fairly special method. First, it takes care of some boiler
        plate code. Second, it keeps track of how many times a workserver has
        failed. If it has failed one too many times, then the workserver is
        taken offline.

        """
        workserver = WorkServer(ws_id, self.db)
        if 'ws_url' in kwargs:
            ws_url = kwargs['ws_url']
            kwargs.pop("ws_url", None)
        else:
            ws_url = workserver.hget('url')
        if 'ws_port' in kwargs:
            ws_port = kwargs['ws_port']
            kwargs.pop("ws_port", None)
        else:
            ws_port = workserver.hget('http_port')
        ws_uri = 'https://'+ws_url+':'+str(ws_port)+path
        client = tornado.httpclient.AsyncHTTPClient()
        try:
            reply = yield client.fetch(ws_uri, validate_cert=is_domain(ws_url),
                                       **kwargs)
            workserver.hset('fail_count', 0)
            return reply
        except socket.gaierror:
            print('SOCKET_ERROR DETECTED')
            workserver.hincrby('fail_count', 1)
            dummy = tornado.httpclient.HTTPRequest(ws_url)
            tempb = io.BytesIO(json.dumps({'error': 'WS down'}).encode())
            reply = tornado.httpclient.HTTPResponse(dummy, 400, buffer=tempb)
            return reply


class AuthDonorHandler(tornado.web.RequestHandler):
    def post(self):
        """ Generate a new authorization token for the donor

        .. http:post:: /auth

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
        """
        .. http:post:: /auth

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


class AddManagerHandler(BaseHandler):
    def post(self):
        """
        .. http:post:: /managers

            Add a manager. The request must have originated from localhost or
            a user whose role is admin. Admins can add new managers, delete
            managers, in addition to modifying any target

            :reqheader Authorization: access token of an administrator

            **Example request**

            .. sourcecode:: javascript

                {
                    "email": "proteneer@gmail.com",
                    "password": "password",
                    "role": "admin" or "manager"
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
        role = content['role']
        if not role in ['admin', 'manager']:
            return self.write(json.dumps({'error': 'bad role'}))
        hash_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
        db_body = {'_id': email,
                   'password_hash': hash_password,
                   'token': token,
                   'role': role
                   }

        managers = self.application.mdb.managers
        try:
            managers.insert(db_body)
        except pymongo.errors.DuplicateKeyError:
            self.write(json.dumps({'error': email+' exists'}))
            return

        self.set_status(200)
        self.write(json.dumps({'token': token}))


class UpdateStageHandler(BaseHandler):
    @authenticate_manager
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

            .. note:: If ``target_id`` is specified, then the WS will try and
                activate one of its streams. In addition, the stage of the
                target is not taken into consideration. Note that ``target_id``
                must be the fully qualified 36 digit uuid.

                Otherwise, we try and find a ``target_id`` whose engine version
                is compatible with the core's engine_version and `stage` is
                either "beta" or "public".

            **Example reply**

            .. sourcecode:: javascript

                {
                    "token": "6lk2j5-tpoi2p6-poipoi23",
                    "url": "https://raynor.stanford.edu:1234/core/start",
                    "steps_per_frame": 50000
                }

            :status 200: OK
            :status 400: Bad request

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
                    break

            # if we reached here then we didn't find a good target
            if not found:
                err = 'no public or beta targets available'
                return self.write(json.dumps({'error': err}))

        steps_per_frame = target.hget('steps_per_frame')

        # shuffle and find an online workserver
        striated_servers = list(filter(lambda x: self.application.ws_online(x),
                                       target.smembers('striated_ws')))
        random.shuffle(striated_servers)

        for ws_id in striated_servers:
            workserver = WorkServer(ws_id, self.db)
            ws_url = workserver.hget('url')
            ws_port = workserver.hget('http_port')
            ws_body = {}
            ws_body['target_id'] = target_id
            if donor_id:
                ws_body['donor_id'] = donor_id
            try:
                reply = yield self.fetch(ws_id, '/streams/activate',
                                         ws_url=ws_url, method='POST',
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
                print('HTTP_ERROR::', e)
                pass

        self.write(json.dumps({'error': 'no free WS available'}))


class DisconnectWSHandler(BaseHandler):
    def put(self):
        """
        .. http:put:: /disconnect_ws

            Disconnect a WorkServer, setting its status to offline.

            :reqheader Authorization: Secret password of the CC

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
            :Status 400: Bad request
            :status 401: Unauthorized

        """
        self.set_status(400)
        auth = self.request.headers['Authorization']
        if auth != self.application.cc_pass:
            return self.set_status(401)
        content = json.loads(self.request.body.decode())
        name = content['name']
        ws = WorkServer(name, self.db)
        ws.hset('fail_count', self.application._max_ws_fails)
        self.set_status(200)
        return self.write(json.dumps({}))


class RegisterWSHandler(BaseHandler):
    def put(self):
        """
        .. http:put:: /register_ws

            Register a WorkServer to be managed by this command center by
            presenting the secret password.

            :reqheader Authorization: Secret password of the CC

            **Example request**

            .. sourcecode:: javascript

                {
                    "name": "some_workserver",
                    "url": "workserver.stanford.edu",
                    "http_port": 443
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
        auth = self.request.headers['Authorization']
        if auth != self.application.cc_pass:
            return self.set_status(401)
        content = json.loads(self.request.body.decode())
        name = content['name']
        url = content['url']
        http_port = content['http_port']
        self.application.add_ws(name, url, http_port)
        print('WS '+content['name']+' is now connected')
        self.set_status(200)
        return self.write(json.dumps({}))


class DeleteStreamHandler(BaseHandler):
    @authenticate_manager
    @tornado.gen.coroutine
    def put(self, stream_id):
        """
        .. http:put:: /streams/delete/:stream_id

            Deletes a stream from the server.

            .. note:: This is a relatively slow method. Partially because 1) we
                don't know which server the stream is on and 2) we don't know
                its target so we need to check all workservers.

            :reqheader Authorization: Manager's authorization token

            **Example request**

            .. sourcecode:: javascript

                {
                    "stream_id": stream_id,
                }

            :status 200: OK
            :status 400: Bad request
            :status 401: Unauthorized

        """
        # this is a relatively slow method. Partially because 1) we don't know
        # which server the stream is on, and 2) we don't know its target so
        # we don't know which servers its striating over
        self.set_status(400)
        found = False
        for ws_name in WorkServer.members(self.db):
            body = {
                "stream_id": stream_id
            }
            rep = yield self.fetch(ws_name, '/streams/delete', method='PUT',
                                   body=json.dumps(body))
            if rep.code == 200:
                found = True
                break
        if found:
            self.set_status(200)
            self.write(json.dumps({}))
        else:
            self.write(json.dumps({'error': 'stream not found'}))


class PostStreamHandler(BaseHandler):
    @authenticate_manager
    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /streams

            Add a new stream to an existing target.

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

        body = {
            'target_id': target_id,
        }

        body['stream_files'] = {}

        for filename, filebin in files.items():
            body['stream_files'][filename] = filebin

        # if this target is not yet striating over the ws,
        # include include the target_files
        if not ws_id in target.smembers('striated_ws'):
            target_files = target.smembers('files')
            body['target_files'] = {}
            for filename in target_files:
                file_path = os.path.join(self.application.targets_folder,
                                         target_id, filename)
                with open(file_path, 'rb') as handle:
                    body['target_files'][filename] = handle.read().decode()

        rep = yield self.fetch(ws_id, '/streams', method='POST',
                               body=json.dumps(body))

        if rep.code == 200:
            target.sadd('striated_ws', ws_id)

        self.set_status(rep.code)
        return self.write(rep.body)


class GetTargetHandler(BaseHandler):
    def get(self, target_id):
        """
        .. http:get:: /targets/info/:target_id

            Get detailed information about a target

            **Example reply**

            .. sourcecode:: javascript

                {
                    "description": "Some secret project",
                    "owner": "diwakar@gmail.com",
                    "steps_per_frame": 50000,
                    "creation_date": 1392784469,
                    "stage": "beta",
                    "allowed_ws": ["raynor", "zeratul", "kerrigan"],
                    "striated_ws": ["raynor", "zeratul"],
                    "engine": "openmm"
                    "engine_versions": ["6.0"]
                }

            .. note:: ``creation_date`` is in seconds since epoch 01/01/1970.

            :status 200: OK
            :status 400: Bad request

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
        """
        .. http:get:: /targets/streams/:target_id

            Return a list of streams on each striated workserver for
            *target_id*.

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
                body[ws_name] = json.loads(reply.body.decode())

        self.set_status(200)
        self.write(json.dumps(body))


class TargetsHandler(BaseHandler):
    def get(self):
        """
        .. http:post:: /targets

            Return a list of all the targets on the CC. If a manager is
            authenticated, then only his set of targets will be returned.

            **Example reply**

            .. sourcecode:: javascript

                {
                    'targets': ['target_id1', 'target_id2', '...']
                }

            :status 200: OK
            :status 400: Bad request

        """
        target_ids = Target.members(self.db)

        manager = self.get_current_user()

        # if number of targets increases dramatically:
        # 1) try pipelining
        # 2) add a reverse lookup of donors to targets
        if manager:
            matched_targets = []
            for target_id in target_ids:
                target = Target(target_id, self.db)
                if target.hget('owner') == manager:
                    matched_targets.append(target_id)
            return self.write(json.dumps({'targets': list(matched_targets)}))
        else:
            return self.write(json.dumps({'targets': list(target_ids)}))

    @authenticate_manager
    def post(self):
        """
        .. http:post:: /targets

            Add a new target to be managed by this command center.

            :reqheader Authorization: Manager's authorization token

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

            .. note:: If ``allowed_ws`` is specified, then we striate streams
                for the target only over the specified workserver. Otherwise
                all workservers available to this cc will be striated over.
            .. note:: If ``stage`` is not given, then the stage defaults to
                "private".

            **Example reply**

            .. sourcecode:: javascript

                {
                    "target_id": "uuid4"
                }

            :status 200: OK
            :status 400: Bad request

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

        fields = {
            'description': description,
            'steps_per_frame': steps_per_frame,
            'creation_date': time.time(),
            'engine': engine,
            'owner': self.get_current_user(),
        }
        if 'stage' in content:
            if content['stage'] in ['private', 'beta', 'public']:
                fields['stage'] = content['stage']
        else:
            fields['stage'] = 'private'
        allowed_versions = set(content['engine_versions'])
        fields['engine_versions'] = allowed_versions
        if 'allowed_ws' in content:
            allowed_workservers = set(content['allowed_ws'])
            fields['allowed_ws'] = allowed_workservers
        fields['files'] = set(files.keys())

        Target.create(target_id, self.db, fields)

        target_dir = os.path.join(self.application.targets_folder, target_id)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        for filename, filebin in files.items():
            file_path = os.path.join(target_dir, filename)
            with open(file_path, 'wb') as handle:
                handle.write(filebin.encode())

        mdb = self.application.mdb
        cc_id = self.application.name
        mdb.managers.update({'_id': self.get_current_user()},
                            {'$push': {'targets.'+cc_id: target_id}})

        self.set_status(200)
        response = {'target_id': target_id}

        return self.write(json.dumps(response))

    # def delete(self):
    #     # delete all streams

    #     # delete the project
    #     return


class CommandCenter(BaseServerMixin, tornado.web.Application):

    def __init__(self, cc_name, cc_pass, redis_options, core_keys=set(),
                 mongo_options=None, targets_folder='targets'):
        print('Starting up Command Center:', cc_name)
        self.base_init(cc_name, redis_options, mongo_options)
        self.cc_pass = cc_pass
        self.targets_folder = targets_folder
        super(CommandCenter, self).__init__([
            (r'/core/assign', AssignHandler),
            (r'/managers/auth', AuthManagerHandler),
            (r'/managers', AddManagerHandler),
            (r'/donors/auth', AuthDonorHandler),
            (r'/donors', AddDonorHandler),
            (r'/targets', TargetsHandler),
            (r'/targets/info/(.*)', GetTargetHandler),
            (r'/targets/streams/(.*)', ListStreamsHandler),
            (r'/register_ws', RegisterWSHandler),
            (r'/disconnect_ws', DisconnectWSHandler),
            (r'/streams', PostStreamHandler),
            (r'/streams/delete/(.*)', DeleteStreamHandler)
            ])

    _max_ws_fails = 10

    def ws_online(self, ws_id):
        """ returns True if the workserver is online, False otherwise """
        ws = WorkServer(ws_id, self.db)
        if ws.hget('fail_count') < self._max_ws_fails:
            return True
        else:
            return False

    # this is mainly here for unit testing purposes
    def add_ws(self, name, url, http_port):
        if not WorkServer.exists(name, self.db):
            fields = {'url': url,
                      'http_port': http_port,
                      'fail_count': 0
                      }
            ws = WorkServer.create(name, self.db, fields)
        else:
            ws = WorkServer(name, self.db)
            pipe = self.db.pipeline()
            ws.hset('url', url, pipeline=pipe)
            ws.hset('http_port', http_port, pipeline=pipe)
            ws.hset('fail_count', 0, pipeline=pipe)
            pipe.execute()


def start():
    extra_options = {
        'external_http_port': int,
        'cc_pass': str,
        'allowed_core_keys': set
    }
    conf_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', 'cc.conf')
    configure_options(extra_options, conf_path)
    options = tornado.options.options

    instance = CommandCenter(cc_name=options.name,
                             cc_pass=options.cc_pass,
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
    tornado.ioloop.IOLoop.instance().start()
