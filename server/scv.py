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

import uuid
import os
import json
import time
import shutil
import hashlib
import base64
import gzip
import functools
import glob

import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httputil
import tornado.httpserver
import tornado.httpclient
import tornado.options
import tornado.process
import tornado.gen

from server.common import BaseServerMixin, configure_options
from server.apollo import Entity, zset, relate


class Stream(Entity):
    prefix = 'stream'
    fields = {'frames': int,  # total number of frames completed
              'status': str,  # 'OK', 'STOPPED'
              'error_count': int,  # number of consecutive errors
              }


class ActiveStream(Entity):
    prefix = 'active_stream'
    fields = {'total_frames': int,  # total frames completed.
              'buffer_frames': int,  # number of frames in buffer.xtc
              'auth_token': str,  # core Authorization token
              'donor': str,  # the donor assigned ? support lookup?
              'steps': int,  # number of steps completed
              'start_time': float,  # time we started at
              'frame_hash': str,  # md5sum of the received frame
              'engine': str,  # which engine is being used
              }


class Target(Entity):
    prefix = 'target'
    fields = {'queue': zset(str)}  # queue of inactive streams


ActiveStream.add_lookup('auth_token')
ActiveStream.add_lookup('donor', injective=False)
relate(Target, 'streams', {Stream}, 'target')
relate(Target, 'active_streams', {ActiveStream})


class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")

    @property
    def db(self):
        return self.application.db

    @property
    def mdb(self):
        return self.application.mdb

    @property
    def motor(self):
        return self.application.motor

    @property
    def deactivate_stream(self):
        return self.application.deactivate_stream

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

    @tornado.gen.coroutine
    def get_stream_owner(self, stream_id):
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        cursor = self.motor.data.targets
        query = yield cursor.find_one({'_id': target_id}, fields=['owner'])
        return query['owner']

    def error(self, message):
        """ Write a message to the output buffer. """
        self.set_status(400)
        self.write({'error': message})

    def authenticate_core(self):
        """ Returns a stream_id if token is valid, None otherwise """
        try:
            token = self.request.headers['Authorization']
            stream_id = ActiveStream.lookup('auth_token', token, self.db)
            if stream_id:
                return stream_id
            else:
                return None
        except:
            return None


def authenticate_cc(method):
    """ Decorator for handlers that require the incoming request's remote_ip
    to be a command center ip or localhost (for testing purposes). """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.request.remote_ip in self.application.cc_ips and\
                self.request.remote_ip != '127.0.0.1':
            self.write({'error': 'unauthorized ip'})
            return self.set_status(401)
        else:
            return method(self, *args, **kwargs)

    return wrapper


def authenticate_core(method):
    """ Decorator for core methods used for authentication. The authorization
    token is mapped to a stream_id that is passed in as an argument.
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            token = self.request.headers['Authorization']
        except:
            self.write({'error': 'missing Authorization header'})
            return self.set_status(401)
        stream_id = ActiveStream.lookup('auth_token', token, self.db)
        if stream_id:
            return method(self, stream_id)
        else:
            self.write({'error': 'bad Authorization header'})
            return self.set_status(401)
    return wrapper


class AliveHandler(BaseHandler):
    def get(self):
        """
        .. http:get:: /

            Used to check and see if the server is up.

            :status 200: OK

        """
        self.set_status(200)


class StreamInfoHandler(BaseHandler):
    def get(self, stream_id):
        """
        .. http:get:: /streams/info/[:stream_id]

            Get information about a particular stream.

            **Example reply**:

            .. sourcecode:: javascript

                {
                    "status": "OK",
                    "frames": 235,
                    "error_count": 0,
                    "active": true
                }

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        stream = Stream(stream_id, self.db)
        body = {
            'status': stream.hget('status'),
            'frames': stream.hget('frames'),
            'error_count': stream.hget('error_count'),
            'active': ActiveStream.exists(stream_id, self.db)
        }
        self.set_status(200)
        self.write(body)


class TargetStreamsHandler(BaseHandler):
    def get(self, target_id):
        """
        .. http:get:: /targets/streams/:target_id

            Get a list of streams for specified target on the scv.

            **Example reply**:

            .. sourcecode:: javascript

                {
                    "streams": [stream_id1, stream_id2, stream_id3, ...]
                }

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        try:
            target = Target(target_id, self.db)
        except KeyError:
            return self.error('specified target does not exist on this scv')
        streams = list(target.smembers('streams'))
        self.set_status(200)
        body = {'streams': streams}
        self.write(body)


class StreamActivateHandler(BaseHandler):
    def post(self):
        """
        .. http:post:: /streams/activate

            Activate and return the highest priority stream of a target by
            popping the head of the priority queue.

            .. note:: This request can only be made by CCs.

            **Example request**

            .. sourcecode:: javascript

                {
                    "target_id": "some_uuid4",
                    "donor_id": "jesse_v" // optional
                    "engine": "engine_name" // optional
                }

            **Example reply**

            .. sourcecode:: javascript

                {
                    "token": "uuid token"
                }

            :status 200: OK
            :status 400: Bad request

        """
        if self.request.headers['Authorization'] != self.application.password:
            return self.error('Not authorized', code=401)
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        target_id = content["target_id"]
        target = Target(target_id, self.db)
        stream_id = target.zrevpop('queue')

        token = str(uuid.uuid4())
        if stream_id:
            fields = {
                'buffer_frames': 0,
                'total_frames': 0,
                'auth_token': token,
                'steps': 0,
                'start_time': time.time()
            }
            if 'donor_id' in content:
                fields['donor'] = content['donor_id']
            if 'engine' in content:
                fields['engine'] = content['engine']
            ActiveStream.create(stream_id, self.db, fields)
            increment = tornado.options.options['heartbeat_increment']
            self.db.zadd('heartbeats', stream_id, time.time() + increment)

            reply = {}
            reply["token"] = token
            self.set_status(200)
            return self.write(reply)
        else:
            return self.error('no streams available')


class StreamsHandler(BaseHandler):
    @tornado.gen.coroutine
    def post(self):
        """
        .. http:post:: /streams

            Add a new stream to this SCV.

            **Example request**

            .. sourcecode:: javascript

                {
                    "target_id": "target_id",
                    "files": {"system.xml.gz.b64": "file1.b64",
                              "integrator.xml.gz.b64": "file2.b64",
                              "state.xml.gz.b64": "file3.b64"
                              }
                }

            .. note:: Binary files must be base64 encoded.

            **Example reply**

            .. sourcecode:: javascript

                {
                    "stream_id" : "715c592f-8487-46ac-a4b6-838e3b5c2543:hello"
                }

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        current_user = yield self.get_current_user()
        if current_user is None:
            return self.error('Bad credentials', 401)

        content = json.loads(self.request.body.decode())
        target_id = content['target_id']
        stream_files = content['files']

        if not Target.exists(target_id, self.db):
            target = Target.create(target_id, self.db)
        else:
            target = Target(target_id, self.db)

        # Bad if server dies here
        cursor = self.motor.data.targets
        result = yield cursor.update({'_id': target_id},
            {'$addToSet': {'shards': self.application.name}})
        if not result['ok']:
            self.set_status(400)
            return self.write(result['ok'])

        stream_id = str(uuid.uuid4())+':'+self.application.name
        stream_dir = os.path.join(self.application.streams_folder, stream_id)
        files_dir = os.path.join(stream_dir, 'files')
        if not os.path.exists(files_dir):
            os.makedirs(files_dir)
        for filename, binary in stream_files.items():
            with open(os.path.join(files_dir, filename), 'w') as handle:
                handle.write(binary)

        pipeline = self.db.pipeline()
        target.zadd('queue', stream_id, 0, pipeline=pipeline)
        stream_fields = {
            'target': target,
            'frames': 0,
            'status': 'OK',
            'error_count': 0
        }
        Stream.create(stream_id, pipeline, stream_fields)
        pipeline.execute()
        self.set_status(200)
        self.write({'stream_id': stream_id})


class StreamStartHandler(BaseHandler):
    @tornado.gen.coroutine
    def put(self, stream_id):
        """
        .. http:put:: /streams/start/:stream_id

            Start a stream and set its status to **OK**.

            :reqheader Authorization: Manager's authorization token

            **Example request**:

            .. sourcecode:: javascript

                {
                    // empty
                }

            :status 200: OK
            :status 400: Bad request

        """
        if not Stream.exists(stream_id, self.db):
            return self.set_status(400)
        current_user = yield self.get_current_user()
        stream_owner = yield self.get_stream_owner(stream_id)
        if stream_owner != current_user:
            self.set_status(401)

        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        target = Target(target_id, self.db)

        if stream.hget('status') != 'OK':
            pipeline = self.db.pipeline()
            stream.hset('status', 'OK', pipeline=pipeline)
            stream.hset('error_count', 0, pipeline=pipeline)
            count = stream.hget('frames')
            target.zadd('queue', stream_id, count, pipeline=pipeline)
            pipeline.execute()

        return self.set_status(200)


class StreamStopHandler(BaseHandler):
    @tornado.gen.coroutine
    def put(self, stream_id):
        """
        .. http:put:: /streams/stop/:stream_id

            Stop a stream and set its status to **STOPPED**.

            :reqheader Authorization: Manager's authorization token

            **Example request**:

            .. sourcecode:: javascript

                {
                    // empty
                }

            :status 200: OK
            :status 400: Bad request

        """
        if not Stream.exists(stream_id, self.db):
            return self.set_status(400)
        current_user = yield self.get_current_user()
        stream_owner = yield self.get_stream_owner(stream_id)
        if stream_owner != current_user:
            self.set_status(401)

        yield self.deactivate_stream(stream_id)
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        target = Target(target_id, self.db)
        if stream.hget('status') != 'STOPPED':
            pipeline = self.db.pipeline()
            stream.hset('status', 'STOPPED', pipeline=pipeline)
            target.zrem('queue', stream_id, pipeline=pipeline)
            pipeline.execute()
        return self.set_status(200)


class StreamDeleteHandler(BaseHandler):
    @tornado.gen.coroutine
    def put(self, stream_id):
        """
        .. http:put:: /streams/delete/:stream_id

            Delete a stream permanently.

            :reqheader Authorization: Manager's authorization token

            **Example request**:

            .. sourcecode:: javascript

                {
                    // empty
                }

            .. note:: When all streams belonging to a target is removed, the
                target and shard information is cleaned up automatically.

            :status 200: OK
            :status 400: Bad request

        """
        # delete from database before deleting from disk
        if not Stream.exists(stream_id, self.db):
            return self.set_status(400)
        current_user = yield self.get_current_user()
        stream_owner = yield self.get_stream_owner(stream_id)
        if stream_owner != current_user:
            self.set_status(401)
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        target = Target(target_id, self.db)

        pipeline = self.db.pipeline()
        yield self.deactivate_stream(stream_id)
        target.zrem('queue', stream_id, pipeline=pipeline)
        stream.delete(pipeline=pipeline)
        pipeline.execute()
        if target.scard('streams') == 0:
            cursor = self.motor.data.targets
            result = yield cursor.update({'_id': target_id},
                {'$pull': {'shards': self.application.name}})
            # we do not check for updatedExisting == False because the target
            # may already be deleted from the mdb if this scv is detached
            # from the target_id's "shards" field.
            if not result['ok']:
                self.set_status(400)
                return self.write(result['ok'])
            target.delete()
        stream_path = os.path.join(self.application.streams_folder, stream_id)
        shutil.rmtree(stream_path)
        self.set_status(200)


class CoreStartHandler(BaseHandler):
    @tornado.gen.coroutine
    def get(self):
        """
        .. http:get:: /core/start

            Get files needed for the core to start an activated stream.

            :reqheader Authorization: core Authorization token

            **Example reply**

            .. sourcecode:: javascript

                {
                    "stream_id": "uuid4",
                    "target_id": "uuid4",
                    "files": {
                        "state.xml.gz.b64": "content.b64",
                        "integrator.xml.gz.b64": "content.b64",
                        "system.xml.gz.b64": "content.b64"
                    }
                    "options": {
                        "steps_per_frame": 50000,
                        "title": "Dihydrofolate Reductase", // used by some
                        "description": "This protein is the canonical benchmark
                            protein used by the MD community."
                        "category": "Benchmark"
                    }
                }

            :status 200: OK
            :status 400: Bad request

        """
        # We need to be extremely careful about checkpoints and frames, as
        # it is important we avoid writing duplicate frames on the first
        # step for the core. We use the follow scheme:
        #
        #               (0,10]                      (10,20]
        #             frameset_10                 frameset_20
        #      -------------------------------------------------------------
        #      |c        core 1      |c|              core 2         |c|
        #      ----                  --|--                           --|--
        # frame x |1 2 3 4 5 6 7 8 9 10| |11 12 13 14 15 16 17 18 19 20| |21
        #         ---------------------| ------------------------------- ---
        #
        # In other words, the core does not write frames for the zeroth frame.
        self.set_status(400)
        stream_id = self.authenticate_core()
        if not stream_id:
            return self.error('Bad core token', code=401)
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        assert stream.hget('status') == 'OK'
        reply = dict()
        reply['files'] = dict()
        files_dir = os.path.join(self.application.streams_folder,
                                 stream_id, 'files')
        frames = stream.hget('frames')
        if frames > 0:
            checkpoint_files = os.path.join(files_dir, frames,
                                            'checkpoint_files')
            for filename in os.listdir(checkpoint_files):
                file_path = os.path.join(checkpoint_files, filename)
                with open(file_path, 'r') as handle:
                    reply['files'][filename] = handle.read()
        for filename in os.listdir(files_dir):
            file_path = os.path.join(files_dir, filename)
            with open(file_path, 'r') as handle:
                if filename not in reply['files']:
                    reply['files'][filename] = handle.read()
        reply['stream_id'] = stream_id
        reply['target_id'] = target_id
        cursor = self.motor.data.targets
        result = yield cursor.find_one({'_id': target_id}, {'options': 1})
        reply['options'] = result['options']
        self.set_status(200)
        return self.write(reply)


class CoreFrameHandler(BaseHandler):
    def put(self):
        """
        ..  http:put:: /core/frame

            Append a frame to the stream's buffer.

            If the core posts to this method, then the WS assumes that the
            frame is valid. The data received is stored in a buffer until a
            checkpoint is received. It is assumed that files given here are
            binary appendable. Files ending in .b64 or .gz are decoded
            automatically.

            :reqheader Authorization: core Authorization token

            **Example request**

            .. sourcecode:: javascript

                {
                    "files" : {
                        "frames.xtc.b64": "file.b64",
                        "log.txt.gz.b64": "file.gz.b64"
                    },
                    "frames": 25,  // optional, number of frames in the files
                }

            :status 200: OK
            :status 400: Bad request

            If the filename ends in b64, it is b64 decoded. If the remaining
            suffix ends in gz, it is unzipped. Afterwards, the file is written
            to disk with the name buffer_[filename], with the b64/gz suffixes
            stripped.

        """
        # There are four intervals:
        #
        # fwi = frame_write_interval (PG Controlled)
        # fsi = frame_send_interval (Core Controlled)
        # cwi = checkpoint_write_interval (Core Controlled)
        # csi = checkpoint_send_interval (Donor Controlled)
        #
        # Where: fwi < fsi = cwi < csi
        #
        # When a set of frames is sent, the core is guaranteed to write a
        # corresponding checkpoint, so that the next checkpoint received is
        # guaranteed to correspond to the head of the buffered files.
        #
        # OpenMM:
        #
        # fwi = fsi = cwi = 50000
        # sci = 2x per day
        #
        # Terachem:
        #
        # fwi = 2
        # fsi = cwf = 100
        # sci = 2x per day
        stream_id = self.authenticate_core()
        if not stream_id:
            return self.error('Bad core token', code=401)
        self.set_status(400)
        active_stream = ActiveStream(stream_id, self.db)
        frame_hash = hashlib.md5(self.request.body).hexdigest()
        if active_stream.hget('frame_hash') == frame_hash:
            return self.set_status(200)
        active_stream.hset('frame_hash', frame_hash)
        content = json.loads(self.request.body.decode())
        if 'frames' in content:
            frame_count = content['frames']
            if frame_count < 1:
                self.set_status(400)
                return self.write({'error': 'frames < 1'})
        else:
            frame_count = 1
        files = content['files']
        streams_folder = self.application.streams_folder
        buffers_folder = os.path.join(streams_folder, stream_id,
                                      'buffer_files')
        if not os.path.exists(buffers_folder):
            os.makedirs(buffers_folder)
        for filename, filedata in files.items():
            filedata = filedata.encode()
            f_root, f_ext = os.path.splitext(filename)
            if f_ext == '.b64':
                filename = f_root
                filedata = base64.b64decode(filedata)
                f_root, f_ext = os.path.splitext(filename)
                if f_ext == '.gz':
                    filename = f_root
                    filedata = gzip.decompress(filedata)
            buffer_filename = os.path.join(buffers_folder, filename)
            with open(buffer_filename, 'ab') as buffer_handle:
                buffer_handle.write(filedata)
        active_stream.hincrby('buffer_frames', frame_count)

        return self.set_status(200)


class CoreCheckpointHandler(BaseHandler):
    def put(self):
        """
        .. http:put:: /core/checkpoint

            Add a checkpoint and flushes buffered files into a state deemed
            safe. It is assumed that the checkpoint corresponds to the last
            frame of the buffered frames.

            :reqheader Authorization: core Authorization token

            **Example Request**

            .. sourcecode:: javascript

                {
                    "files": {
                        "state.xml.gz.b64" : "state.xml.gz.b64"
                    }
                }

            ..note:: filenames must be almost be present in stream_files

            :status 200: OK
            :status 400: Bad request

        """

        # New Algorithm:

        # All buffered frame files are stored in the buffer_files folder:

        # buffer_files/frames.xtc
        # buffer_files/misc.txt

        # When a checkpoint is submitted, the checkpoint files are written to
        # the folder buffer_files/checkpoint_files/

        # When the checkpoints are written successfully, buffer_files is renamed
        # to the frame count. This also marks the successful completion of an
        # atomic transaction.

        # When streams are started, checkpoint_files U initial_files are
        # combined, with filenames in checkpoint_files taking precedence. 

        # When a stream deactivates, buffer_files folder is completedly blown
        # away. Note that when the server starts, all streams are deactivated.

        self.set_status(400)
        stream_id = self.authenticate_core()
        if not stream_id:
            return self.error('Bad core token', code=401)
        content = json.loads(self.request.body.decode())
        stream = Stream(stream_id, self.db)
        active_stream = ActiveStream(stream_id, self.db)
        stream_frames = stream.hget('frames')
        buffer_frames = active_stream.hget('buffer_frames')
        total_frames = stream_frames + buffer_frames
        # Important check for idempotency
        if buffer_frames == 0:
            return self.set_status(200)
        streams_folder = self.application.streams_folder
        buffer_folder = os.path.join(streams_folder, stream_id, 'buffer_files')
        checkpoint_folder = os.path.join(buffer_folder, 'checkpoint_files')
        if not os.path.exists(checkpoint_folder):
            os.makedirs(checkpoint_folder)

        # 1) Extract checkpoint files

        for filename, bytes in content['files'].items():
            checkpoint_bytes = content['files'][filename].encode()
            checkpoint_path = os.path.join(checkpoint_folder, filename)
            with open(checkpoint_path, 'wb') as handle:
                handle.write(checkpoint_bytes)

        # 2) Rename buffer folder
        frame_folder = os.path.join(streams_folder, stream_id,
                                    str(total_frames))
        os.rename(buffer_folder, frame_folder)

        # TODO: If the server crashes here, we need a check to make sure that
        # extraneous buffer frames are cleaned up properly.

        stream.hincrby('frames', buffer_frames)
        active_stream.hincrby('total_frames', buffer_frames)
        active_stream.hset('buffer_frames', 0)
        self.set_status(200)


class CoreStopHandler(BaseHandler):
    @tornado.gen.coroutine
    def put(self):
        """
        ..  http:put:: /core/stop

            Stop the stream and deactivate.

            :reqheader Authorization: core Authorization token

            **Example Request**

            .. sourcecode:: javascript

                {
                    "error": "message_b64",  // optional
                }

            .. note:: ``error`` must be b64 encoded.

            :status 200: OK
            :status 400: Bad request

        """
        stream_id = self.authenticate_core()
        if not stream_id:
            return self.error('Bad core token', code=401)
        # TODO: add field denoting if stream should be finished
        stream = Stream(stream_id, self.db)
        content = json.loads(self.request.body.decode())
        if 'error' in content:
            stream.hincrby('error_count', 1)
            message = base64.b64decode(content['error']).decode()
            log_path = os.path.join(self.application.streams_folder,
                                    stream_id, 'error_log.txt')
            with open(log_path, 'a') as handle:
                handle.write(time.strftime("%c")+'\n'+message)
        self.set_status(200)
        yield self.application.deactivate_stream(stream_id)


class ActiveStreamsHandler(BaseHandler):
    def get(self):
        """
        .. http:get:: /active_streams

            Get information about active streams on the scv.

            **Example Reply**

            .. sourcecode:: javascript

                {
                    "target_id": {
                        "stream_id_1": {
                            "donor_id": None,
                            "start_time": 31875.3,
                            "active_frames": 23
                        }
                    }
                }

            .. note:: ``start_time`` is in seconds since the Unix epoch time.

            .. note:: ``active_frames`` is the number of frames completed by
                core so far.

            :status 200: OK
            :status 400: Bad request

        """
        reply = dict()
        for target in Target.members(self.db):
            # Hardcoded
            streams_key = Target.prefix+':'+target+':streams'
            good_streams = self.db.sinter('active_streams', streams_key)
            if len(good_streams) > 0:
                reply[target] = dict()
            for stream_id in good_streams:
                reply[target][stream_id] = dict()
                active_stream = ActiveStream(stream_id, self.db)
                donor = active_stream.hget('donor')
                start_time = active_stream.hget('start_time')
                active_frames = active_stream.hget('total_frames')
                buffer_frames = active_stream.hget('buffer_frames')
                reply[target][stream_id]['donor'] = donor
                reply[target][stream_id]['start_time'] = start_time
                reply[target][stream_id]['active_frames'] = active_frames
                reply[target][stream_id]['buffer_frames'] = buffer_frames
        self.write(reply)


class StreamReplaceHandler(BaseHandler):
    @tornado.gen.coroutine
    def put(self, stream_id):
        """
        .. http:put:: /streams/replace/:stream_id

            Replace files in ``files`` with other files.

            :reqheader Authorization: manager authorization token

            **Example request**

            .. sourcecode:: javascript

                {
                    "files": {"state.xml.gz.b64": "newstate_3.b64"}
                }

            **Example reply**:

            .. sourcecode:: javascript

                {
                    // empty
                }

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        stream = Stream(stream_id, self.db)
        current_user = yield self.get_current_user()
        stream_owner = yield self.get_stream_owner(stream_id)
        if stream_owner != current_user:
            self.set_status(401)
        if stream.hget('status') != 'STOPPED':
            return self.error('stream must be stopped first')
        content = json.loads(self.request.body.decode())
        files = content['files']
        stream_dir = os.path.join(self.application.streams_folder, stream_id,
                                  'files')
        for filename, binary in files.items():
            if not filename in os.listdir(stream_dir):
                return self.error(filename+' is not in files directory')
        for filename, binary in files.items():
            with open(os.path.join(stream_dir, filename), 'w') as handle:
                handle.write(binary)
        self.set_status(200)


class StreamDownloadHandler(BaseHandler):
    @tornado.gen.coroutine
    def get(self, stream_id, filename):
        """
        .. http:get:: /streams/download/:stream_id/:filename

            Download file ``filename`` from ``stream_id``. ``filename`` can be
            either a file in ``files`` or a frame file posted by the core.
            If it is a frame file, then the frames are concatenated on the fly
            before returning.

            .. note:: Even if ``filename`` is not found, this handler will
                return an empty file with the status code set to 200. This is
                because we cannot distinguish between a frame file that has not
                been received from that of a non-existent file.

            .. note:: This is so far the only method that is not in JSON format
                because the additional 33 percent overhead is far too much for
                large trajectory files.

            :reqheader Authorization: manager authorization token

            :resheader Content-Type: application/octet-stream
            :resheader Content-Disposition: attachment; filename=filename

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        # prevent files from leaking outside of the dir
        streams_folder = self.application.streams_folder
        stream_dir = os.path.join(streams_folder, stream_id)
        file_dir = os.path.dirname(os.path.abspath(os.path.join(
                                                   stream_dir, filename)))
        if(file_dir != os.path.abspath(stream_dir)):
            return
        stream = Stream(stream_id, self.db)
        current_user = yield self.get_current_user()
        stream_owner = yield self.get_stream_owner(stream_id)
        if stream_owner != current_user:
            self.set_status(401)

        buf_size = 4096
        # check if filename is a stream file
        stream_files_path = os.path.join(stream_dir, 'files')
        if filename in os.listdir(stream_files_path):
            filepath = os.path.join(stream_dir, 'files', filename)
            self.set_status(200)
            with open(filepath, 'rb') as f:
                while True:
                    data = f.read(buf_size)
                    if not data:
                        break
                    self.write(data)
                    yield tornado.gen.Task(self.flush)
            self.finish()
            return
        # assume file is a frame file that needs concatenation
        elif stream.hget('frames') > 0:
            file_dirs = sorted([int(f) for f in os.listdir(stream_dir)
                                if f.isdigit()])
            self.set_header('Content-Type', 'application/octet-stream')
            self.set_header('Content-Disposition',
                            'attachment; filename='+filename)
            self.set_status(200)
            for file_dir in file_dirs:
                filepath = os.path.join(stream_dir, str(file_dir), filename)
                if not os.path.exists(filepath):
                    self.write('')
                    return
                with open(filepath, 'rb') as f:
                    while True:
                        data = f.read(buf_size)
                        if not data:
                            break
                        self.write(data)
                        yield tornado.gen.Task(self.flush)
            self.finish()
            return
        else:
            self.write('')
            return self.set_status(200)


class CoreHeartbeatHandler(BaseHandler):
    @authenticate_core
    def post(self, stream_id):
        """
        .. http:post:: /core/heartbeat

            Cores POST to this handler to notify the WS that it is still
            alive.

            :reqheader Authorization: core Authorization token

            :status 200: OK
            :status 400: Bad request

        """
        increment = tornado.options.options['heartbeat_increment']
        self.db.zadd('heartbeats', stream_id, time.time()+increment)
        self.set_status(200)


class SCV(BaseServerMixin, tornado.web.Application):
    def _get_command_centers(self):
        """ Return a dict of Command Center names and hosts """

    @tornado.gen.coroutine
    def _register(self):
        """ Register the SCV in MDB. """
        cursor = self.motor.servers.scvs
        yield cursor.update({'_id': self.name},
                            {'_id': self.name,
                             'password': self.password,
                             'host': self.external_host}, upsert=True)

    def __init__(self, name, external_host, redis_options,
                 mongo_options=None, streams_folder='streams'):
        self.base_init(name, redis_options, mongo_options)
        self.external_host = external_host
        self.streams_folder = os.path.join(self.data_folder, streams_folder)
        self.ccs = None
        self.db.setnx('password', str(uuid.uuid4()))
        self.password = self.db.get('password')
        super(SCV, self).__init__([
            (r'/', AliveHandler),
            (r'/active_streams', ActiveStreamsHandler),
            (r'/streams', StreamsHandler),
            (r'/streams/activate', StreamActivateHandler),
            (r'/streams/info/(.*)', StreamInfoHandler),
            (r'/streams/start/(.*)', StreamStartHandler),
            (r'/streams/stop/(.*)', StreamStopHandler),
            (r'/streams/delete/(.*)', StreamDeleteHandler),
            (r'/streams/download/(.*)/(.*)', StreamDownloadHandler),
            (r'/streams/replace/(.*)', StreamReplaceHandler),
            (r'/targets/streams/(.*)', TargetStreamsHandler),
            (r'/core/start', CoreStartHandler),
            (r'/core/frame', CoreFrameHandler),
            (r'/core/checkpoint', CoreCheckpointHandler),
            (r'/core/stop', CoreStopHandler),
            (r'/core/heartbeat', CoreHeartbeatHandler)
        ])

    def shutdown(self, *args, **kwargs):
        BaseServerMixin.shutdown(self, *args, **kwargs)

    @tornado.gen.coroutine
    def check_heartbeats(self):
        for dead_stream in self.db.zrangebyscore('heartbeats', 0, time.time()):
            yield self.deactivate_stream(dead_stream)

    @tornado.gen.coroutine
    def deactivate_stream(self, stream_id):
        # activation happens atomically so we can deactivate without too much
        # worrying about atomicity
        try:
            active_stream = ActiveStream(stream_id, self.db)
        except KeyError:
            pass
        else:
            self.db.zrem('heartbeats', stream_id)
            stream_path = os.path.join(self.streams_folder, stream_id)
            buffer_path = os.path.join(stream_path, 'buffer_files')
            if os.path.exists(buffer_path):
                shutil.rmtree(buffer_path)
            donor = active_stream.hget('donor')
            engine = active_stream.hget('engine')
            start_time = active_stream.hget('start_time')
            end_time = time.time()
            frames = active_stream.hget('total_frames')
            body = {
                'engine': engine,
                'donor': donor,
                'start_time': start_time,
                'end_time': end_time,
                'frames': frames
            }
            cursor = self.motor.stats.fragments
            yield cursor.insert(body)
            active_stream.delete()
            # push this stream back into queue
            stream = Stream(stream_id, self.db)
            frames_completed = stream.hget('frames')
            target = Target(stream.hget('target'), self.db)
            # TODO: do a check to make sure the stream's status is OK. Check
            # the error count, if it's too high, then the stream is stopped
            target.zadd('queue', stream_id, frames_completed)

#########################
# Defined here globally #
#########################

tornado.options.define('heartbeat_increment', default=900, type=int)
tornado.options.define('check_heart_frequency_in_ms', default=300, type=int)


def start():
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', 'scv.conf')
    configure_options(config_file)
    options = tornado.options.options

    instance = SCV(name=options.name,
                   external_host=options.external_host,
                   redis_options=options.redis_options,
                   mongo_options=options.mongo_options)

    cert_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', options.ssl_certfile)
    key_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            '..', options.ssl_key)
    ca_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           '..', options.ssl_ca_certs)

    server = tornado.httpserver.HTTPServer(instance, ssl_options={
        'certfile': cert_path, 'keyfile': key_path, 'ca_certs': ca_path})
    server.bind(options.internal_http_port)
    server.start(0)
    instance.initialize_motor()

    if tornado.process.task_id() == 0:
        tornado.ioloop.IOLoop.instance().add_callback(instance.check_heartbeats)
        tornado.ioloop.IOLoop.instance().add_callback(instance._register)
        frequency = tornado.options.options['check_heart_frequency_in_ms']
        pulse = tornado.ioloop.PeriodicCallback(instance.check_heartbeats,
                                                frequency)
        pulse.start()
    tornado.ioloop.IOLoop.instance().start()
