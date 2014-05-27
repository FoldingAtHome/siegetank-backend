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
import logging
import signal
import functools
import sys

import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httputil
import tornado.httpserver
import tornado.httpclient
import tornado.options
import tornado.process
import tornado.gen
import tornado.netutil

from server.common import BaseServerMixin, configure_options, CommonHandler
from server.apollo import Entity, zset, relate
from server import process2


class Stream(Entity):
    prefix = 'stream'
    fields = {'frames': int,  # total number of frames completed
              'status': str,  # 'OK', 'STOPPED'
              'error_count': int,  # number of consecutive errors,
              'creation_date': int,  # when the stream was created,
              'checkpoints': str,  # checkpoints string, "1, 3, 587, 928"
              }


class ActiveStream(Entity):
    prefix = 'active_stream'
    fields = {'total_frames': int,  # total frames completed.
              'buffer_frames': int,  # number of frames in buffer.xtc
              'auth_token': str,  # core Authorization token
              'user': str,  # the user assigned to the stream
              'start_time': float,  # time we started at
              'frame_hash': str,  # md5sum of the received frame
              'engine': str,  # which engine is being used
              }


class Target(Entity):
    prefix = 'target'
    fields = {'queue': zset(str)}  # queue of inactive streams

relate(Target, 'streams', {Stream}, 'target')

class BaseHandler(CommonHandler):

    def get_stream_id_from_token(self, token):
        return self.db.get('auth_token:'+token+':active_stream')

    def initialize(self):
        self.deactivate_stream = self.application.deactivate_stream
        self.start_lock = self.application.start_lock

    @tornado.gen.coroutine
    def get_stream_owner(self, stream_id):
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        cursor = self.motor.data.targets
        query = yield cursor.find_one({'_id': target_id}, fields=['owner'])
        return query['owner']

    def authenticate_core(self):
        """ Authenticate a core to see if the given token is mapped to a
        particular active_stream or not. If so, it returns (stream_id, method),
        where method is used for unlocking the stream.
        """

        if not 'Authorization' in self.request.headers:
            self.error('Missing Authorization header')
        token = self.request.headers['Authorization']
        script = """
        local token = KEYS[1]
        local time = KEYS[2]
        local stream_id = redis.call('get', 'auth_token:'..token..':active_stream')
        if not stream_id then
            return -2
        end
        local already_locked = redis.call('zscore', 'locks', stream_id)
        if already_locked then
            return -1
        else
            redis.call('zadd', 'locks', time, stream_id)
            return stream_id
        end
        """

        action = self.db.register_script(script)
        start_time = time.time()
        attempts = 0
        while(True):
            result = action(keys=[token, time.time()])
            if result == -2:
                self.error('invalid stream')
            elif result == -1:
                print('sleeping')
                time.sleep(0.1)
                if attempts == 2:
                    self.error('stream is busy', code=400)
                attempts += 1
            else:
                stream_id = result
                unlock = functools.partial(self.application.release_lock, stream_id)
                return stream_id, unlock

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
            self.error('specified target does not exist on this scv')
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
                    "engine": "engine_name",
                    "user": "jesse_v" // optional
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
            self.error('Not authorized', code=401)
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        target_id = content["target_id"]

        # this method does check for locks the stream as it assumes that if
        # a stream has been deactivated, then it is fully safe. That is, it has
        # no pending filesystem operations or other non-redis pending ops.

        script = """
        local target_id = KEYS[1]
        local token = KEYS[2]
        local engine = KEYS[3]
        local user = KEYS[4]
        local expiration = KEYS[5]
        local stime = KEYS[6]
        local target_queue = 'target:'..target_id..':queue'
        local stream_id = redis.call('zrange', target_queue, -1, -1)[1]
        if stream_id then
            redis.call('zremrangebyrank', target_queue, -1, -1)
            redis.call('sadd', 'active_streams', stream_id)
            redis.call('hset', 'active_stream:'..stream_id, 'buffer_frames', 0)
            redis.call('hset', 'active_stream:'..stream_id, 'total_frames', 0)
            redis.call('hset', 'active_stream:'..stream_id, 'auth_token', token)
            redis.call('hset', 'active_stream:'..stream_id, 'user', user)
            redis.call('hset', 'active_stream:'..stream_id, 'start_time', stime)
            redis.call('hset', 'active_stream:'..stream_id, 'engine', engine)
            redis.call('zadd', 'heartbeats', expiration, stream_id)
            redis.call('set', 'auth_token:'..token..':active_stream', stream_id)
        end
        return stream_id
        """

        action = self.db.register_script(script)
        token = str(uuid.uuid4())
        if 'user' in content:
            user = content['user']
        else:
            user = None
        expiration = time.time()+tornado.options.options['heartbeat_increment']
        stime = time.time()
        result = action(keys=[target_id, token, content['engine'], user,
                              expiration, stime])
        if result:
            self.set_status(200)
            return self.write({'token': token})
        else:
            self.error('No streams available')


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
            self.error('Invalid user', 401)
        if not self.is_manager():
            self.error('User is not a manager', 401)
        content = json.loads(self.request.body.decode())
        target_id = content['target_id']
        stream_files = content['files']
        stream_id = str(uuid.uuid4())+':'+self.application.name
        unlock = self.start_lock(stream_id)
        script = """
        local target_id = KEYS[1]
        local stream_id = KEYS[2]
        local new_target = false
        if redis.call('sismember', 'targets', target_id) == 0 then
            redis.call('sadd', 'targets', target_id)
            new_target = true
        end
        redis.call('sadd', 'streams', stream_id)
        redis.call('hset', 'stream:'..stream_id, 'target', target_id)
        redis.call('hset', 'stream:'..stream_id, 'frames', 0)
        redis.call('hset', 'stream:'..stream_id, 'status', 'OK')
        redis.call('hset', 'stream:'..stream_id, 'error_count', 0)
        redis.call('zadd', 'target:'..target_id..':queue', 0, stream_id)
        redis.call('sadd', 'target:'..target_id..':streams', stream_id)
        return new_target
        """
        action = self.db.register_script(script)
        is_new_target = action(keys=[target_id, stream_id])
        stream_dir = os.path.join(self.application.streams_folder, stream_id)
        files_dir = os.path.join(stream_dir, 'files')
        if not os.path.exists(files_dir):
            os.makedirs(files_dir)
        for filename, binary in stream_files.items():
            with open(os.path.join(files_dir, filename), 'w') as handle:
                handle.write(binary)
        unlock()

        if is_new_target:
            cursor = self.motor.data.targets
            result = yield cursor.update({'_id': target_id},
                {'$addToSet': {'shards': self.application.name}})
            if not result['ok']:
                self.set_status(400)
                return self.write('Could not access mdb while adding stream')

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
            return self.set_status(401)
        unlock = self.start_lock(stream_id)
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
        unlock()
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
            return self.set_status(401)
        unlock = self.start_lock(stream_id)
        yield self.deactivate_stream(stream_id)
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        target = Target(target_id, self.db)
        if stream.hget('status') != 'STOPPED':
            pipeline = self.db.pipeline()
            stream.hset('status', 'STOPPED', pipeline=pipeline)
            target.zrem('queue', stream_id, pipeline=pipeline)
            pipeline.execute()
        unlock()
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
            self.error('Invalid stream_id:', stream_id)
        current_user = yield self.get_current_user()
        stream_owner = yield self.get_stream_owner(stream_id)
        if stream_owner != current_user:
            return self.set_status(401)
        unlock = self.start_lock(stream_id)
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        target = Target(target_id, self.db)
        yield self.deactivate_stream(stream_id)
        pipeline = self.db.pipeline()
        target.zrem('queue', stream_id, pipeline=pipeline)
        stream.delete(pipeline=pipeline)
        pipeline.execute()
        stream_path = os.path.join(self.application.streams_folder, stream_id)
        # TODO: can change to subprocess.call(['rm', '-rf', stream_path])
        # since it's much much faster (4x as fast in certain scenarios)
        shutil.rmtree(stream_path)
        self.set_status(200)
        unlock()
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
        stream_id, unlock = self.authenticate_core()
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        assert stream.hget('status') == 'OK'
        reply = dict()
        reply['files'] = dict()
        seed_files_dir = os.path.join(self.application.streams_folder,
                                      stream_id, 'files')
        frames = stream.hget('frames')
        if frames > 0:
            checkpoint_files = os.path.join(self.application.streams_folder,
                                            stream_id, str(frames),
                                            'checkpoint_files')
            for filename in os.listdir(checkpoint_files):
                file_path = os.path.join(checkpoint_files, filename)
                with open(file_path, 'r') as handle:
                    reply['files'][filename] = handle.read()
        for filename in os.listdir(seed_files_dir):
            file_path = os.path.join(seed_files_dir, filename)
            with open(file_path, 'r') as handle:
                if filename not in reply['files']:
                    reply['files'][filename] = handle.read()
        unlock()
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

        """
        # There are four intervals:
        #
        # fwi = frame_write_interval (PG Controlled)
        # fsi = frame_send_interval (Core Controlled)
        # cwi = checkpoint_write_interval (Core Controlled)
        # csi = checkpoint_send_interval (User Controlled)
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
        stream_id, unlock = self.authenticate_core()
        self.set_status(400)
        active_stream = ActiveStream(stream_id, self.db)
        frame_hash = hashlib.md5(self.request.body).hexdigest()
        if active_stream.hget('frame_hash') == frame_hash:
            unlock()
            return self.set_status(200)
        active_stream.hset('frame_hash', frame_hash)
        content = json.loads(self.request.body.decode())
        if 'frames' in content:
            frame_count = content['frames']
            if frame_count < 1:
                self.set_status(400)
                unlock()
                self.error('frames < 1')
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
        unlock()
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

        # All buffered frame files are stored in the buffer_files folder:

        # buffer_files/frames.xtc
        # buffer_files/misc.txt

        # When a checkpoint is submitted, the checkpoint files are written to
        # the folder buffer_files/checkpoint_files/

        # When the checkpoints are written successfully, buffer_files is renamed
        # to the frame count. This also marks the successful completion of an
        # atomic transaction.

        # When streams are started, checkpoint_files U seed_files are
        # combined, with filenames in checkpoint_files taking precedence.

        # When a stream deactivates, buffer_files folder is completedly blown
        # away. Note that when the server starts, all streams are deactivated.

        self.set_status(400)
        stream_id, unlock = self.authenticate_core()
        content = json.loads(self.request.body.decode())
        stream = Stream(stream_id, self.db)
        active_stream = ActiveStream(stream_id, self.db)
        stream_frames = stream.hget('frames')
        buffer_frames = active_stream.hget('buffer_frames')
        total_frames = stream_frames + buffer_frames
        # Important check for idempotency
        if buffer_frames == 0:
            unlock()
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

        # 3) TODO: remove extra checkpoints

        # TODO: If the server crashes here, we need a check to make sure that
        # extraneous buffer frames are cleaned up properly.

        stream.hincrby('frames', buffer_frames)
        active_stream.hincrby('total_frames', buffer_frames)
        active_stream.hset('buffer_frames', 0)

        unlock()

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
        stream_id, unlock = self.authenticate_core()
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
        yield self.deactivate_stream(stream_id)
        unlock()


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
                            "user": None,
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
                user = active_stream.hget('user')
                start_time = active_stream.hget('start_time')
                active_frames = active_stream.hget('total_frames')
                buffer_frames = active_stream.hget('buffer_frames')
                reply[target][stream_id]['user'] = user
                reply[target][stream_id]['start_time'] = start_time
                reply[target][stream_id]['active_frames'] = active_frames
                reply[target][stream_id]['buffer_frames'] = buffer_frames
        self.write(reply)


class StreamSyncHandler(BaseHandler):

    @tornado.gen.coroutine
    def get(self, stream_id):
        """
        .. http:get:: /streams/sync/:stream_id

            Retrieve the information needed to sync data back in an efficient
            manner. This method does not invoke os.walk() or anything that
            requires invoking stat on a large number of files.

            If the partition is comprised of the list [5, 12, 38], then the
            stream is divided into the partition (0, 5](5, 12](12, 38], where
            (a,b] denote the open and closed ends.

            :reqheader Authorization: manager authorization token

            **Example reply**:

            .. sourcecode:: javascript

                {
                    'partitions': [5, 12, 38],
                    'frame_files': ['frames.xtc', 'log.txt'],
                    'checkpoint_files': ['state.xml.gz.b64'], // optional
                    'seed_files': ['state.xml.gz.b64',
                                      'system.xml.gz.b64',
                                      'integrator.xml.gz.b64']
                }

            .. note:: If 'partitions' is not an empty list, then 'frame_files'
                and 'checkpoint_files' are present.

        """
        self.set_status(400)
        if not Stream.exists(stream_id, self.db):
            self.error('Stream does not exist')
        current_user = yield self.get_current_user()
        stream_owner = yield self.get_stream_owner(stream_id)
        if stream_owner != current_user:
            return self.set_status(401)
        streams_folder = self.application.streams_folder
        stream_dir = os.path.join(streams_folder, stream_id)
        folders = os.listdir(stream_dir)
        partitions = []
        for item in folders:
            try:
                partitions.append(int(item))
            except:
                pass
        partitions = sorted(partitions)
        seed_files = os.listdir(os.path.join(stream_dir, 'files'))
        reply = {
            'partitions': partitions,
            'seed_files': seed_files,
        }
        if len(partitions) > 0:
            frame_dir = os.path.join(stream_dir, str(partitions[0]))
            frame_files = os.listdir(frame_dir)
            chkpt_name = 'checkpoint_files'
            try:
                frame_files.remove(chkpt_name)
                chkpt_files = os.listdir(os.path.join(frame_dir, chkpt_name))
                reply['checkpoint_files'] = chkpt_files
            except:
                pass
            reply['frame_files'] = frame_files

        self.set_status(200)
        self.write(reply)


class StreamUploadHandler(BaseHandler):

    @tornado.gen.coroutine
    def put(self, stream_id, filename):
        """
        .. http:put:: /streams/upload/:stream_id/:filename

            Upload a new file. This file must exist.

            .. note:: Uploads do not support streaming, so you must take care
                that the data sent does not exceed 10MB.

            **Example Request**

            :reqheader Authorization: manager authorization token
            :reqheader Content-MD5: MD5 hash of the file

            .. sourcecode:: javascript

                [binary_data]

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        current_user = yield self.get_current_user()
        stream_owner = yield self.get_stream_owner(stream_id)
        if stream_owner != current_user:
            return self.set_status(401)
        stream = Stream(stream_id, self.db)
        if stream.hget('status') != 'STOPPED':
            self.error('Stream must be stopped before upload')
        # prevent files from leaking out
        streams_folder = self.application.streams_folder
        stream_dir = os.path.abspath(os.path.join(streams_folder, stream_id))
        requested_file = os.path.abspath(os.path.join(stream_dir, filename))
        if len(requested_file) < len(stream_dir):
            return
        if requested_file[0:len(stream_dir)] != stream_dir:
            return
        if not os.path.exists(requested_file):
            self.error('Requested file does not exist')
        md5 = self.request.headers.get('Content-MD5')
        if md5 != hashlib.md5(self.request.body).hexdigest():
            self.error('MD5 mismatch')
        with open(requested_file, 'wb') as f:
            f.write(self.request.body)
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

            :reqheader Authorization: manager authorization token

            :resheader Content-Type: application/octet-stream
            :resheader Content-Disposition: attachment; filename=filename
            :resheader Content-Length: size of file

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        streams_folder = self.application.streams_folder
        stream_dir = os.path.abspath(os.path.join(streams_folder, stream_id))
        requested_file = os.path.abspath(os.path.join(stream_dir, filename))
        # prevent files from leaking out
        if len(requested_file) < len(stream_dir):
            return
        if requested_file[0:len(stream_dir)] != stream_dir:
            return
        if not Stream.exists(stream_id, self.db):
            self.error('Stream does not exist')
        current_user = yield self.get_current_user()
        stream_owner = yield self.get_stream_owner(stream_id)
        if stream_owner != current_user:
            return self.set_status(401)
        if not os.path.exists(requested_file):
            self.error('Requested file does not exist')

        self.set_header('Content-Type', 'application/octet-stream')
        self.set_header('Content-Length', os.path.getsize(requested_file))
        self.set_header('Content-Disposition',
                        'attachment; filename='+filename)
        self.set_status(200)
        buf_size = 2048
        with open(requested_file, 'rb') as f:
            while True:
                data = f.read(buf_size)
                if not data:
                    break
                self.write(data)
                yield tornado.gen.Task(self.flush)
        self.finish()


class CoreHeartbeatHandler(BaseHandler):

    def post(self):
        """
        .. http:post:: /core/heartbeat

            Cores POST to this handler to notify the WS that it is still
            alive.

            :reqheader Authorization: core Authorization token

            :status 200: OK
            :status 400: Bad request

        """
        stream_id, unlock = self.authenticate_core()
        increment = tornado.options.options['heartbeat_increment']
        self.db.zadd('heartbeats', stream_id, time.time()+increment)
        self.set_status(200)
        unlock()


class SCV(BaseServerMixin, tornado.web.Application):

    def start_lock(self, stream_id):
        """ Lock stream_id and return an unlock function for invocation later
        on. Acquiring the lock fully blocks the event loop.

        Usage:

        unlock = self.start_lock(stream_id)
        # do something to stream
        unlock()

        """
        start_time = time.time()
        while(True):
            if self.acquire_lock(stream_id):
                return functools.partial(self.release_lock, stream_id)
            elif time.time() - start_time > 0.1:
                raise tornado.web.HTTPError(400, reason="stream is busy")

    @tornado.gen.coroutine
    def scruffy(self):
        """ Cleans up after streams with bad locks. """
        print('Checking for bad locks ...')
        for stream_id in self.db.zrangebyscore('locks', 0, time.time()-3):
            message = 'Scruffy found a bad stream: '+stream_id
            logging.getLogger('tornado.application').critical(message)
            if not Stream.exists(stream_id, self.db):
                stream_dir = os.path.join(self.streams_folder, stream_id)
                if os.path.exists(stream_dir):
                    os.shutil.rmtree(stream_dir)
            else:
                yield self.deactivate_stream(stream_id)
            self.release_lock(stream_id)

    def acquire_lock(self, resource):
        script = """
        local resource = KEYS[1]
        local time = KEYS[2]
        local already_locked = redis.call('zscore', 'locks', resource)
        if already_locked then
            return false
        else
            redis.call('zadd', 'locks', time, resource)
            return 'OK'
        end
        """
        acquire_lock = self.db.register_script(script)
        result = acquire_lock(keys=[resource, time.time()])
        if result == 'OK':
            return True
        else:
            return False

    def release_lock(self, resource):
        self.db.zrem('locks', resource)

    @tornado.gen.coroutine
    def register(self):
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
        if not os.path.exists(self.streams_folder):
            os.makedirs(self.streams_folder)
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
            (r'/streams/sync/(.*)', StreamSyncHandler),
            (r'/streams/upload/([^/]+)/(.+)', StreamUploadHandler),
            (r'/streams/download/([^/]+)/(.+)', StreamDownloadHandler),
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
            unlock = self.start_lock(dead_stream)
            self.deactivate_stream(dead_stream)
            unlock()

    @tornado.gen.coroutine
    def deactivate_stream(self, stream_id):
        """ Deactivate a stream. """
        script = """
        local stream_id = KEYS[1]
        if redis.call('sismember', 'active_streams', stream_id) == 0 then
            return false
        else
            local token = redis.call('hget', 'active_stream:'..stream_id, 'auth_token')
            redis.call('del', 'auth_token:'..token..':active_stream')
            local tf = redis.call('hget', 'active_stream:'..stream_id, 'total_frames')
            local us = redis.call('hget', 'active_stream:'..stream_id, 'user')
            local st = redis.call('hget', 'active_stream:'..stream_id, 'start_time')
            local en = redis.call('hget', 'active_stream:'..stream_id, 'engine')
            redis.call('del', 'active_stream:'..stream_id)
            redis.call('zrem', 'heartbeats', stream_id)
            redis.call('srem', 'active_streams', stream_id)
            local target_id = redis.call('hget', 'stream:'..stream_id, 'target')
            local frames = redis.call('hget', 'stream:'..stream_id, 'frames')
            redis.call('zadd', 'target:'..target_id..':queue', frames, stream_id)
            return {tf, us, st, en}
        end
        """
        action = self.db.register_script(script)
        result = action(keys=[stream_id])
        if result:
            frames = result[0]
            user = result[1]
            start_time = result[2]
            engine = result[3]
            end_time = time.time()
            stream_path = os.path.join(self.streams_folder, stream_id)
            buffer_path = os.path.join(stream_path, 'buffer_files')
            if os.path.exists(buffer_path):
                shutil.rmtree(buffer_path)
            if frames:
                body = {
                    'engine': engine,
                    'user': user,
                    'start_time': start_time,
                    'end_time': end_time,
                    'frames': frames,
                    'stream': stream_id
                }
                cursor = self.motor.stats.fragments
                attempts = 0
                while(attempts < 3):
                    try:
                        yield cursor.insert(body)
                        break
                    except:
                        yield tornado.gen.Task(
                            tornado.ioloop.IOLoop.instance().add_timeout,
                            time.time() + 5)
                        attempts += 1
                if attempts == 3:
                    message = 'Could not record fragment: '+json.dumps(body)
                    logging.getLogger('tornado.general').critical(message)

#########################
# Defined here globally #
#########################

tornado.options.define('heartbeat_increment', default=900, type=int)
tornado.options.define('check_heart_frequency_in_ms', default=1000, type=int)


def stop_parent(sig, frame):
    print('parent is waiting for children to terminate')
    for pid in process2.children.keys():
        os.waitpid(pid, 0)
    print('shutting down redis')
    app.db.shutdown()
    sys.exit(0)


def stop_children(sig, frame):
    print('    stopping children', process2.task_id())
    app.db.zrange('locks', 0, -1)  # access db
    server.stop()
    time.sleep(10)
    app.shutdown()
    sys.exit(0)


def start():
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', 'scv.conf')
    configure_options(config_file)
    options = tornado.options.options

    global app
    global server

    app = SCV(name=options.name,
              external_host=options.external_host,
              redis_options=options.redis_options,
              mongo_options=options.mongo_options)
    cert_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', options.ssl_certfile)
    key_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            '..', options.ssl_key)
    ca_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           '..', options.ssl_ca_certs)

    sockets = tornado.netutil.bind_sockets(options.internal_http_port)

    # parent handles signals differently
    signal.signal(signal.SIGINT, stop_parent)
    signal.signal(signal.SIGTERM, stop_parent)

    process2.fork_processes(0)


    #server.bind(options.internal_http_port)
    #server.start(2)
    # children execute the lines below, but not the parent
    signal.signal(signal.SIGINT, stop_children)
    signal.signal(signal.SIGTERM, stop_children)

    server = tornado.httpserver.HTTPServer(app, ssl_options={
        'certfile': cert_path, 'keyfile': key_path, 'ca_certs': ca_path})
    server.add_sockets(sockets)

    app.initialize_motor()

    if tornado.process.task_id() is None:
        tornado.ioloop.IOLoop.instance().run_sync(app.scruffy)
        tornado.ioloop.IOLoop.instance().run_sync(app.check_heartbeats)
        tornado.ioloop.IOLoop.instance().run_sync(app.register)
        frequency = tornado.options.options['check_heart_frequency_in_ms']
        pulse = tornado.ioloop.PeriodicCallback(app.check_heartbeats,
                                                frequency)
        pulse.start()
        tornado.ioloop.PeriodicCallback(app.scruffy, 2000).start()
    tornado.ioloop.IOLoop.instance().start()
