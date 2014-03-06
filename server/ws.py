# Tornado-powered workserver backend.
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

import uuid
import os
import json
import time
import shutil
import hashlib
import socket
import base64
import gzip
import functools

import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httputil
import tornado.httpserver
import tornado.httpclient
import tornado.options
import tornado.process
import tornado.gen

from server.common import BaseServerMixin, is_domain, configure_options
from server.common import authenticate_manager
from server.apollo import Entity, zset, relate

# Capacity

# Suppose each stream returns a frame once every 5 minutes. A single stream
# returns 288 frames per day. The WS is designed to handle about 50 frame
# PUTs per second. In a single day, a WS can handle about 4,320,000 frames.
# This is equal to about 86,400 active streams. Note that 4.3 million frames
# @ 80kb/frame = 328GB worth of data per day. We will fill up 117 TB
# worth of a data a year - so we will run out of disk space way before that.

# [MISC Redis DB]

# ZSET  KEY     'heartbeats'                   | { stream_id : expire_time }

# download_token: issued by CC, set to expire after 10 days
# shared_token: issued by CC, deleted by heartbeat
# heartbeats: each key deleted on restart, and by check_heartbeats

# Expiration mechanism:
# hearbeats is a sorted set. A POST to ws/update extends expire_time in
# heartbeats. A checker callback is passed into ioloop.PeriodicCallback(),
# which checks for expired streams against the current time. Streams that
# expire can be obtained by: redis.zrangebyscore('heartbeat',0,current_time)

# TODO:
# [ ] Stats

# In general, we should try and use GETs/PUTs whenever possible. Idempotency
# is an incredibly useful way of dealing with failures. Suppose a core
# either POSTs (non idempotent), or PUTs (idempotent) a frame to a stream.

# One of two failure scenarios can happen:

#              FAILS
#   Core --Send Request--> Client --Send Reply--> Core

#                                      FAILS
#   Core --Send Request--> Client --Send Reply--> Core

# Note that the Core does NOT know which scenario happened. All it knows
# is that it did not get a reply. In the second scenario, POSTing the same
# frame twice would be bad, since the stream would end up with a duplicate
# stream. However, PUTing the same frame twice (by means of checking the
# md5sum of last frame) would be the same as PUTing it once.

# When the WS dies, we can recreate the entire redis database using data from
# the disk! This implies we don't actually need to save an rdb.


class Stream(Entity):
    prefix = 'stream'
    fields = {'frames': int,            # total number of frames completed
              'status': str,            # 'OK', 'DISABLED'
              'error_count': int,       # number of consecutive errors
              }


class ActiveStream(Entity):
    prefix = 'active_stream'
    fields = {'total_frames': int,  # total frames completed.
              'buffer_frames': int,  # number of frames in buffer.xtc
              'auth_token': str,  # used by core to send requests
              'donor': str,  # the donor assigned ? support lookup?
              'steps': int,  # number of steps completed
              'start_time': float,  # time we started at
              'frame_hash': str,  # md5sum of the received frame
              'buffer_files': {str},  # set of frame files sent
              }


class Target(Entity):
    prefix = 'target'
    fields = {'queue': zset(str),  # queue of inactive streams
              'stream_files': {str},  # set of filenames for the stream
              'target_files': {str},  # set of filenames for the target
              }


class CommandCenter(Entity):
    prefix = 'cc'
    fields = {'ip': str,        # ip of the command center
              'http_port': str  # http port
              }

#Target.add_lookup('owner', injective=False)
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
    def deactivate_stream(self):
        return self.application.deactivate_stream

    def get_current_user(self):
        header_token = self.request.headers['Authorization']
        managers = self.mdb.users.managers
        query = managers.find_one({'token': header_token},
                                  fields=['_id'])
        if query:
            return query['_id']
        else:
            return None


def authenticate_cc(method):
    """ Decorate handlers that require the incoming remote_ip be that of a
    known command center """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.request.remote_ip in self.application.cc_ips and\
                self.request.remote_ip != '127.0.0.1':
            self.write(json.dumps({'error': 'unauthorized ip'}))
            return self.set_status(401)
        else:
            return method(self, *args, **kwargs)

    return wrapper


def authenticate_core(method):
    """ Decorate handlers whose authorization token maps to a specific stream
    identifier.

    If the authorization token is valid, then the stream_id is sent to the
    method as an argument.

    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            token = self.request.headers['Authorization']
        except:
            self.write(json.dumps({'error': 'missing Authorization header'}))
            return self.set_status(401)

        stream_id = ActiveStream.lookup('auth_token', token, self.db)

        if stream_id:
            return method(self, stream_id)
        else:
            self.write(json.dumps({'error': 'bad Authorization header'}))
            return self.set_status(401)

    return wrapper


class AliveHandler(BaseHandler):
    def get(self):
        """
        .. http:get:: /

            Used to check and see if the WS is up or not

            :status 200: OK

        """
        self.set_status(200)


class DeleteTargetHandler(BaseHandler):
    @authenticate_cc
    def put(self, target_id):
        """
        .. http:put:: /targets/delete/:target_id

            Delete ``target_id`` from the workserver. Note, even if the target
            does not exist on this WS the method returns 200 to allow for
            idempotency.

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)

        try:
            target = Target(target_id, self.db)
        except:
            return self.set_status(200)
        stream_ids = target.smembers('streams')
        pipe = self.db.pipeline()
        for stream_id in stream_ids:
            try:
                self.deactivate_stream(stream_id)
            except KeyError:
                pass
            stream_dir = os.path.join(self.application.streams_folder,
                                      stream_id)
            shutil.rmtree(stream_dir)
            # verify=False for performance reasons
            stream = Stream(stream_id, self.db, verify=False)
            stream.delete(pipeline=pipe)
        target_dir = os.path.join(self.application.targets_folder, target_id)
        shutil.rmtree(target_dir)
        target.delete(pipeline=pipe)
        pipe.execute()
        self.set_status(200)


class TargetStreamsHandler(BaseHandler):
    @authenticate_cc
    def get(self, target_id):
        """
        .. http:get:: /targets/streams/:target_id

            Get a list of streams for the target and their status and frames

            **Example reply**:

            .. sourcecode:: javascript

                {
                    "stream_id_1": {
                        "status": "OK",
                        "frames": 253,
                    },
                    "stream_id_2": {
                        "status": "OK",
                        "frames": 1902,
                    }
                }

            :status 200: OK
            :status 400: Bad request

        """
        self.set_status(400)
        target = Target(target_id, self.db)
        body = {}
        for stream_id in target.smembers('streams'):
            stream = Stream(stream_id, self.db)
            body[stream_id] = {}
            body[stream_id]['status'] = stream.hget('status')
            body[stream_id]['frames'] = stream.hget('frames')
        self.set_status(200)
        self.write(json.dumps(body))


class ActivateStreamHandler(BaseHandler):
    @authenticate_cc
    def post(self):
        """
        .. http:post:: /streams/activate

            Activate and return the highest priority stream of a target.

            **Example request**

            .. sourcecode:: javascript

                {
                    "target_id": "some_uuid4",
                    "donor_id": "jesse_v" // optional
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
            ActiveStream.create(stream_id, self.db, fields)
            increment = tornado.options.options['heartbeat_increment']
            self.db.zadd('heartbeats', stream_id, time.time() + increment)

            reply = {}
            reply["token"] = token
            self.set_status(200)
            return self.write(json.dumps(reply))
        else:
            return self.write(json.dumps(dict()))


class PostStreamHandler(BaseHandler):
    @authenticate_cc
    def post(self):
        """
        .. http:post:: /streams

            Add a new stream to WS.

            **Example request**

            .. sourcecode:: javascript

                {
                    "target_id": "target_id",
                    "target_files": {"system.xml.gz.b64": "file1.b64",
                                     "integrator.xml.gz.b64": "file2.b64",
                                     } // required if target_id does not exist

                    "stream_files": {"state.xml.gz.b64": "file3.b64"}
                }

            .. note:: Binaries must be base64 encoded.

            **Example reply**

            .. sourcecode:: javascript

                {
                    "stream_id" : "uuid hash"
                }

            :status 200: OK
            :status 400: Bad request

        """
        # TODO: stream creation needs to be pipelined and atomic
        # Safety-guarantee: dd files before database entries.

        #if not CommandCenter.lookup('ip', self.request.remote_ip, self.db):
        #    return self.set_status(401)
        self.set_status(400)
        content = json.loads(self.request.body.decode())
        target_id = content['target_id']
        stream_files = content['stream_files']
        targets_folder = self.application.targets_folder

        if not Target.exists(target_id, self.db):
            target_files = content['target_files']
            target_dir = os.path.join(targets_folder, target_id)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            target_fields = {
                'stream_files': set(stream_files.keys()),
                'target_files': set(target_files.keys())
            }
            target = Target.create(target_id, self.db, target_fields)
            for filename, binary in target_files.items():
                target_file = os.path.join(target_dir, filename)
                with open(target_file, 'w') as handle:
                    handle.write(binary)
        else:
            target = Target(target_id, self.db)
            if target.smembers('stream_files') != stream_files.keys():
                self.write(json.dumps({'error': 'inconsistent stream files'}))
                return

        stream_id = str(uuid.uuid4())
        stream_dir = os.path.join(self.application.streams_folder, stream_id)
        if not os.path.exists(stream_dir):
            os.makedirs(stream_dir)

        for filename, binary in stream_files.items():
            with open(os.path.join(stream_dir, filename), 'w') as handle:
                handle.write(binary)

        # create using a transaction
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

        response = {'stream_id': stream_id}
        self.set_status(200)
        self.write(json.dumps(response))


class DeleteStreamHandler(BaseHandler):
    @authenticate_cc
    def put(self, stream_id):
        """
        .. http:put:: /streams/delete/:stream_id

            Delete a stream from the workserver

            **Example request**:

            .. sourcecode:: javascript

                {
                    // empty
                }

            :status 200: OK
            :status 400: Bad request

        """
        # delete from database before deleting from disk
        if not Stream.exists(stream_id, self.db):
            return self.set_status(400)
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        target = Target(target_id, self.db)

        pipeline = self.db.pipeline()
        try:
            active_stream = ActiveStream(stream_id, self.db)
            if active_stream:
                active_stream.delete(pipeline=pipeline)
        except KeyError:
            pass
        target.zrem('queue', stream_id, pipeline=pipeline)
        stream.delete(pipeline=pipeline)
        pipeline.execute()

        shutil.rmtree(os.path.join(self.application.streams_folder, stream_id))
        # manual cleanup

        if target.scard('streams') == 0:
            target.delete()
            shutil.rmtree(os.path.join(self.application.targets_folder,
                                       target_id))

        self.set_status(200)


class CoreStartHandler(BaseHandler):
    @authenticate_core
    def get(self, stream_id):
        """
        .. http:get:: /core/start

            Start a stream and retrieve files

            :reqheader Authorization: authorization token given by the cc

            **Example reply**

            .. sourcecode:: javascript

                {
                    "stream_id": "uuid4",
                    "target_id": "uuid4",
                    "stream_files": {"state.xml.gz.b64": "content.b64"},
                    "target_files": {"integrator.xml.gz.b64": "content.b64",
                                     "system.xml.gz.b64": "content.b64"
                                     }
                }

            :status 200: OK
            :status 400: Bad request

        """
        # We need to be extremely careful about checkpoints and frames, as
        # it is important we avoid writing duplicate frames on the first
        # step for the core. We use the follow scheme:

        #               (0-10]                      (10-20]
        #             frameset_10                 frameset_20
        #       ------------------------------------------------------------
        #       |c       core 1      |c|              core 2         |c|
        #       ---                  --|--                           --|--
        # frame x |1 2 3 4 5 6 7 8 9 10| |11 12 13 14 15 16 17 18 19 20| |21
        #         ---------------------| ------------------------------- ---

        # When a core fetches a state.xml, it makes sure to NOT write the
        # first frame (equivalent to the frame of fetched state.xml file).
        self.set_status(400)
        stream = Stream(stream_id, self.db)
        target_id = stream.hget('target')
        target = Target(target_id, self.db)
        # a core should NEVER be able to get a non OK stream
        assert stream.hget('status') == 'OK'

        reply = dict()

        reply['stream_files'] = dict()
        for filename in target.smembers('stream_files'):
            file_path = os.path.join(self.application.streams_folder,
                                     stream_id, filename)
            with open(file_path, 'r') as handle:
                reply['stream_files'][filename] = handle.read()

        reply['target_files'] = dict()
        for filename in target.smembers('target_files'):
            file_path = os.path.join(self.application.targets_folder,
                                     target_id, filename)
            with open(file_path, 'r') as handle:
                reply['target_files'][filename] = handle.read()

        reply['stream_id'] = stream_id
        reply['target_id'] = target_id

        self.set_status(200)
        return self.write(json.dumps(reply))


class CoreFrameHandler(BaseHandler):
    @authenticate_core
    def put(self, stream_id):
        """
        ..  http:put:: /core/frame

            Append a frame to the stream's buffer.

            If the core posts to this method, then the WS assumes that the
            frame is valid. The data received is stored in a buffer until a
            checkpoint is received. It is assumed that files given here are
            binary appendable. Files ending in .b64 or .gz are decoded
            automatically.

            :reqheader Authorization: authorization token given by the cc

            **Example request**

            .. sourcecode:: javascript

                {
                    "files" : {
                        "frames.xtc.b64": "file.b64",
                        "log.txt.gz.b64": "file.gz.b64"
                    }
                    "frames": 25  // optional, number of frames in the files
                }

            :status 200: OK
            :status 400: Bad request

        If the filename ends in b64, it is b64 decoded. If the next suffix ends
        in gz, it is gunzipped. Afterwards, the written to disk with the name
        buffer_[filename], with the b64/gz suffixes stripped.

        """
        # There are four intervals:

        # fwi = frame_write_interval (PG Controlled)
        # fsi = frame_send_interval (Core Controlled)
        # cwi = checkpoint_write_interval (Core Controlled)
        # csi = checkpoint_send_interval (Donor Controlled)

        # Where: fwi < fsi = cwi < csi

        # When a set of frames is sent, the core is guaranteed to write a
        # corresponding checkpoint, so that the next checkpoint received is
        # guaranteed to correspond to the head of the buffered files.

        # OpenMM:

        # fwi = fsi = cwi = 50000
        # sci = 2x per day

        # Terachem:

        # fwi = 2
        # fsi = cwf = 100
        # sci = 2x per day
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
                return self.write(json.dumps({'error': 'frames < 1'}))
        else:
            frame_count = 1
        files = content['files']
        streams_folder = self.application.streams_folder

        # empty the set
        active_stream.sremall('buffer_files')
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
            buffer_filename = os.path.join(streams_folder, stream_id,
                                           'buffer_'+filename)
            with open(buffer_filename, 'ab') as buffer_handle:
                buffer_handle.write(filedata)
            active_stream.sadd('buffer_files', filename)
        active_stream.hincrby('buffer_frames', frame_count)

        return self.set_status(200)


class CoreCheckpointHandler(BaseHandler):
    @authenticate_core
    def put(self, stream_id):
        """
        .. http:put:: /core/checkpoint

            Add a checkpoint and renames buffered files into a valid filenames
            prefixed with the corresponding frame count. It is assumed that the
            checkpoint given corresponds to the last frame of the buffered
            frames.

            :reqheader Authorization: authorization token given by the cc

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
        # Naming scheme:

        # active_stream.hset('buffer_frames', 0)

        # total_frames = stream.hget('frames') +
        #                active_stream.hget('buffer_frames')
        #              = 9

        # ACID Compliance:

        # 1) rename state.xml.gz.b64 -> chkpt_5_state.xml.gz.b64
        # 2) rename buffered files: buffer_frames.xtc -> 9_frames.xtc
        # 3) write checkpoint as state.xml.gz.b64
        # 4) delete chkpt_5_state.xml.gz.b64

        # If the WS crashes, and a file chkpt_* is present, then that means
        # this process has been interrupted and we need to recover.

        # On a restart, we can revert to a safe state by doing:

        # 1) Identify the checkpoint files and their frame number:
        #    eg. a file called chkpt_5_something is a checkpoint file, with a
        #        frame number of 5, and a filename of something
        # 2) Identify the frame files and their frame number:
        #    eg. a file called 5_something is a frame file, with a frame number
        #        of 5, and a filename of something
        # 3) Remove all frame files with a frame number than that of the
        #    checkpoint, as well as all buffer files
        # 4) Rename checkpoint files to their proper names.

        self.set_status(400)
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
        buffers_folder = os.path.join(streams_folder, stream_id)

        # 1) rename old checkpoint file
        for filename, bytes in content['files'].items():
            src = os.path.join(buffers_folder, filename)
            dst = os.path.join(buffers_folder, 'chkpt_'+str(stream_frames)+'_'+
                                               filename)
            os.rename(src, dst)

        # 2) rename buffered files
        for filename in active_stream.smembers('buffer_files'):
            dst = os.path.join(buffers_folder, str(total_frames)+'_'+filename)
            src = os.path.join(buffers_folder, 'buffer_'+filename)
            os.rename(src, dst)

        # 3) write checkpoint
        for filename, bytes in content['files'].items():
            checkpoint_bytes = content['files'][filename].encode()
            checkpoint_path = os.path.join(buffers_folder, filename)
            with open(checkpoint_path, 'wb') as handle:
                handle.write(checkpoint_bytes)

        # 4) delete old checkpoint safely
        for filename, bytes in content['files'].items():
            dst = os.path.join(buffers_folder, 'chkpt_'+str(stream_frames)+'_'+
                                               filename)
            os.remove(dst)

        stream.hincrby('frames', buffer_frames)
        active_stream.hincrby('total_frames', buffer_frames)
        active_stream.hset('buffer_frames', 0)
        self.set_status(200)


class CoreStopHandler(BaseHandler):
    @authenticate_core
    def put(self, stream_id):
        """
        ..  http:put:: /core/stop

            Stop a stream

            :reqheader Authorization: authorization token given by the cc

            **Example Request**

            .. sourcecode:: javascript

                {
                    "error": "error message b64",  // optional
                    "debug_files": {"file1_name": "file1_bin_b64",
                                    "file2_name": "file2_bin_b64"
                                    } // optional
                }

            .. note:: ``error`` is b64 encoded

            :status 200: OK
            :status 400: Bad request

        """
        # TODO: add field denoting if stream should be finished
        stream = Stream(stream_id, self.db)
        content = json.loads(self.request.body.decode())
        if 'error' in content:
            stream.hincrby('error_count', 1)
            message = base64.b64decode(content['error']).decode()
            log_path = os.path.join(self.application.streams_folder,
                                    stream_id, 'log.txt')
            with open(log_path, 'a') as handle:
                handle.write(time.strftime("%c")+' | '+message)

        self.set_status(200)
        self.deactivate_stream(stream_id)


class ActiveStreamsHandler(BaseHandler):
    def get(self):
        """ Display statistics about active streams on the workserver. The list
        of streams are not sorted, so the client must sort them.

        Reply:

        {
            target_id: {
                    stream_id_1: {
                        donor_id: None,
                        start_time: time,
                        total_frames: frames
                    }
                    ...
            }
            ...
        }

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
                total_frames = active_stream.hget('total_frames')
                buffer_frames = active_stream.hget('buffer_frames')
                reply[target][stream_id]['donor'] = donor
                reply[target][stream_id]['start_time'] = start_time
                reply[target][stream_id]['total_frames'] = total_frames
                reply[target][stream_id]['buffer_frames'] = buffer_frames
        self.write(json.dumps(reply))


class DownloadHandler(BaseHandler):
    @authenticate_manager
    @tornado.gen.coroutine
    def get(self, stream_id, filename):
        """
        .. http:get:: /streams/:stream_id/:filename

            Download file ``filename`` from ``stream_id``. This function
            concatenates the list of frames on the fly by reading the files and
            writing chunks.

            :resheader Content-Type: application/octet-stream
            :resheader Content-Disposition: attachment; filename=frames.xtc

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

        try:
            stream = Stream(stream_id, self.db)
            target_id = stream.hget('target')

            query = self.mdb.data.targets.find_one({'_id': target_id},
                                                   fields=['owner'])
            # query should never be none
            if query['owner'] != self.get_current_user():
                return

            if stream.hget('frames') > 0:
                files = [f for f in os.listdir(stream_dir)
                         if (filename in f and 'buffer_' not in f)]
                files = sorted(files, key=lambda k: int(k.split('_')[0]))
                buf_size = 4096
                self.set_header('Content-Type', 'application/octet-stream')
                self.set_header('Content-Disposition',
                                'attachment; filename='+filename)
                self.set_status(200)
                for sorted_file in files:
                    filepath = os.path.join(stream_dir, sorted_file)
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
        except Exception as e:
            print('DownloadHandler Exception', str(e))
            return self.write(json.dumps({'error': str(e)}))


class CoreHeartbeatHandler(BaseHandler):
    @authenticate_core
    def post(self, stream_id):
        """
        .. http:post:: /core/heartbeat

            Cores POST to this handler to notify the WS that it is still
            alive.

            :reqheader Authorization: authorization token given by the cc

            **Example request**

            .. sourcecode:: javascript

                {
                    // empty
                }

            **Example reply**:

            .. sourcecode:: javascript

                {
                    // empty
                }

            :status 200: OK
            :status 400: Bad request

        """
        increment = tornado.options.options['heartbeat_increment']
        self.db.zadd('heartbeats', stream_id, time.time()+increment)
        self.set_status(200)


class WorkServer(BaseServerMixin, tornado.web.Application):
    def _cleanup(self):
        # clear active streams (and clear buffer)
        active_streams = ActiveStream.members(self.db)
        if active_streams:
            for stream in active_streams:
                self.deactivate_stream(stream)
        ccs = CommandCenter.members(self.db)
        if ccs:
            for cc_id in ccs:
                CommandCenter.delete(cc_id, self.db)
        self.db.delete('heartbeats')

        # inform the CC gracefully that the WS is dying (ie.expire everything)

    def __init__(self, ws_name, external_options, redis_options,
                 mongo_options=None, command_centers=None,
                 targets_folder='targets', streams_folder='streams'):
        print('Starting up Work Server:', ws_name)
        self.base_init(ws_name, redis_options, mongo_options)

        self.targets_folder = targets_folder
        self.streams_folder = streams_folder
        self.command_centers = command_centers
        self.cc_ips = set()

        # Notify the command centers that this workserver is online
        if command_centers:
            for cc_name, properties in command_centers.items():
                headers = {
                    'Authorization': properties['pass']
                }
                body = {
                    'name': ws_name,
                    'url': external_options['external_url'],
                    'http_port': external_options['external_http_port'],
                }
                client = tornado.httpclient.HTTPClient()
                url = properties['url']
                hostname = url.split(':')[0]
                self.cc_ips.add(socket.gethostbyname(hostname))
                uri = 'https://'+url+'/ws/register'
                try:
                    rep = client.fetch(uri, method='PUT', connect_timeout=2,
                                       body=json.dumps(body), headers=headers,
                                       validate_cert=is_domain(url))
                    if rep.code != 200:
                        print('Warning: not connect to CC '+cc_name)
                except Exception as e:
                    print(e.code)
                    print('Warning: not connect to CC '+cc_name)

        super(WorkServer, self).__init__([
            (r'/', AliveHandler),
            (r'/active_streams', ActiveStreamsHandler),
            (r'/streams/activate', ActivateStreamHandler),
            (r'/streams', PostStreamHandler),
            (r'/streams/delete/(.*)', DeleteStreamHandler),
            (r'/streams/(.*)/(.*)', DownloadHandler),
            (r'/targets/streams/(.*)', TargetStreamsHandler),
            (r'/targets/delete/(.*)', DeleteTargetHandler),
            (r'/core/start', CoreStartHandler),
            (r'/core/frame', CoreFrameHandler),
            (r'/core/checkpoint', CoreCheckpointHandler),
            (r'/core/stop', CoreStopHandler),
            (r'/core/heartbeat', CoreHeartbeatHandler)
        ])

    def notify_cc_shutdown(self):
        print('notifying CCs of shutdown...')
        if tornado.process.task_id() == 0:
            client = tornado.httpclient.HTTPClient()
            for cc_name, properties in self.command_centers.items():
                url = properties['url']
                uri = 'https://'+url+'/ws/disconnect'
                body = {
                    'name': self.name
                }
                headers = {
                    'Authorization': properties['pass']
                }
                try:
                    client.fetch(uri, method='PUT', connect_timeout=2,
                                 body=json.dumps(body), headers=headers,
                                 validate_cert=is_domain(url))
                except tornado.httpclient.HTTPError:
                    print('Failed to notify '+cc_name+' that WS is down')

    def shutdown(self, *args, **kwargs):
        self.notify_cc_shutdown()
        BaseServerMixin.shutdown(self, *args, **kwargs)

    def initialize_pulse(self):
        # check for heartbeats only on the 0th process.
        if tornado.process.task_id() == 0:
            frequency = tornado.options.options['pulse_frequency_in_ms']
            self.pulse = tornado.ioloop.PeriodicCallback(self.check_heartbeats,
                                                         frequency)
            self.pulse.start()

    def check_heartbeats(self):
        for dead_stream in self.db.zrangebyscore('heartbeats', 0, time.time()):
            self.deactivate_stream(dead_stream)

    def deactivate_stream(self, stream_id):
        active_stream = ActiveStream(stream_id, self.db)

        self.db.zrem('heartbeats', stream_id)
        buffer_files = active_stream.smembers('buffer_files')
        for fname in buffer_files:
            fname = 'buffer_'+fname
            buffer_path = os.path.join(self.streams_folder, stream_id, fname)
            if os.path.exists(buffer_path):
                os.remove(buffer_path)

        active_stream.delete()
        # push this stream back into queue
        stream = Stream(stream_id, self.db)
        frames_completed = stream.hget('frames')
        target = Target(stream.hget('target'), self.db)
        # TODO: to a check to make sure the stream's status is OK. Check the
        # error count, if it's too high, then the stream is stopped
        target.zadd('queue', stream_id, frames_completed)

#########################
# Defined here globally #
#########################

tornado.options.define('heartbeat_increment', default=900, type=int)
tornado.options.define('pulse_frequency_in_ms', default=3000, type=int)


def start():
    extra_options = {
        'command_centers': dict,
        'external_options': dict
    }
    conf_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', 'ws.conf')
    configure_options(extra_options, conf_path)
    options = tornado.options.options

    ws_instance = WorkServer(ws_name=options.name,
                             external_options=options.external_options,
                             command_centers=options.command_centers,
                             redis_options=options.redis_options,
                             mongo_options=options.mongo_options)

    cert_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             '..', options.ssl_certfile)
    key_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            '..', options.ssl_key)
    ca_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           '..', options.ssl_ca_certs)

    ws_server = tornado.httpserver.HTTPServer(ws_instance, ssl_options={
        'certfile': cert_path, 'keyfile': key_path, 'ca_certs': ca_path})

    ws_server.bind(options.internal_http_port)
    ws_server.start(0)
    ws_instance.initialize_pulse()
    tornado.ioloop.IOLoop.instance().start()
