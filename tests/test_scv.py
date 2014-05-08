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

import tornado.testing
import server.scv as scv

import unittest
import os
import shutil
import sys
import uuid
import json
import time
import base64
import random
import hashlib
import time
import pymongo

from os.path import isfile


class TestSCV(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        redis_options = {'port': 3828, 'logfile': os.devnull}
        self.scv = scv.SCV(name='test_scv',
                           external_host='127.0.0.1',
                           mongo_options=self.mongo_options,
                           redis_options=redis_options)
        self.scv.initialize_motor()
        return self.scv

    def setUp(self):
        self.mongo_options = {'host': 'localhost', 'port': 27017}
        self.mdb = pymongo.MongoClient('localhost', 27017)
        token = str(uuid.uuid4())
        test_manager = "test_ws@gmail.com"
        db_body = {'_id': test_manager,
                   'token': token,
                   'role': 'manager',
                   'weight': 1,
                   }
        managers = self.mdb.users.managers
        managers.insert(db_body)
        self.auth_token = token
        self.test_manager = test_manager
        super(TestSCV, self).setUp()

    def tearDown(self):
        super(TestSCV, self).tearDown()
        self.scv.db.flushdb()
        self.scv.shutdown_redis()
        for db_name in self.mdb.database_names():
            self.mdb.drop_database(db_name)
        shutil.rmtree(self.scv.data_folder)

    def _post_stream(self, target_id=None):
        if target_id is None:
            target_id = str(uuid.uuid4())
            targets = self.mdb.data.targets
            body = {'_id': target_id,
                    'owner': self.test_manager,
                    'options': {'steps_per_frame': 50000}}
            targets.insert(body)
        files = {}
        for i in range(4):
            filename = hashlib.md5(os.urandom(1024)).hexdigest()
            files[filename] = hashlib.md5(os.urandom(1024)).hexdigest()
        body = {'target_id': target_id,
                'files': files
                }
        headers = {'Authorization': self.auth_token}
        reply = self.fetch('/streams', method='POST', body=json.dumps(body),
                           headers=headers)
        self.assertEqual(reply.code, 200)
        stream_id = json.loads(reply.body.decode())['stream_id']
        result = {}
        result['stream_id'] = stream_id
        result['target_id'] = target_id
        result['files'] = files
        return result

    def _delete_stream(self, stream_id):
        headers = {'Authorization': self.auth_token}
        response = self.fetch('/streams/delete/'+stream_id,
                              method='PUT',
                              body='',
                              headers=headers)
        self.assertEqual(response.code, 200)

    def _activate_stream(self, target_id):
        body = {'target_id': target_id}
        headers = {'Authorization': self.scv.password}
        reply = self.fetch('/streams/activate', method='POST',
                           body=json.dumps(body), headers=headers)

        self.assertEqual(reply.code, 200)
        reply_data = json.loads(reply.body.decode())
        token = reply_data['token']
        stream_id = scv.ActiveStream.lookup('auth_token', token, self.scv.db)
        return stream_id, token

    def _post_and_activate_stream(self, target_id=None):
        result = self._post_stream(target_id)
        stream_id, token = self._activate_stream(result['target_id'])
        result['token'] = token
        return result

    def _get_streams(self, target_id=None, expected_code=200):
        headers = {'Authorization': self.auth_token}
        if target_id:
            reply = self.fetch('/targets/streams/'+target_id,
                               headers=headers)
            self.assertEqual(reply.code, expected_code)
            return json.loads(reply.body.decode())
        else:
            raise ValueError("not implemented!")

    def _core_start(self, target_id=None):
        result = self._post_and_activate_stream(target_id)
        files = result['files']
        target_id = result['target_id']
        stream_id = result['stream_id']
        token = result['token']
        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        content = json.loads(response.body.decode())
        self.assertEqual(content['stream_id'], stream_id)
        self.assertEqual(content['target_id'], target_id)
        self.assertEqual(content['files'], files)
        result['core_files'] = content['files']
        return result

    def _add_frames(self, core_token):
        headers = {'Authorization': core_token}
        frame_bin = os.urandom(1024)
        body = {
            'files': {'frames.xtc.b64':
                      base64.b64encode(frame_bin).decode()},
            }
        response = self.fetch('/core/frame', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)
        return frame_bin

    def test_post_stream(self):
        result = self._post_stream()
        stream_id = result['stream_id']
        target_id = result['target_id']
        files = result['files']
        headers = {'Authorization': self.auth_token}

        for filename, filebin in files.items():
            reply = self.fetch('/streams/download/'+stream_id+'/'+
                               filename, headers=headers)
            self.assertEqual(filebin.encode(), reply.body)

        target = scv.Target(target_id, self.scv.db)
        self.assertFalse(target.zscore('queue', stream_id) is None)
        self.assertTrue(scv.Stream.exists(stream_id, self.scv.db))
        self.assertTrue(scv.Target.exists(target_id, self.scv.db))
        self.assertEqual(scv.Stream(stream_id, self.scv.db).hget('target'),
                         target_id)
        self.assertSetEqual({stream_id},
            scv.Target(target_id, self.scv.db).smembers('streams'))
        expected = {'streams': [stream_id]}
        self.assertEqual(self._get_streams(target_id), expected)
        cursor = self.mdb.data.targets
        result = cursor.find_one({'_id': target_id}, {'shards': 1})
        self.assertEqual(result['shards'], [self.scv.name])

    def test_delete_stream(self):
        result = self._post_stream()
        stream_id = result['stream_id']
        target_id = result['target_id']
        target = scv.Target(target_id, self.scv.db)
        cursor = self.mdb.data.targets
        result = cursor.find_one({'_id': target_id}, {'shards': 1})
        self.assertEqual(result['shards'], [self.scv.name])
        self._delete_stream(stream_id)
        self._get_streams(target_id, expected_code=400)
        self.assertEqual(target.zscore('queue', stream_id), None)
        self.assertEqual(self.scv.db.keys('*'), ['password'])
        result = cursor.find_one({'_id': target_id}, {'shards': 1})
        self.assertEqual(result['shards'], [])
        stream_path = os.path.join(self.scv.streams_folder, stream_id)
        self.assertFalse(os.path.exists(stream_path))

    def test_sharding(self):
        result = self._post_stream()
        stream1 = result['stream_id']
        target_id = result['target_id']
        result = self._post_stream(target_id)
        stream2 = result['stream_id']
        cursor = self.mdb.data.targets
        result = cursor.find_one({'_id': target_id}, {'shards': 1})
        self.assertEqual(result['shards'], [self.scv.name])
        self._delete_stream(stream1)
        result = cursor.find_one({'_id': target_id}, {'shards': 1})
        self.assertEqual(result['shards'], [self.scv.name])
        self._delete_stream(stream2)
        result = cursor.find_one({'_id': target_id}, {'shards': 1})
        self.assertEqual(result['shards'], [])

    def test_activate_stream(self):
        result = self._post_stream()
        target_id = result['target_id']
        stream1 = result['stream_id']
        stream2, token = self._activate_stream(target_id)
        self.assertEqual(stream1, stream2)
        self.assertTrue(scv.ActiveStream(stream1, self.scv.db))
        increment = tornado.options.options['heartbeat_increment']
        self.assertAlmostEqual(self.scv.db.zscore('heartbeats', stream1),
                               time.time()+increment, 1)
        self.assertEqual(scv.ActiveStream.lookup('auth_token',
                         token, self.scv.db), stream1)

    def test_get_active_streams(self):
        tornado.options.options.heartbeat_increment = 10
        result = self._post_stream()
        target_id = result['target_id']
        stream1 = result['stream_id']
        stream2, token = self._activate_stream(target_id)
        self.assertEqual(stream1, stream2)
        reply = self.fetch('/active_streams', method='GET')
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())
        for tid in content:
            self.assertEqual(tid, target_id)
            for sid in content[tid]:
                self.assertEqual(sid, stream1)
        new_stream1 = self._post_stream(target_id)['stream_id']
        new_stream2, token = self._activate_stream(target_id)
        self.assertEqual(new_stream1, new_stream2)
        reply = self.fetch('/active_streams', method='GET')
        content = json.loads(reply.body.decode())
        targets = set()
        streams = set()
        for tid in content:
            targets.add(tid)
            for sid in content[tid]:
                streams.add(sid)
        self.assertEqual(targets, {target_id})
        self.assertEqual(streams, {stream1, new_stream1})

    def test_delete_target(self):
        total_streams = set()
        result = self._post_stream()
        target_id = result['target_id']
        headers = {'Authorization': self.auth_token}
        total_streams.add(result['stream_id'])
        for i in range(49):
            total_streams.add(self._post_stream(target_id)['stream_id'])
        active_streams = set()
        for i in range(10):
            stream_id, token = self._activate_stream(target_id)
            active_streams.add(stream_id)
        self.assertEqual(scv.Target.members(self.scv.db), {target_id})
        self.assertEqual(scv.Stream.members(self.scv.db), total_streams)
        self.assertEqual(scv.ActiveStream.members(self.scv.db), active_streams)
        for stream_id in total_streams:
            stream_dir = os.path.join(self.scv.streams_folder, stream_id)
            self.assertTrue(os.path.exists(stream_dir))
        for stream_id in total_streams:
            reply = self.fetch('/streams/delete/'+stream_id,
                              method='PUT', body='', headers=headers)
            self.assertEqual(reply.code, 200)
        self.assertEqual(scv.Target.members(self.scv.db), set())
        self.assertEqual(scv.Stream.members(self.scv.db), set())
        self.assertEqual(scv.ActiveStream.members(self.scv.db), set())
        for stream_id in total_streams:
            stream_dir = os.path.join(self.scv.streams_folder, stream_id)
            self.assertFalse(os.path.exists(stream_dir))
        self.assertEqual(self.scv.db.keys('*'), ['password'])
        cursor = self.mdb.data.targets
        result = cursor.find_one({'_id': target_id}, {'shards': 1})
        self.assertEqual(result['shards'], [])

    def test_stream_replace(self):
        result = self._post_and_activate_stream()
        stream_id = result['stream_id']
        files = result['files']
        m_headers = {'Authorization': self.auth_token}
        new_bin = str(uuid.uuid4())
        filename = random.choice(list(files.keys()))
        body = json.dumps({
            "files": {filename: new_bin}
            })
        # make sure stream must be stopped first
        response = self.fetch('/streams/replace/'+stream_id, body=body,
                              headers=m_headers, method='PUT')
        self.assertEqual(response.code, 400)
        response = self.fetch('/streams/stop/'+stream_id, body='',
                              headers=m_headers, method='PUT')
        self.assertEqual(response.code, 200)
        response = self.fetch('/streams/download/'+stream_id+'/'+filename,
                              headers=m_headers)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body.decode(), files[filename])
        response = self.fetch('/streams/replace/'+stream_id, body=body,
                              headers=m_headers, method='PUT')
        self.assertEqual(response.code, 200)
        response = self.fetch('/streams/download/'+stream_id+'/'+filename,
                              headers=m_headers)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body.decode(), new_bin)

    def test_core_start(self):
        self._core_start()

    def test_core_frame(self):
        result = self._post_and_activate_stream()
        stream_id = result['stream_id']
        target_id = result['target_id']
        files = result['files']
        token = result['token']
        headers = {'Authorization': token}
        self._core_start(target_id)

        frame_buffer = bytes()
        n_frames = 25
        active_stream = scv.ActiveStream(stream_id, self.scv.db)
        stream = scv.Stream(stream_id, self.scv.db)

        # PUT 20 frames
        for count in range(n_frames):
            frame_buffer += self._add_frames(token)
        self.assertEqual(active_stream.hget('buffer_frames'), n_frames)

        streams_dir = self.scv.streams_folder
        buffer_folder = os.path.join(streams_dir, stream_id, 'buffer_files')
        buffer_path = os.path.join(buffer_folder, 'frames.xtc')
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # PUT a checkpoint
        replacement_filename = random.choice(list(files.keys()))
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        body = {'files': {replacement_filename: checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)
        self.assertEqual(active_stream.hget('buffer_frames'), 0)
        self.assertEqual(stream.hget('frames'), n_frames)
        self.assertFalse(os.path.exists(buffer_folder))
        checkpoint_path = os.path.join(streams_dir, stream_id, str(n_frames),
                                       'checkpoint_files', replacement_filename)
        self.assertEqual(checkpoint_bin, open(checkpoint_path, 'rb').read())
        frames_path = os.path.join(streams_dir, stream_id, str(n_frames),
                                   'frames.xtc')
        self.assertEqual(frame_buffer, open(frames_path, 'rb').read())

        # PUT a few more frames
        frame_buffer = bytes()
        more_frames = 5
        for count in range(more_frames):
            frame_buffer += self._add_frames(token)
        self.assertEqual(active_stream.hget('buffer_frames'), more_frames)
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # PUT another checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        body = {'files': {replacement_filename: checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertFalse(os.path.exists(buffer_path))
        self.assertEqual(stream.hget('frames'), n_frames+more_frames)
        self.assertEqual(active_stream.hget('buffer_frames'), 0)
        initial_state_path = os.path.join(streams_dir, stream_id, 'files',
                                          replacement_filename)
        self.assertTrue(isfile(initial_state_path))
        checkpoint_path = os.path.join(streams_dir, stream_id,
                                       str(n_frames+more_frames),
                                       'checkpoint_files', replacement_filename)
        self.assertEqual(checkpoint_bin, open(checkpoint_path, 'rb').read())

        # test idempotency of put checkpoint
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertFalse(os.path.exists(buffer_path))
        self.assertEqual(stream.hget('frames'), n_frames+more_frames)
        self.assertEqual(active_stream.hget('buffer_frames'), 0)
        initial_state_path = os.path.join(streams_dir, stream_id, 'files',
                                          replacement_filename)
        self.assertTrue(isfile(initial_state_path))
        self.assertEqual(checkpoint_bin, open(checkpoint_path, 'rb').read())

        # test idempotency of put frame
        frame_buffer = self._add_frames(token)
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())
        response = self.fetch('/core/frame', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        self.scv.maintain_integrity()

    def test_core_frame_variadic(self):
        result = self._post_and_activate_stream()
        stream_id = result['stream_id']
        target_id = result['target_id']
        files = result['files']
        token = result['token']
        headers = {'Authorization': token}
        self._core_start(target_id)

        active_stream = scv.ActiveStream(stream_id, self.scv.db)
        stream = scv.Stream(stream_id, self.scv.db)

        # PUT 20 frames
        frame_buffer = bytes()
        n_puts = 25
        n_counts = []
        for count in range(n_puts):
            frame_bin = os.urandom(1024)
            frame_buffer += frame_bin
            count = random.randrange(1, 150)
            n_counts.append(count)
            body = {
                'frames': count,
                'files': {'frames.xtc.b64':
                          base64.b64encode(frame_bin).decode()}
                }
            response = self.fetch('/core/frame', headers=headers,
                                  body=json.dumps(body), method='PUT')
            self.assertEqual(response.code, 200)

        self.assertEqual(active_stream.hget('buffer_frames'), sum(n_counts))
        streams_dir = self.scv.streams_folder
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer_files',
                                   'frames.xtc')
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # add a checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        replacement_filename = random.choice(list(files.keys()))
        body = {'files': {replacement_filename: checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)
        self.assertEqual(active_stream.hget('buffer_frames'), 0)
        self.assertEqual(stream.hget('frames'), sum(n_counts))
        self.assertFalse(os.path.exists(buffer_path))

        # test downloading the frames again
        manager_headers = {'Authorization': self.auth_token}
        response = self.fetch('/streams/download/'+stream_id+'/frames.xtc',
                              headers=manager_headers)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, frame_buffer)

        self.scv.maintain_integrity()

    def test_core_stop(self):
        result = self._post_and_activate_stream()
        target_id = result['target_id']
        stream_id = result['stream_id']
        token = result['token']
        headers = {'Authorization': token}
        frame_buffer = bytes()
        n_frames = 25
        stream = scv.Stream(stream_id, self.scv.db)
        target = scv.Target(target_id, self.scv.db)
        for count in range(n_frames):
            frame_buffer += self._add_frames(token)
        self.assertTrue(scv.ActiveStream.exists(stream_id, self.scv.db))
        self.assertTrue(target.zscore('queue', stream_id) is None)
        response = self.fetch('/core/stop', headers=headers, method='PUT',
                              body='{}')
        self.assertEqual(response.code, 200)
        self.assertEqual(stream.hget('error_count'), 0)
        self.assertFalse(scv.ActiveStream.exists(stream_id, self.scv.db))
        self.assertFalse(target.zscore('queue', stream_id) is None)
        buffer_path = os.path.join(self.scv.streams_folder,
                                   stream_id, 'buffer_frames.xtc')
        self.assertFalse(os.path.exists(buffer_path))
        stream_id, token = self._activate_stream(target_id)

    def test_core_stop_error(self):
        result = self._post_and_activate_stream()
        target_id = result['target_id']
        stream_id = result['stream_id']
        token = result['token']
        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        frame_buffer = bytes()
        n_frames = 25

        stream = scv.Stream(stream_id, self.scv.db)
        target = scv.Target(target_id, self.scv.db)

        for count in range(n_frames):
            frame_buffer += self._add_frames(token)

        self.assertTrue(scv.ActiveStream.exists(stream_id, self.scv.db))
        self.assertTrue(target.zscore('queue', stream_id) is None)

        body = {'error': base64.b64encode(b'NaN').decode()}
        response = self.fetch('/core/stop', headers=headers, method='PUT',
                              body=json.dumps(body))
        self.assertEqual(response.code, 200)
        self.assertEqual(stream.hget('error_count'), 1)
        error_path = os.path.join(self.scv.streams_folder,
                                  stream_id, 'error_log.txt')
        self.assertTrue(b'NaN' in open(error_path, 'rb').read())
        self.assertFalse(scv.ActiveStream.exists(stream_id, self.scv.db))
        self.assertFalse(target.zscore('queue', stream_id) is None)
        buffer_path = os.path.join(self.scv.streams_folder,
                                   stream_id, 'buffer_frames.xtc')
        self.assertFalse(os.path.exists(buffer_path))

    def test_download_stream(self):
        result = self._post_and_activate_stream()
        stream_id = result['stream_id']
        target_id = result['target_id']
        files = result['files']
        token = result['token']
        headers = {'Authorization': token}
        manager_headers = {'Authorization': self.auth_token}
        self._core_start(target_id)

        frame_buffer = bytes()
        n_frames = 25
        active_stream = scv.ActiveStream(stream_id, self.scv.db)

        random_file = random.choice(list(files.keys()))
        # download a non-frame file that has not been replaced yet
        response = self.fetch('/streams/download/'+stream_id+'/'+random_file,
                              headers=manager_headers)

        self.assertEqual(response.code, 200)
        self.assertEqual(response.body.decode(), files[random_file])

        # PUT 25 frames
        for count in range(n_frames):
            frame_buffer += self._add_frames(token)
        self.assertEqual(active_stream.hget('buffer_frames'), n_frames)
        streams_dir = self.scv.streams_folder
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer_files',
                                   'frames.xtc')
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # PUT a checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        replacement_filename = random.choice(list(files.keys()))

        body = {'files': {replacement_filename: checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)

        # Get info about the stream
        response = self.fetch('/streams/info/'+stream_id)
        content = json.loads(response.body.decode())
        self.assertEqual(n_frames, content['frames'])
        self.assertEqual('OK', content['status'])
        self.assertEqual(0, content['error_count'])
        self.assertEqual(True, content['active'])

        # download the frames
        response = self.fetch('/streams/download/'+stream_id+'/frames.xtc',
                              headers=manager_headers)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, frame_buffer)

        old_buffer = frame_buffer
        # PUT 25 more frames
        for count in range(n_frames):
            frame_buffer += self._add_frames(token)
        self.assertEqual(active_stream.hget('buffer_frames'), n_frames)
        streams_dir = self.scv.streams_folder
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer_frames.xtc')

        # download the frames
        response = self.fetch('/streams/download/'+stream_id+'/frames.xtc',
                              headers=manager_headers)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, old_buffer)

        # PUT a checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        body = {'files': {replacement_filename: checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)

        # download the frames
        response = self.fetch('/streams/download/'+stream_id+'/frames.xtc',
                              headers=manager_headers)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, frame_buffer)

        # # download a non-frame file that has been replaced by checkpoint
        # response = self.fetch('/streams/download/'+stream_id+'/'+
        #                       replacement_filename, headers=manager_headers)
        # self.assertEqual(response.code, 200)
        # self.assertEqual(response.body, checkpoint_bin)

        # Get info about the stream
        response = self.fetch('/streams/info/'+stream_id)
        content = json.loads(response.body.decode())
        self.assertEqual(n_frames*2, content['frames'])
        self.assertEqual('OK', content['status'])
        self.assertEqual(0, content['error_count'])

        response = self.fetch('/core/stop', headers=headers, method='PUT',
                              body='{}')
        self.assertEqual(response.code, 200)

        response = self.fetch('/streams/info/'+stream_id)
        content = json.loads(response.body.decode())
        self.assertEqual(n_frames*2, content['frames'])
        self.assertEqual('OK', content['status'])
        self.assertEqual(0, content['error_count'])
        self.assertEqual(False, content['active'])

    def test_stream_start_stop(self):
        result = self._post_and_activate_stream()
        target_id = result['target_id']
        stream_id = result['stream_id']
        headers = {'Authorization': self.auth_token}
        self.assertTrue(scv.ActiveStream.exists(stream_id, self.scv.db))
        response = self.fetch('/streams/stop/'+stream_id, headers=headers,
                              method='PUT', body='')
        self.assertEqual(response.code, 200)
        self.assertFalse(scv.ActiveStream.exists(stream_id, self.scv.db))

        # activating the stream should fail
        body = json.dumps({
            'target_id': target_id
        })
        response = self.fetch('/streams/activate', method='POST', body=body,
                              headers={'Authorization': self.scv.password})
        self.assertEqual(response.code, 400)

        stream = scv.Stream(stream_id, self.scv.db)
        target = scv.Target(target_id, self.scv.db)

        self.assertEqual(stream.hget('status'), 'STOPPED')
        self.assertEqual(target.zrange('queue', 0, -1), [])

        response = self.fetch('/streams/start/'+stream_id, headers=headers,
                              method='PUT', body='')

        self.assertEqual(response.code, 200)
        self.assertEqual(stream.hget('status'), 'OK')
        self.assertEqual(target.zrange('queue', 0, -1), [stream_id])

        # activating stream should succeed
        stream_id_activated, token_id = self._activate_stream(target_id)
        self.assertEqual(stream_id, stream_id_activated)

    def test_priority_queue(self):
        # test to make sure we get the stream with the most number of frames
        result = self._post_stream()
        stream_id = result['stream_id']
        target_id = result['target_id']
        files = result['files']

        token = str(uuid.uuid4())
        stream_id, token = self._activate_stream(target_id)
        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        frame_buffer = bytes()
        n_frames = 25
        active_stream = scv.ActiveStream(stream_id, self.scv.db)

        # PUT 20 frames
        for count in range(n_frames):
            frame_bin = os.urandom(1024)
            frame_buffer += frame_bin
            body = {
                'files': {'frames.xtc.b64':
                          base64.b64encode(frame_bin).decode()}
                }
            response = self.fetch('/core/frame', headers=headers,
                                  body=json.dumps(body), method='PUT')
            self.assertEqual(response.code, 200)
        self.assertEqual(active_stream.hget('buffer_frames'), n_frames)
        streams_dir = self.scv.streams_folder
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer_files',
                                   'frames.xtc')
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # PUT a checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        random_file = random.choice(list(files.keys()))
        body = {'files': {random_file: checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)

        # STOP the stream
        response = self.fetch('/core/stop', headers=headers, method='PUT',
                              body='{}')
        self.assertEqual(response.code, 200)

        new_stream_id, token = self._activate_stream(target_id)
        self.assertEqual(stream_id, new_stream_id)

    def test_heartbeat(self):
        tornado.options.options.heartbeat_increment = 5
        result = self._post_and_activate_stream()
        target_id = result['target_id']
        stream_id = result['stream_id']
        token = result['token']
        test_set = set([stream_id])
        self.assertEqual(scv.ActiveStream.members(self.scv.db), test_set)
        increment_time = tornado.options.options['heartbeat_increment']
        time.sleep(increment_time+0.5)
        self.io_loop.run_sync(self.scv.check_heartbeats)
        self.assertEqual(scv.ActiveStream.members(self.scv.db), set())
        stream_id, token = self._activate_stream(target_id)
        self.assertEqual(scv.ActiveStream.members(self.scv.db), test_set)
        time.sleep(3)
        headers = {'Authorization': token}
        response = self.fetch('/core/heartbeat', method='POST',
                              headers=headers, body='')
        self.assertEqual(response.code, 200)
        self.io_loop.run_sync(self.scv.check_heartbeats)
        self.assertEqual(scv.ActiveStream.members(self.scv.db), test_set)
        time.sleep(3)
        self.io_loop.run_sync(self.scv.check_heartbeats)
        self.assertEqual(scv.ActiveStream.members(self.scv.db), test_set)
        time.sleep(5)
        self.io_loop.run_sync(self.scv.check_heartbeats)
        self.assertEqual(scv.ActiveStream.members(self.scv.db), set())

    def test_expiration(self):
        # test posting a bunch of streams, make random heartbeats to make sure
        # they expire, assert that active_streams U queue = total streams
        tornado.options.options.heartbeat_increment = 10
        result = self._post_stream()
        target_id = result['target_id']
        stream_ids = [result['stream_id']]
        n_streams = 100
        for i in range(n_streams-1):
            stream_ids.append(self._post_stream(target_id)['stream_id'])

        for i in range(5):
            activation_times = []
            action_times = []
            # activate all the streams randomly
            for stream_id in stream_ids:
                activation_times.append(random.uniform(0, 5))
                action_times.append(random.uniform(0, 7))

            action_times = sorted(action_times)
            activation_times = sorted(activation_times)
            tokens = []
            for index, unused in enumerate(activation_times):
                if index == 0:
                    sleep_time = activation_times[index]
                else:
                    sleep_time = activation_times[index] - activation_times[index-1]
                time.sleep(sleep_time)
                active_stream, token = self._activate_stream(target_id)
                tokens.append(token)
            for index, unused in enumerate(action_times):
                if index == 0:
                    sleep_time = action_times[index]
                else:
                    sleep_time = action_times[index] - action_times[index-1]
                time.sleep(sleep_time)
                headers = {'Authorization': tokens[index]}
                if bool(random.getrandbits(1)):
                    print('/core/stop')
                    response = self.fetch('/core/stop', method='PUT',
                                          headers=headers, body='{}')
                else:
                    print('/core/heartbeat')
                    response = self.fetch('/core/heartbeat', method='POST',
                                          headers=headers, body='')
                self.assertEqual(response.code, 200)
                self.io_loop.run_sync(self.scv.check_heartbeats)
           
             # queue U active_streams should equal to total streams
            target = scv.Target(target_id, self.scv.db)
            total_streams = target.smembers('streams')
            target_queue = set(target.zrange('queue', 0, -1))
            active_streams = scv.ActiveStream.members(self.scv.db)
            self.assertEqual(target_queue.union(active_streams), total_streams)
            time.sleep(20)

            self.io_loop.run_sync(self.scv.check_heartbeats)
            target_queue = set(target.zrange('queue', 0, -1))
            self.assertEqual(target_queue, total_streams)
            active_streams = scv.ActiveStream.members(self.scv.db)
            self.assertEqual(active_streams, set())
            # queue U active_streams should equal to total streams


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)
