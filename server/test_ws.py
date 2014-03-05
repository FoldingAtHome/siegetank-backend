import tornado.testing
import server.ws as ws

import unittest
import os
import shutil
import sys
import uuid
import json
import time
import base64
import random
import pymongo
from os.path import isfile


class TestStreamMethods(tornado.testing.AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        super(TestStreamMethods, self).setUpClass()
        external_options = {
            'external_url': '127.0.0.1',
            'external_http_port': '8960'
        }
        redis_options = {
            'port': 3828,
            'logfile': os.devnull
        }
        mongo_options = {
            'host': 'localhost',
            'port': 27017
        }

        self.ws = ws.WorkServer(ws_name='test_server',
                                mongo_options=mongo_options,
                                external_options=external_options,
                                redis_options=redis_options)

        # add a manager for testing purposes
        token = str(uuid.uuid4())
        test_manager = "test_ws@gmail.com"
        db_body = {'_id': test_manager,
                   'token': token,
                   'role': 'manager'
                   }

        managers = self.ws.mdb.users.managers

        try:
            managers.insert(db_body)
        except pymongo.errors.DuplicateKeyError:
            pass

        self.auth_token = token
        self.test_manager = test_manager

    @classmethod
    def tearDownClass(self):
        self.ws.shutdown_redis()
        self.ws.mdb.drop_database('users')
        self.ws.mdb.drop_database('community')
        self.ws.mdb.drop_database('data')
        folders = [self.ws.targets_folder, self.ws.streams_folder]
        for folder in folders:
            if os.path.exists(folder):
                shutil.rmtree(folder)
        super(TestStreamMethods, self).tearDownClass()

    def get_app(self):
        return self.ws

    def tearDown(self):
        super(TestStreamMethods, self).tearDown()
        self.ws.db.flushdb()
        self.ws.mdb.data.targets.drop()

    def test_post_stream(self):
        target_id = str(uuid.uuid4())
        fn1, fn2, fn3, fn4 = (str(uuid.uuid4()) for i in range(4))
        fb1, fb2, fb3, fb4 = (str(uuid.uuid4()) for i in range(4))

        body = {'target_id': target_id,
                'target_files': {fn1: fb1, fn2: fb2},
                'stream_files': {fn3: fb3, fn4: fb4}
                }

        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 200)
        stream_id = json.loads(response.body.decode())['stream_id']

        targets_dir = self.ws.targets_folder
        streams_dir = self.ws.streams_folder

        self.assertTrue(isfile(os.path.join(targets_dir, target_id, fn1)))
        self.assertTrue(isfile(os.path.join(targets_dir, target_id, fn2)))
        self.assertTrue(isfile(os.path.join(streams_dir, stream_id, fn3)))
        self.assertTrue(isfile(os.path.join(streams_dir, stream_id, fn4)))

        target = ws.Target(target_id, self.ws.db)
        self.assertFalse(target.zscore('queue', stream_id) is None)
        self.assertTrue(ws.Stream.exists(stream_id, self.ws.db))
        self.assertTrue(ws.Target.exists(target_id, self.ws.db))
        self.assertEqual(ws.Stream(stream_id, self.ws.db).hget('target'),
                         target_id)
        self.assertSetEqual(
            ws.Target(target_id, self.ws.db).smembers('streams'), {stream_id})
        self.assertEqual(
            ws.Target(target_id, self.ws.db).smembers('target_files'),
            {fn1, fn2})
        self.assertEqual(
            ws.Target(target_id, self.ws.db).smembers('stream_files'),
            {fn3, fn4})

        # post a second stream with bad filenames

        fn5, fb5 = (str(uuid.uuid4()) for i in range(2))

        body = {'target_id': target_id,
                'stream_files': {fn5: fb5}
                }

        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 400)

        # post a third stream with the correct filenames

        body = {'target_id': target_id,
                'stream_files': {fn3: fb5, fn4: fn5}
                }
        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 200)
        stream_id2 = json.loads(response.body.decode())['stream_id']

        self.assertTrue(stream_id != stream_id2)
        streams_dir = self.ws.streams_folder
        self.assertTrue(isfile(os.path.join(streams_dir, stream_id2, fn3)))
        self.assertTrue(isfile(os.path.join(streams_dir, stream_id2, fn4)))
        self.assertEqual(ws.Stream(stream_id2, self.ws.db).hget('target'),
                         target_id)
        self.assertEqual(ws.Target(target_id, self.ws.db).smembers('streams'),
                         {stream_id, stream_id2})
        self.assertEqual(
            ws.Target(target_id, self.ws.db).smembers('target_files'),
            {fn1, fn2})
        self.assertEqual(
            ws.Target(target_id, self.ws.db).smembers('stream_files'),
            {fn3, fn4})

    def test_delete_stream(self):
        target_id = str(uuid.uuid4())
        fn1, fn2, fn3, fn4 = (str(uuid.uuid4()) for i in range(4))
        fb1, fb2, fb3, fb4 = (str(uuid.uuid4()) for i in range(4))
        body = {'target_id': target_id,
                'target_files': {fn1: fb1, fn2: fb2},
                'stream_files': {fn3: fb3, fn4: fb4}
                }
        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 200)
        stream_id1 = json.loads(response.body.decode())['stream_id']
        target = ws.Target(target_id, self.ws.db)
        self.assertEqual(target.smembers('streams'), {stream_id1})

        fn5, fn6, fn7, fn8 = (str(uuid.uuid4()) for i in range(4))
        fb5, fb6, fb7, fb8 = (str(uuid.uuid4()) for i in range(4))
        body = {'target_id': target_id,
                'target_files': {fn1: fb5, fn2: fb6},
                'stream_files': {fn3: fb7, fn4: fb8}
                }
        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 200)
        stream_id2 = json.loads(response.body.decode())['stream_id']
        self.assertEqual(target.smembers('streams'), {stream_id1, stream_id2})

        response = self.fetch('/streams/delete/'+stream_id1,
                              method='PUT',
                              body=json.dumps(body))
        self.assertEqual(response.code, 200)
        self.assertEqual(target.smembers('streams'), {stream_id2})

        streams_dir = self.ws.streams_folder
        self.assertFalse(isfile(os.path.join(streams_dir, stream_id1, fn3)))
        self.assertFalse(isfile(os.path.join(streams_dir, stream_id1, fn4)))
        self.assertTrue(target.zscore('queue', stream_id1) is None)

        body = {'stream_id': stream_id2}
        response = self.fetch('/streams/delete/'+stream_id2,
                              method='PUT',
                              body=json.dumps(body))
        self.assertEqual(response.code, 200)
        self.assertFalse(ws.Target.exists(target_id, self.ws.db))
        self.assertFalse(isfile(os.path.join(streams_dir, stream_id2, fn5)))
        self.assertFalse(isfile(os.path.join(streams_dir, stream_id2, fn6)))
        self.assertTrue(target.zscore('queue', stream_id1) is None)

        targets_dir = self.ws.targets_folder

        self.assertFalse(isfile(os.path.join(targets_dir, target_id, fn1)))
        self.assertFalse(isfile(os.path.join(targets_dir, target_id, fn2)))

    def _activate_stream(self, target_id):
        body = {'target_id': target_id}
        reply = self.fetch('/streams/activate', method='POST',
                           body=json.dumps(body))

        self.assertEqual(reply.code, 200)
        reply_data = json.loads(reply.body.decode())
        token = reply_data['token']
        stream_id = ws.ActiveStream.lookup('auth_token', token, self.ws.db)
        return stream_id, token

    def test_activate_stream(self):
        target_id = str(uuid.uuid4())
        fn1, fn2, fn3, fn4 = (str(uuid.uuid4()) for i in range(4))
        fb1, fb2, fb3, fb4 = (str(uuid.uuid4()) for i in range(4))
        body = {'target_id': target_id,
                'target_files': {fn1: fb1, fn2: fb2},
                'stream_files': {fn3: fb3, fn4: fb4}
                }
        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 200)
        stream1 = json.loads(response.body.decode())['stream_id']
        stream2, token = self._activate_stream(target_id)
        self.assertEqual(stream1, stream2)
        self.assertTrue(ws.ActiveStream(stream1, self.ws.db))
        increment = tornado.options.options['heartbeat_increment']
        self.assertAlmostEqual(self.ws.db.zscore('heartbeats', stream1),
                               time.time()+increment, 1)
        self.assertEqual(ws.ActiveStream.lookup('auth_token',
                         token, self.ws.db), stream1)

    def test_get_active_streams(self):
        tornado.options.options.heartbeat_increment = 10
        target_id = str(uuid.uuid4())
        fn1, fn2, fn3, fn4 = (str(uuid.uuid4()) for i in range(4))
        fb1, fb2, fb3, fb4 = (str(uuid.uuid4()) for i in range(4))
        body = {'target_id': target_id,
                'target_files': {fn1: fb1, fn2: fb2},
                'stream_files': {fn3: fb3, fn4: fb4}
                }
        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 200)
        stream1 = json.loads(response.body.decode())['stream_id']
        token = str(uuid.uuid4())
        stream2, token = self._activate_stream(target_id)
        self.assertEqual(stream1, stream2)
        reply = self.fetch('/active_streams', method='GET')
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())
        for tid in content:
            self.assertEqual(tid, target_id)
            for sid in content[tid]:
                self.assertEqual(sid, stream1)
        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 200)
        new_stream1 = json.loads(response.body.decode())['stream_id']
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

    def _post_and_activate_stream(self):
        target_id = str(uuid.uuid4())

        targets = self.ws.mdb.data.targets
        body = {
            '_id': target_id,
            'owner': self.test_manager,
        }
        targets.insert(body)

        fn1 = 'system.xml.gz.b64'
        fn2 = 'integrator.xml.gz.b64'
        fn3 = 'state.xml.gz.b64'
        fb1, fb2, fb3 = (str(uuid.uuid4()) for i in range(3))

        body = {'target_id': target_id,
                'target_files': {fn1: fb1, fn2: fb2},
                'stream_files': {fn3: fb3}
                }

        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 200)
        stream_id, token = self._activate_stream(target_id)
        return target_id, fn1, fn2, fn3, fb1, fb2, fb3, stream_id, token

    def test_delete_target(self):
        target_id = str(uuid.uuid4())

        targets = self.ws.mdb.data.targets
        body = {
            '_id': target_id,
            'owner': self.test_manager,
        }
        targets.insert(body)

        fn1 = 'system.xml.gz.b64'
        fn2 = 'integrator.xml.gz.b64'
        fn3 = 'state.xml.gz.b64'
        fb1, fb2, fb3 = (str(uuid.uuid4()) for i in range(3))

        body = {'target_id': target_id,
                'target_files': {fn1: fb1, fn2: fb2},
                'stream_files': {fn3: fb3}
                }

        total_streams = set()

        for i in range(50):
            response = self.fetch('/streams', method='POST',
                                  body=json.dumps(body))
            self.assertEqual(response.code, 200)
            total_streams.add(json.loads(response.body.decode())['stream_id'])

        active_streams = set()

        for i in range(10):
            stream_id, token = self._activate_stream(target_id)
            active_streams.add(stream_id)

        self.assertEqual(ws.Target.members(self.ws.db), {target_id})
        self.assertEqual(ws.Stream.members(self.ws.db), total_streams)
        self.assertEqual(ws.ActiveStream.members(self.ws.db), active_streams)
        for stream_id in total_streams:
            stream_dir = os.path.join(self.ws.streams_folder, stream_id)
            self.assertTrue(os.path.exists(stream_dir))
        target_dir = os.path.join(self.ws.targets_folder, target_id)
        self.assertTrue(os.path.exists(target_dir))

        reply = self.fetch('/targets/delete/'+target_id, method='PUT', body='')
        self.assertEqual(reply.code, 200)

        self.assertEqual(ws.Target.members(self.ws.db), set())
        self.assertEqual(ws.Stream.members(self.ws.db), set())
        self.assertEqual(ws.ActiveStream.members(self.ws.db), set())
        for stream_id in total_streams:
            stream_dir = os.path.join(self.ws.streams_folder, stream_id)
            self.assertFalse(os.path.exists(stream_dir))
        self.assertFalse(os.path.exists(target_dir))

    def test_start_stream(self):
        target_id, fn1, fn2, fn3, fb1, fb2, fb3, stream_id, token = \
            self._post_and_activate_stream()

        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        content = json.loads(response.body.decode())
        self.assertEqual(content['target_files'][fn1], fb1)
        self.assertEqual(content['target_files'][fn2], fb2)
        self.assertEqual(content['stream_files'][fn3], fb3)
        self.assertEqual(content['stream_id'], stream_id)
        self.assertEqual(content['target_id'], target_id)

    def test_get_streams(self):
        target_id, fn1, fn2, fn3, fb1, fb2, fb3, stream_id, token = \
            self._post_and_activate_stream()

        # post another stream
        fn1 = 'system.xml.gz.b64'
        fn2 = 'integrator.xml.gz.b64'
        fn3 = 'state.xml.gz.b64'
        fb1, fb2, fb3 = (str(uuid.uuid4()) for i in range(3))
        body = {'target_id': target_id,
                'target_files': {fn1: fb1, fn2: fb2},
                'stream_files': {fn3: fb3}
                }
        reply = self.fetch('/streams', method='POST', body=json.dumps(body))
        stream_id2 = json.loads(reply.body.decode())['stream_id']
        self.assertEqual(reply.code, 200)

        headers = {'Authorization': token}
        reply = self.fetch('/targets/streams/'+target_id, headers=headers,
                           method='GET')
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())

        sids = set()

        for sid in content:
            sids.add(sid)
            frames = content[sid]['frames']
            status = content[sid]['status']
            self.assertEqual(frames, 0)
            self.assertEqual(status, 'OK')

        self.assertEqual(sids, {stream_id2, stream_id})

    def test_download_stream(self):
        target_id, fn1, fn2, fn3, fb1, fb2, fb3, stream_id, token = \
            self._post_and_activate_stream()

        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)

        frame_buffer = bytes()
        n_frames = 25
        active_stream = ws.ActiveStream(stream_id, self.ws.db)

        # PUT 25 frames
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

        streams_dir = self.ws.streams_folder
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer_frames.xtc')
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # PUT a checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        body = {'files': {'state.xml.gz.b64': checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)

        # download the frames
        manager_headers = {'Authorization': self.auth_token}
        response = self.fetch('/streams/'+stream_id+'/frames.xtc',
                              headers=manager_headers)
        print(response.body)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, frame_buffer)

        old_buffer = frame_buffer
        # PUT 25 more frames
        for count in range(n_frames):
            frame_bin = os.urandom(5)
            frame_buffer += frame_bin
            body = {
                'files': {'frames.xtc.b64':
                          base64.b64encode(frame_bin).decode()}
                }
            response = self.fetch('/core/frame', headers=headers,
                                  body=json.dumps(body), method='PUT')
            self.assertEqual(response.code, 200)
        self.assertEqual(active_stream.hget('buffer_frames'), n_frames)

        streams_dir = self.ws.streams_folder
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer_frames.xtc')

        # download the frames
        response = self.fetch('/streams/'+stream_id+'/frames.xtc',
                              headers=manager_headers)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, old_buffer)

        # PUT a checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        body = {'files': {'state.xml.gz.b64': checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)

        # download the frames
        response = self.fetch('/streams/'+stream_id+'/frames.xtc',
                              headers=manager_headers)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, frame_buffer)

    def test_put_frame_variadic(self):
        target_id, fn1, fn2, fn3, fb1, fb2, fb3, stream_id, token = \
            self._post_and_activate_stream()

        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)

        active_stream = ws.ActiveStream(stream_id, self.ws.db)
        stream = ws.Stream(stream_id, self.ws.db)

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
        streams_dir = self.ws.streams_folder
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer_frames.xtc')
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # add a checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        body = {'files': {'state.xml.gz.b64': checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)
        self.assertEqual(active_stream.hget('buffer_frames'), 0)
        self.assertEqual(stream.hget('frames'), sum(n_counts))
        self.assertFalse(os.path.exists(buffer_path))

        # test downloading the frames again
        manager_headers = {'Authorization': self.auth_token}
        response = self.fetch('/streams/'+stream_id+'/frames.xtc',
                              headers = manager_headers)
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, frame_buffer)

    def test_put_frame(self):
        target_id, fn1, fn2, fn3, fb1, fb2, fb3, stream_id, token = \
            self._post_and_activate_stream()

        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)

        frame_buffer = bytes()
        n_frames = 25
        active_stream = ws.ActiveStream(stream_id, self.ws.db)
        stream = ws.Stream(stream_id, self.ws.db)

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

        streams_dir = self.ws.streams_folder
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer_frames.xtc')
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # PUT a checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        body = {'files': {'state.xml.gz.b64': checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)
        self.assertEqual(active_stream.hget('buffer_frames'), 0)
        self.assertEqual(stream.hget('frames'), n_frames)
        self.assertFalse(os.path.exists(buffer_path))
        checkpoint_path = os.path.join(streams_dir, stream_id,
                                       'state.xml.gz.b64')
        self.assertEqual(checkpoint_bin, open(checkpoint_path, 'rb').read())
        frames_path = os.path.join(streams_dir, stream_id,
                                   str(n_frames)+'_frames.xtc')
        self.assertEqual(frame_buffer, open(frames_path, 'rb').read())

        # PUT a few more frames
        frame_buffer = bytes()
        more_frames = 5
        for count in range(more_frames):
            frame_bin = os.urandom(1024)
            frame_buffer += frame_bin
            body = {
                'files': {'frames.xtc.b64':
                          base64.b64encode(frame_bin).decode()}
                }
            response = self.fetch('/core/frame', headers=headers,
                                  body=json.dumps(body), method='PUT')
            self.assertEqual(response.code, 200)
        self.assertEqual(active_stream.hget('buffer_frames'), more_frames)
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # PUT another checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        body = {'files': {'state.xml.gz.b64': checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertFalse(os.path.exists(buffer_path))
        self.assertEqual(stream.hget('frames'), n_frames+more_frames)
        self.assertEqual(active_stream.hget('buffer_frames'), 0)
        initial_state_path = os.path.join(streams_dir, stream_id,
                                          'state.xml.gz.b64')
        self.assertTrue(isfile(initial_state_path))
        self.assertEqual(checkpoint_bin, open(checkpoint_path, 'rb').read())

        # test idempotency of put checkpoint
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertFalse(os.path.exists(buffer_path))
        self.assertEqual(stream.hget('frames'), n_frames+more_frames)
        self.assertEqual(active_stream.hget('buffer_frames'), 0)
        initial_state_path = os.path.join(streams_dir, stream_id,
                                          'state.xml.gz.b64')
        self.assertTrue(isfile(initial_state_path))
        self.assertEqual(checkpoint_bin, open(checkpoint_path, 'rb').read())

        # test idempotency of put frame
        frame_bin = os.urandom(1024)
        frame_buffer = frame_bin
        body = {
            'files': {'frames.xtc.b64':
                      base64.b64encode(frame_bin).decode()}
            }
        response = self.fetch('/core/frame', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())
        response = self.fetch('/core/frame', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

    def test_stop_error_stream(self):
        target_id, fn1, fn2, fn3, fb1, fb2, fb3, stream_id, token = \
            self._post_and_activate_stream()
        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        frame_buffer = bytes()
        n_frames = 25

        stream = ws.Stream(stream_id, self.ws.db)
        target = ws.Target(target_id, self.ws.db)

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

        self.assertTrue(ws.ActiveStream.exists(stream_id, self.ws.db))
        self.assertTrue(target.zscore('queue', stream_id) is None)

        body = {'error': base64.b64encode(b'NaN').decode()}
        response = self.fetch('/core/stop', headers=headers, method='PUT',
                              body=json.dumps(body))
        self.assertEqual(response.code, 200)
        self.assertEqual(stream.hget('error_count'), 1)
        self.assertFalse(ws.ActiveStream.exists(stream_id, self.ws.db))
        self.assertFalse(target.zscore('queue', stream_id) is None)
        buffer_path = os.path.join(self.ws.streams_folder,
                                   stream_id, 'buffer_frames.xtc')
        self.assertFalse(os.path.exists(buffer_path))

    def test_priority_queue(self):
        # test to make sure we get the stream with the most number of frames
        target_id = str(uuid.uuid4())
        fn1 = 'system.xml.gz.b64'
        fn2 = 'integrator.xml.gz.b64'
        fn3 = 'state.xml.gz.b64'
        fb1, fb2, fb3 = (str(uuid.uuid4()) for i in range(3))

        for i in range(20):
            body = {'target_id': target_id,
                    'target_files': {fn1: fb1, fn2: fb2},
                    'stream_files': {fn3: fb3}
                    }
            response = self.fetch('/streams', method='POST',
                                  body=json.dumps(body))
            self.assertEqual(response.code, 200)

        token = str(uuid.uuid4())
        stream_id, token = self._activate_stream(target_id)
        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        frame_buffer = bytes()
        n_frames = 25
        active_stream = ws.ActiveStream(stream_id, self.ws.db)

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
        streams_dir = self.ws.streams_folder
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer_frames.xtc')
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # PUT a checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        body = {'files': {'state.xml.gz.b64': checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)

        # STOP the stream
        response = self.fetch('/core/stop', headers=headers, method='PUT',
                              body='{}')
        self.assertEqual(response.code, 200)

        new_stream_id, token = self._activate_stream(target_id)
        self.assertEqual(stream_id, new_stream_id)

    def test_stop_stream(self):
        target_id, fn1, fn2, fn3, fb1, fb2, fb3, stream_id, token = \
            self._post_and_activate_stream()
        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        frame_buffer = bytes()
        n_frames = 25

        stream = ws.Stream(stream_id, self.ws.db)
        target = ws.Target(target_id, self.ws.db)

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

        self.assertTrue(ws.ActiveStream.exists(stream_id, self.ws.db))
        self.assertTrue(target.zscore('queue', stream_id) is None)

        response = self.fetch('/core/stop', headers=headers, method='PUT',
                              body='{}')
        self.assertEqual(response.code, 200)
        self.assertEqual(stream.hget('error_count'), 0)
        self.assertFalse(ws.ActiveStream.exists(stream_id, self.ws.db))
        self.assertFalse(target.zscore('queue', stream_id) is None)
        buffer_path = os.path.join(self.ws.streams_folder,
                                   stream_id, 'buffer_frames.xtc')
        self.assertFalse(os.path.exists(buffer_path))

    def test_heartbeat(self):
        tornado.options.options.heartbeat_increment = 5
        target_id = str(uuid.uuid4())
        fn1 = 'system.xml.gz.b64'
        fn2 = 'integrator.xml.gz.b64'
        fn3 = 'state.xml.gz.b64'
        fb1, fb2, fb3 = (str(uuid.uuid4()) for i in range(3))
        body = {'target_id': target_id,
                'target_files': {fn1: fb1, fn2: fb2},
                'stream_files': {fn3: fb3}
                }
        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 200)
        self.assertEqual(ws.ActiveStream.members(self.ws.db), set())
        stream_id, token = self._activate_stream(target_id)
        test_set = set([stream_id])
        self.assertEqual(ws.ActiveStream.members(self.ws.db), test_set)
        increment_time = tornado.options.options['heartbeat_increment']
        time.sleep(increment_time+0.5)
        self.ws.check_heartbeats()
        self.assertEqual(ws.ActiveStream.members(self.ws.db), set())
        stream_id, token = self._activate_stream(target_id)
        self.assertEqual(ws.ActiveStream.members(self.ws.db), test_set)
        time.sleep(3)
        body = '{}'
        headers = {'Authorization': token}
        response = self.fetch('/core/heartbeat', method='POST',
                              headers=headers, body=json.dumps(body))
        self.assertEqual(response.code, 200)
        self.ws.check_heartbeats()
        self.assertEqual(ws.ActiveStream.members(self.ws.db), test_set)
        time.sleep(3)
        self.ws.check_heartbeats()
        self.assertEqual(ws.ActiveStream.members(self.ws.db), test_set)
        time.sleep(5)
        self.ws.check_heartbeats()
        self.assertEqual(ws.ActiveStream.members(self.ws.db), set())


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)
