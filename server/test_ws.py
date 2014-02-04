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
from os.path import isfile


class TestStreamMethods(tornado.testing.AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        redis_port = str(3828)
        self.ws = ws.WorkServer(ws_name='test_server',
                                redis_port=redis_port,
                                ws_ext_http_port=23847)
        super(TestStreamMethods, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.ws.shutdown_redis()
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

        body = {'stream_id': stream_id1}
        response = self.fetch('/streams/delete',
                              method='PUT',
                              body=json.dumps(body))
        self.assertEqual(response.code, 200)
        self.assertEqual(target.smembers('streams'), {stream_id2})

        streams_dir = self.ws.streams_folder
        self.assertFalse(isfile(os.path.join(streams_dir, stream_id1, fn3)))
        self.assertFalse(isfile(os.path.join(streams_dir, stream_id1, fn4)))
        self.assertTrue(target.zscore('queue', stream_id1) is None)

        body = {'stream_id': stream_id2}
        response = self.fetch('/streams/delete',
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
        token = str(uuid.uuid4())
        increment = tornado.options.options['heartbeat_increment']
        stream2 = ws.WorkServer.activate_stream(target_id, token, self.ws.db)
        self.assertEqual(stream1, stream2)
        self.assertTrue(ws.ActiveStream(stream1, self.ws.db))
        self.assertAlmostEqual(self.ws.db.zscore('heartbeats', stream1),
                               time.time()+increment, 2)
        self.assertEqual(ws.ActiveStream.lookup('auth_token',
                         token, self.ws.db), stream1)

    def test_start_stream(self):
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
        token = str(uuid.uuid4())
        stream_id = ws.WorkServer.activate_stream(target_id, token, self.ws.db)
        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        content = json.loads(response.body.decode())
        self.assertEqual(content['target_files'][fn1], fb1)
        self.assertEqual(content['target_files'][fn2], fb2)
        self.assertEqual(content['stream_files'][fn3], fb3)
        self.assertEqual(content['stream_id'], stream_id)
        self.assertEqual(content['target_id'], target_id)

    def test_download_stream(self):
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
        token = str(uuid.uuid4())
        stream_id = ws.WorkServer.activate_stream(target_id, token, self.ws.db)

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
        response = self.fetch('/streams/'+stream_id+'/frames.xtc')
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
        response = self.fetch('/streams/'+stream_id+'/frames.xtc')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, old_buffer)

        # PUT a checkpoint
        checkpoint_bin = base64.b64encode(os.urandom(1024))
        body = {'files': {'state.xml.gz.b64': checkpoint_bin.decode()}}
        response = self.fetch('/core/checkpoint', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)

        # download the frames
        response = self.fetch('/streams/'+stream_id+'/frames.xtc')
        self.assertEqual(response.code, 200)
        self.assertEqual(response.body, frame_buffer)

    def test_put_frame(self):
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
        token = str(uuid.uuid4())
        stream_id = ws.WorkServer.activate_stream(target_id, token, self.ws.db)

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
                                       str(n_frames)+'_state.xml.gz.b64')
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
        total_frames = n_frames+more_frames

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
        # make sure the old checkpoint is removed
        self.assertFalse(isfile(checkpoint_path))
        checkpoint_path = os.path.join(streams_dir, stream_id,
                                       str(total_frames)+'_state.xml.gz.b64')
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
        checkpoint_path = os.path.join(streams_dir, stream_id,
                                       str(total_frames)+'_state.xml.gz.b64')
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
        token = str(uuid.uuid4())
        stream_id = ws.WorkServer.activate_stream(target_id, token, self.ws.db)
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

        body = {'error': 'NaN'}
        response = self.fetch('/core/stop', headers=headers, method='PUT',
                              body=json.dumps(body))
        self.assertEqual(response.code, 200)
        self.assertEqual(stream.hget('error_count'), 1)
        self.assertFalse(ws.ActiveStream.exists(stream_id, self.ws.db))
        self.assertFalse(target.zscore('queue', stream_id) is None)
        buffer_path = os.path.join(self.ws.streams_folder,
                                   stream_id, 'buffer_frames.xtc')
        self.assertFalse(os.path.exists(buffer_path))

    def test_stop_stream(self):
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
        token = str(uuid.uuid4())
        stream_id = ws.WorkServer.activate_stream(target_id, token, self.ws.db)
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
        # for sanity
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
        token = str(uuid.uuid4())
        self.assertEqual(ws.ActiveStream.members(self.ws.db), set())
        stream_id = ws.WorkServer.activate_stream(target_id, token, self.ws.db)
        test_set = set([stream_id])
        self.assertEqual(ws.ActiveStream.members(self.ws.db), test_set)
        increment_time = tornado.options.options['heartbeat_increment']
        time.sleep(increment_time+0.5)
        self.ws.check_heartbeats()
        self.assertEqual(ws.ActiveStream.members(self.ws.db), set())


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)
