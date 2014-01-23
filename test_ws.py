import tornado.testing
import ws

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
        self.increment = 3
        self.ws = ws.WorkServer('test_server', redis_port, None,
                                23847, None, self.increment)
        super(TestStreamMethods, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.ws.db.flushdb()
        self.ws.shutdown_redis()
        folders = [self.ws.targets_folder, self.ws.streams_folder]
        for folder in folders:
            if os.path.exists(folder):
                shutil.rmtree(folder)
        super(TestStreamMethods, self).tearDownClass()

    def get_app(self):
        return self.ws

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
        self.assertEqual(ws.Target(target_id, self.ws.db).smembers('files'),
                         {fn1, fn2})
        self.assertEqual(ws.Stream(stream_id, self.ws.db).smembers('files'),
                         {fn3, fn4})

        # post a second stream

        fn5, fb5 = (str(uuid.uuid4()) for i in range(2))

        body = {'target_id': target_id,
                'stream_files': {fn5: fb5}
                }

        response = self.fetch('/streams', method='POST', body=json.dumps(body))
        self.assertEqual(response.code, 200)
        stream_id2 = json.loads(response.body.decode())['stream_id']

        self.assertTrue(stream_id != stream_id2)
        streams_dir = self.ws.streams_folder
        self.assertTrue(isfile(os.path.join(streams_dir, stream_id2, fn5)))
        self.assertEqual(ws.Stream(stream_id2, self.ws.db).hget('target'),
                         target_id)
        self.assertEqual(ws.Target(target_id, self.ws.db).smembers('streams'),
                         {stream_id, stream_id2})
        self.assertEqual(ws.Target(target_id, self.ws.db).smembers('files'),
                         {fn1, fn2})
        self.assertEqual(ws.Stream(stream_id2, self.ws.db).smembers('files'),
                         {fn5})

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
                'target_files': {fn5: fb5, fn6: fb6},
                'stream_files': {fn7: fb7, fn8: fb8}
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
        increment = 30*60
        stream2 = ws.WorkServer.activate_stream(target_id, token,
                                                self.ws.db, 30*60)
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
        stream_id = ws.WorkServer.activate_stream(target_id, token,
                                                  self.ws.db, 30*60)
        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        content = json.loads(response.body.decode())
        self.assertEqual(content['target_files'][fn1], fb1)
        self.assertEqual(content['target_files'][fn2], fb2)
        self.assertEqual(content['stream_files'][fn3], fb3)
        self.assertEqual(content['stream_id'], stream_id)
        self.assertEqual(content['target_id'], target_id)

    def test_post_frame(self):
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
        stream_id = ws.WorkServer.activate_stream(target_id, token,
                                                  self.ws.db, 30*60)

        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)

        frame_buffer = bytes()
        n_frames = 25
        active_stream = ws.ActiveStream(stream_id, self.ws.db)
        stream = ws.Stream(stream_id, self.ws.db)

        for count in range(n_frames):
            frame_bin = os.urandom(1024)
            frame_buffer += frame_bin
            body = {'frame': base64.b64encode(frame_bin).decode()}
            response = self.fetch('/core/frame', headers=headers,
                                  body=json.dumps(body), method='PUT')
            self.assertEqual(response.code, 200)

        self.assertEqual(active_stream.hget('buffer_frames'), n_frames)

        streams_dir = self.ws.streams_folder
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer.xtc')
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # PUT a checkpoint
        frame_bin = os.urandom(1024)
        checkpoint_bin = os.urandom(1024)
        body = {'frame': base64.b64encode(frame_bin).decode(),
                'checkpoint': base64.b64encode(checkpoint_bin).decode()
                }
        response = self.fetch('/core/frame', headers=headers,
                              body=json.dumps(body), method='PUT')
        self.assertEqual(response.code, 200)
        self.assertEqual(active_stream.hget('buffer_frames'), 0)
        self.assertEqual(stream.hget('frames'), n_frames+1)
        self.assertEqual(b'', open(buffer_path, 'rb').read())
        checkpoint_path = os.path.join(streams_dir, stream_id, fn3)
        self.assertEqual(checkpoint_bin, open(checkpoint_path, 'rb').read())
        frame_buffer += frame_bin
        frames_path = os.path.join(streams_dir, stream_id, 'frames.xtc')
        self.assertEqual(frame_buffer, open(frames_path, 'rb').read())

        # PUT a few more frames
        frame_buffer = bytes()
        n_frames = 5
        for count in range(n_frames):
            frame_bin = os.urandom(1024)
            frame_buffer += frame_bin
            body = {'frame': base64.b64encode(frame_bin).decode()}
            response = self.fetch('/core/frame', headers=headers,
                                  body=json.dumps(body), method='PUT')
            self.assertEqual(response.code, 200)
        self.assertEqual(active_stream.hget('buffer_frames'), n_frames)
        buffer_path = os.path.join(streams_dir, stream_id, 'buffer.xtc')
        self.assertEqual(frame_buffer, open(buffer_path, 'rb').read())

        # test idempotency of PUT
        frame_bin = os.urandom(1024)
        frame_buffer += frame_bin
        body = {'frame': base64.b64encode(frame_bin).decode()}
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
        stream_id = ws.WorkServer.activate_stream(target_id, token,
                                                  self.ws.db, 30*60)
        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        frame_buffer = bytes()
        n_frames = 25

        active_stream = ws.ActiveStream(stream_id, self.ws.db)
        stream = ws.Stream(stream_id, self.ws.db)
        target = ws.Target(target_id, self.ws.db)

        for count in range(n_frames):
            frame_bin = os.urandom(1024)
            frame_buffer += frame_bin
            body = {'frame': base64.b64encode(frame_bin).decode()}
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
                                   stream_id, 'buffer.xtc')
        self.assertEqual(b'', open(buffer_path, 'rb').read())

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
        stream_id = ws.WorkServer.activate_stream(target_id, token,
                                                  self.ws.db, 30*60)
        headers = {'Authorization': token}
        response = self.fetch('/core/start', headers=headers, method='GET')
        self.assertEqual(response.code, 200)
        frame_buffer = bytes()
        n_frames = 25

        active_stream = ws.ActiveStream(stream_id, self.ws.db)
        stream = ws.Stream(stream_id, self.ws.db)
        target = ws.Target(target_id, self.ws.db)

        for count in range(n_frames):
            frame_bin = os.urandom(1024)
            frame_buffer += frame_bin
            body = {'frame': base64.b64encode(frame_bin).decode()}
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
                                   stream_id, 'buffer.xtc')
        self.assertEqual(b'', open(buffer_path, 'rb').read())


    #  def test_heartbeat(self):
    #     token_id = str(uuid.uuid4())
    #     stream_id = str(uuid.uuid4())
    #     self.redis_client.set('shared_token:'+token_id+':active_stream', stream_id)

    #     # test sending request to uri: /heartbeat extends the expiration time
    #     for iteration in range(10):
    #         response = self.fetch('/heartbeat', method='POST',
    #                             body=json.dumps({'shared_token' : token_id}))
    #         hb = self.redis_client.zscore('heartbeats',stream_id)
    #         ws_start_time = common.sum_time(self.redis_client.time())
    #         self.assertAlmostEqual(ws_start_time+self.increment, hb, places=1)
    #     # test expirations
    #     response = self.fetch('/heartbeat', method='POST',
    #                 body=json.dumps({'shared_token' : token_id}))
    #     self.redis_client.sadd('active_streams',stream_id)
    #     self.redis_client.hset('active_stream:'+stream_id, 
    #                            'shared_token', token_id)
    #     self.assertTrue(
    #         self.redis_client.sismember('active_streams',stream_id) and
    #         self.redis_client.exists('active_stream:'+stream_id) and 
    #         self.redis_client.exists('shared_token:'+token_id+':active_stream'))
    #     time.sleep(self.increment+1)
    #     self.ws.check_heartbeats() 
    #     self.assertFalse(
    #         self.redis_client.sismember('active_streams',stream_id) and
    #         self.redis_client.exists('active_stream:'+stream_id) and 
    #         self.redis_client.exists('shared_token:'+token_id+':active_stream'))
    # 

    # def test_init_stream(self):
    #     active_streams = self.redis_client.exists('active_streams')
    #     active_stream_ids = self.redis_client.keys('active_stream:*')
    #     shared_tokens = self.redis_client.keys('shared_token:*')

    #     self.assertFalse(active_streams)
    #     self.assertFalse(active_stream_ids)
    #     self.assertFalse(shared_tokens)

    #     self.assertTrue(self.redis_client.sismember('ccs','test_cc'))
    #     self.assertEqual(self.redis_client.hget('cc:test_cc','ip'),
    #                      '127.0.0.1')
    #     self.assertEqual(self.redis_client.hget('cc:test_cc','http_port'),
    #                      '999')

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)
