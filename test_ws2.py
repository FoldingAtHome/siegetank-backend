import tornado.testing
import ws

import unittest
import os
import shutil
import sys
import uuid
import json
import apollo
from os.path import isfile

class TestStreamMethods(tornado.testing.AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        redis_port = str(3828)
        self.increment = 3
        cc = ('test_cc', '127.0.0.1', '999', 'PROTOSS_IS_FOR_NOOB')
        self.ws = ws.WorkServer(
            'test_server', redis_port, None, [cc], self.increment)
        super(TestStreamMethods, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.ws.shutdown_redis()
        folders = ['streams', 'targets']
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

        self.assertTrue(isfile(os.path.join('targets', target_id, fn1)))
        self.assertTrue(isfile(os.path.join('targets', target_id, fn2)))
        self.assertTrue(isfile(os.path.join('streams', stream_id, fn3)))
        self.assertTrue(isfile(os.path.join('streams', stream_id, fn4)))

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
        self.assertTrue(isfile(os.path.join('streams', stream_id2, fn5)))
        self.assertEqual(ws.Stream(stream_id2, self.ws.db).hget('target'),
                         target_id)
        self.assertSetEqual(
            ws.Target(target_id, self.ws.db).smembers('streams'),
            {stream_id, stream_id2})
        self.assertEqual(ws.Target(target_id, self.ws.db).smembers('files'),
                         {fn1, fn2})
        self.assertEqual(ws.Stream(stream_id2, self.ws.db).smembers('files'),
                         {fn5})

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
