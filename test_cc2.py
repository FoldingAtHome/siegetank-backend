import tornado.testing
import cc
import os
import shutil
import unittest
import sys
import common
import json


class TestWSRegistration(tornado.testing.AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        redis_port = str(3828)
        self.increment = 3
        self.cc_auth = '5lik2j3l4'
        self.cc = cc.CommandCenter('test_cc', redis_port, self.cc_auth)
        super(TestWSRegistration, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.cc.db.flushdb()
        self.cc.shutdown_redis()
        folders = ['targets']
        for folder in folders:
            if os.path.exists(folder):
                shutil.rmtree(folder)
        super(TestWSRegistration, self).tearDownClass()

    def test_register_cc(self):

        ws_name = 'ramanujan'
        ext_http_port = 5829
        ws_redis_port = 1234
        ws_redis_pass = 'blackmill'

        test_db = common.init_redis(ws_redis_port, ws_redis_pass)
        test_db.ping()

        body = {'name': ws_name,
                'http_port': ext_http_port,
                'redis_port': ws_redis_port,
                'redis_pass': ws_redis_pass,
                'auth': self.cc_auth
                }

        reply = self.fetch('/register_ws', method='PUT', body=json.dumps(body))
        self.assertEqual(reply.code, 200)

        ws = cc.WorkServer(ws_name, self.cc.db)
        self.assertEqual(ws.hget('ip'), '127.0.0.1')
        self.assertEqual(ws.hget('http_port'), ext_http_port)
        self.assertEqual(ws.hget('redis_port'), ws_redis_port)
        self.assertEqual(ws.hget('redis_pass'), ws_redis_pass)

        test_db.shutdown()

    def get_app(self):
        return self.cc

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)
