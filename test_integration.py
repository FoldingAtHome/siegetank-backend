import unittest
import os
import shutil
import sys

import tornado
import tornado.web
import tornado.httpclient
import tornado.testing
import tornado.gen

import ws
import cc


class Handler1(tornado.web.RequestHandler):
    def get(self):
        self.write("One")


class Handler2(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        client = tornado.httpclient.AsyncHTTPClient()
        response = yield client.fetch('http://localhost:8000/one')
        self.write("%s plus Two" % response.body)


class Test(tornado.testing.AsyncTestCase):

    @classmethod
    def setUpClass(cls):
        super(Test, cls).setUpClass()
        cls.cc = cc.CommandCenter('goliath', redis_port=5872)
        cls.ws = ws.WorkServer('mengsk', redis_port=2398)

    def setUp(self):
        super(Test, self).setUp()
        self.cc_http_port = 9028
        self.ws_http_port = 8342
        self.cc.listen(self.cc_http_port, io_loop=self.io_loop)
        self.ws.listen(self.ws_http_port, io_loop=self.io_loop)

    @tornado.testing.gen_test
    def test_register_ws(self):
        client = tornado.httpclient.AsyncHTTPClient(io_loop=self.io_loop)
        response = yield client.fetch('https://google.com')
        print(response.code)

    @classmethod
    def tearDown(cls):
        super(Test, cls).tearDownClass()
        cls.cc.db.flushdb()
        cls.cc.shutdown_redis()
        cls.ws.db.flushdb()
        cls.ws.shutdown_redis()
        folders = ['streams', 'targets']
        for folder in folders:
            if os.path.exists(folder):
                shutil.rmtree(folder)

if __name__ == '__main__':
    unittest.main()