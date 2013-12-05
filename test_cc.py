import cc
import hashlib
import redis
import tornado.ioloop
from tornado.testing import AsyncHTTPTestCase
import unittest
import subprocess
import json
import time
import uuid
import base64
import os
import random
import struct
import requests
import shutil
import cStringIO
import tarfile
import sys

class TestWSRegistration(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        self.cc         = cc.CommandCenter('eisley','2398')
        self.auth_token = hashlib.md5(str(uuid.uuid4())).hexdigest()
        self.registrar  = tornado.web.Application([
            (r"/register_ws",cc.RegisterWSHandler,
            dict(cc=self.cc, cc_auth_pass=self.auth_token))])
        tornado.httpserver.HTTPServer(self.cc)
        tornado.httpserver.HTTPServer(self.registrar)
        super(AsyncHTTPTestCase, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.cc.shutdown_redis()
        super(AsyncHTTPTestCase, self).tearDownClass()

    def get_app(self):
        return self.registrar

    def test_registration(self):
        self.io_loop 

        ws_name = 'firebat'
        http_port = '80'
        redis_port = '423'
        redis_pass = 'ramanujan'

        test_body = json.dumps({
            'name'       : ws_name, 
            'http_port'  : http_port, 
            'redis_port' : redis_port, 
            'redis_pass' : redis_pass, 
            'auth_pass'  : self.auth_token
            })
        resp = self.fetch('/register_ws',method='POST',body=test_body)
        self.assertEqual(resp.code,200)


        self.assertTrue(self.cc.db.sismember('active_ws','firebat'))
        self.assertTrue(
            self.cc.db.hget('ws:'+ws_name,':http_port') == http_port and
            self.cc.db.hget('ws:'+ws_name,':redis_port') == redis_port and
            self.cc.db.hget('ws:'+ws_name,':redis_pass') == redis_pass)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)