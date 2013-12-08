import cc
import ws
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

# Tests
# User registration
# Workserver roundtrip

# FAH user

class TestWSRegistration(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        self.cc         = cc.CommandCenter('eisley','2398')
        self.auth_token = hashlib.md5(str(uuid.uuid4())).hexdigest()
        self.registrar  = tornado.web.Application([
            (r"/register_ws",cc.RegisterWSHandler,
            dict(cc=self.cc, cc_auth_pass=self.auth_token))])
        super(AsyncHTTPTestCase, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.cc.shutdown_redis()
        super(AsyncHTTPTestCase, self).tearDownClass()

    def get_app(self):
        return self.registrar

    def test_registration(self):
        ws_name = 'firebat'
        ws_http_port = '80'
        ws_redis_port = '27390'
        ws_redis_pass = hashlib.md5(str(uuid.uuid4())).hexdigest()
        workserver = ws.WorkServer(ws_name,ws_redis_port,ws_redis_pass)
        test_body = json.dumps({
            'name'       : ws_name, 
            'http_port'  : ws_http_port, 
            'redis_port' : ws_redis_port, 
            'redis_pass' : ws_redis_pass, 
            'auth_pass'  : self.auth_token
            })
        resp = self.fetch('/register_ws',method='POST',body=test_body)
        self.assertEqual(resp.code,200)
        self.assertTrue(self.cc.db.sismember('active_ws','firebat'))
        self.assertTrue(
            self.cc.db.hget('ws:'+ws_name,':http_port') == ws_http_port and
            self.cc.db.hget('ws:'+ws_name,':redis_port') == ws_redis_port and
            self.cc.db.hget('ws:'+ws_name,':redis_pass') == ws_redis_pass)
        test_r_message = 'A MESSAGE'
        self.cc.ws_dbs[ws_name].set('Test',test_r_message   )
        self.assertEqual(workserver.db.get('Test'),test_r_message)
        workserver.shutdown_redis()
        return ws_name

class TestCCMethods(AsyncHTTPTestCase):
    @classmethod
    def post(self):
        self.cc         = cc.CommandCenter('hoth','2438')
        self.auth_token = hashlib.md5(str(uuid.uuid4())).hexdigest()
        self.registrar  = tornado.web.Application([
        super(AsyncHTTPTestCase, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.cc.shutdown_redis()
        super(AsyncHTTPTestCase, self).tearDownClass()

    def get_app(self):
        return self.cc

    def test_post_target(self):
        async_client = tornado.tornado.httpclient.AsyncHTTPClient()
        resp = self.fetch(

class TestStream(AsyncHTTPTestCase):
    ''' Test and see if we can send a stream to the CC, which then gets
        routed to the WS. '''
    @classmethod
    def setUpClass(self):
        self.cc     = cc.CommandCenter()



if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)