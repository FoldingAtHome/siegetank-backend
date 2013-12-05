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
        super(AsyncHTTPTestCase, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.cc.shutdown_redis()
        super(AsyncHTTPTestCase, self).tearDownClass()

    def get_app(self):
        return self.registrar

    def test_foobar(self):
        test_body = json.dumps({
            "name" : "firebat", 
            "http_port":"80", 
            "redis_port":"423", 
            "redis_pass":"ramanujan", 
            "auth_pass" : self.auth_token
            })
        resp = self.fetch('/register_ws',method='POST',body=test_body)
        print resp.code

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)