import us
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
import signal
import sys

class USInterfaceTestCase(AsyncHTTPTestCase):
    ''' This class tests the basic interface of the US to ensure DB entries
        are correctly setup, and that replies are valid. '''
    @classmethod
    def setUpClass(self):
        redis_port = str(3829)
        self.us    = us.UserServer('test_server',redis_port)
        super(AsyncHTTPTestCase, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.us.shutdown_redis()
        super(AsyncHTTPTestCase, self).tearDownClass()


    def get_app(self):
        return self.us

    def test_add_user(self):
        name = str(uuid.uuid4())
        password = 'hehe'
        email = 'ramanujan@ramanujan.com'
        payload = json.dumps({
            'username' : name,
            'password' : password,
            'email'    : email
        })
        rep = self.fetch('/user',method='POST',body=payload)
        self.assertEqual(rep.code,200)
        self.assertTrue(self.us.db.exists('user:'+name))
        self.assertEqual(self.us.db.hget('user:'+name,'password'),password)
        self.assertEqual(self.us.db.hget('user:'+name,'email'),email)
        # see if adding two users with same name dies
        rep = self.fetch('/user',method='POST',body=payload)
        self.assertEqual(rep.code,400)

        return name,password

    def test_auth_user(self):
        username,password = self.test_add_user()
        print username,password
        payload = json.dumps({
            'username' : username,
            'password' : password
        })
        rep = self.fetch('/auth',method='POST',body=payload)
        token = rep.body
        self.assertEqual(rep.code,200)
        self.assertEqual(token,self.us.db.hget('user:'+username,'token'))
        self.assertEqual(self.us.db.get('token:'+token+':user'),username)

        # auth again to make sure the old token is deleted
        rep = self.fetch('/auth',method='POST',body=payload)
        new_token = rep.body
        self.assertEqual(new_token,self.us.db.hget('user:'+username,'token'))
        self.assertEqual(self.us.db.get('token:'+new_token+':user'),username)
        self.assertEqual(rep.code,200)
        self.assertFalse(self.us.db.get('token:'+token+':user'),username)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)