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

class WSHandlerTestCase(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        ''' Start a single server for all test cases '''
        redis_port = str(6827)
        self.redis_client = ws.init_redis(redis_port)
        # Use a single DB session
        self.redis_client.flushdb()
        self.increment = 3
        self.app = tornado.web.Application([
                        (r'/frame', ws.FrameHandler),
                        (r'/stream', ws.StreamHandler),
                        (r'/heartbeat', ws.HeartbeatHandler, 
                                        dict(increment=self.increment))
                        ])
        super(AsyncHTTPTestCase, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        ''' Destroy the server '''
        self.redis_client.shutdown()
        tornado.ioloop.IOLoop.instance().stop()
        super(AsyncHTTPTestCase, self).tearDownClass()

    def get_app(self):
        return self.app

    def test_heartbeat(self):
        token_id = str(uuid.uuid4())
        stream_id = str(uuid.uuid4())
        self.redis_client.set('shared_token:'+token_id+':stream', stream_id)

        # test sending request to uri: /heartbeat extends the expiration time
        for iteration in range(10):
            start_time = time.time()
            response = self.fetch('/heartbeat', method='POST',
                                body=json.dumps({'shared_token' : token_id}))
            hb = self.redis_client.zscore('heartbeats',stream_id)
            self.assertAlmostEqual(start_time+self.increment, hb, places=1)

        # test expirations
        response = self.fetch('/heartbeat', method='POST',
                    body=json.dumps({'shared_token' : token_id}))
        self.redis_client.sadd('active_streams',stream_id)
        self.redis_client.hset('active_stream:'+stream_id, 
                               'shared_token', token_id)
        time.sleep(self.increment+1)
        ws.check_heartbeats() 

        self.assertFalse(self.redis_client.smembers('active_streams'))
        self.assertFalse(self.redis_client.hvals('active_stream:'+stream_id))

    def test_post_stream(self):
        if not os.path.exists('files'):
            os.makedirs('files')
        if not os.path.exists('streams'):
            os.makedirs('streams')
        
        system_bin     = 'system.xml.tar.gz'
        state_bin      = 'state.xml.tar.gz'
        integrator_bin = 'integrator.xml.tar.gz'

        system_hash = hashlib.md5(system_bin).hexdigest()
        integrator_hash = hashlib.md5(integrator_bin).hexdigest()

        # Test 1. Send binaries of system.xml and integrator
        message = {
            'frame_format' : 'xtc'
        }

        files = {
            'json' : json.dumps(message),
            'state_bin' : state_bin,
            'system_bin' : system_bin,
            'integrator_bin' : integrator_bin
        }

        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)

        self.assertEqual(resp.code, 200)

        stream_id1 = resp.body

        self.assertTrue(
            self.redis_client.sismember('file_hashes',system_hash) and 
            self.redis_client.sismember('file_hashes',integrator_hash) and
            os.path.exists(os.path.join('files',system_hash)) and
            os.path.exists(os.path.join('files',integrator_hash)) and 
            os.path.exists(os.path.join('streams',
                                         stream_id1,'state.xml.tar.gz')))

        # Test 2. Send hashes of existing files
        message = {
            'frame_format' : 'xtc',
            'system_hash' : system_hash,
            'integrator_hash' : integrator_hash
        }

        files = { 
            'json' : json.dumps(message),
            'state_bin' : state_bin
        }

        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)

        print resp.code

        stream_id2 = resp.body

        server_streams = self.redis_client.smembers('streams')
        print server_streams
        self.assertTrue(stream_id1 in server_streams)
        self.assertTrue(stream_id2 in server_streams)

        os.remove(os.path.join('files',system_hash))
        os.remove(os.path.join('files',integrator_hash))
        shutil.rmtree(os.path.join('streams',stream_id1))
        shutil.rmtree(os.path.join('streams',stream_id2))

if __name__ == '__main__':
    unittest.main()