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

class WSHandlerTestCase(AsyncHTTPTestCase):

    @classmethod
    def setUpClass(self):
        ''' Start a single server for all test cases '''
        redis_port = str(6827)
        self.redis_client = ws.init_redis(redis_port)
        # Use a single DB session
        self.redis_client.flushdb()
        self.increment = 3
        super(AsyncHTTPTestCase, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        ''' Destroy the server '''
        self.redis_client.flushdb()
        self.redis_client.shutdown()
        tornado.ioloop.IOLoop.instance().stop()
        super(AsyncHTTPTestCase, self).tearDownClass()

    def get_app(self):
        return tornado.web.Application([
                        (r'/frame', ws.FrameHandler),
                        (r'/stream', ws.StreamHandler),
                        (r'/heartbeat', ws.HeartbeatHandler, 
                                        dict(increment=self.increment))
                        ])

    def test_get_frame(self):
        # Add a stream
        system_bin      = str(uuid.uuid4())
        state_bin       = str(uuid.uuid4())
        integrator_bin  = str(uuid.uuid4())
       
        # Test send binaries of system.xml and integrator
        files = {
            'state_bin' : state_bin,
            'system_bin' : system_bin,
            'integrator_bin' : integrator_bin
        }
        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)
        self.assertEqual(resp.code, 200)

        stream_id = resp.body
        token_id = str(uuid.uuid4())

        # Mimic what the CC would do to prepare a stream as a job
        self.redis_client.sadd('active_streams',stream_id)
        self.redis_client.hset('active_stream:'+stream_id, 
                               'shared_token', token_id)
        self.redis_client.hset('active_stream:'+stream_id, 
                               'donor', 'proteneer')  
        self.redis_client.hset('active_stream:'+stream_id, 
                               'start_time', time.time())
        self.redis_client.hset('active_stream:'+stream_id, 
                               'steps', 0)
        self.redis_client.sadd('shared_tokens',token_id)
        self.redis_client.set('shared_token:'+token_id+':stream', stream_id)
        self.redis_client.zadd('heartbeats',stream_id,time.time()+20)
        headers  = {'shared_token' : token_id}
        response = self.fetch('/frame', headers=headers, method='GET')  
        tarball  = tarfile.open(mode='r',
                                fileobj=cStringIO.StringIO(response.body))
        for member in tarball.getmembers():
            if member.name == 'system.xml.gz':
                self.assertEqual(tarball.extractfile(member).read(),
                                 system_bin)
            if member.name == 'state.xml.gz':
                self.assertEqual(tarball.extractfile(member).read(),
                                 state_bin)
            if member.name == 'integrator.xml.gz':
                self.assertEqual(tarball.extractfile(member).read(),
                                 integrator_bin)

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

        self.assertFalse(
            self.redis_client.sismember('active_streams',stream_id) and
            self.redis_client.hvals('active_stream:'+stream_id))

    def test_post_stream(self):
        if not os.path.exists('files'):
            os.makedirs('files')
        if not os.path.exists('streams'):
            os.makedirs('streams')
        
        system_bin     = 'system.xml.gz'
        state_bin      = 'state.xml.gz'
        integrator_bin = 'integrator.xml.gz'
        system_hash = hashlib.md5(system_bin).hexdigest()
        integrator_hash = hashlib.md5(integrator_bin).hexdigest()

        # Test send binaries of system.xml and integrator
        files = {
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
                                         stream_id1,'state.xml.gz')))

        # Test send hashes of existing files
        files = { 
            'state_bin' : state_bin,
            'system_hash' : system_hash,
            'integrator_hash' : integrator_hash
        }
        prep = requests.Request('POST','http://url',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)
        stream_id2 = resp.body
        self.assertEqual(resp.code, 200)
        server_streams = self.redis_client.smembers('streams')
        self.assertTrue(stream_id1 in server_streams)
        self.assertTrue(stream_id2 in server_streams)

        # Test send one hash one bin
        files = { 
            'state_bin' : state_bin,
            'system_hash' : system_hash,
            'integrator_bin' : integrator_bin
        }
        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)
        stream_id3 = resp.body
        self.assertEqual(resp.code, 200)
        server_streams = self.redis_client.smembers('streams')
        self.assertTrue(stream_id1 in server_streams)
        self.assertTrue(stream_id2 in server_streams)
        self.assertTrue(stream_id3 in server_streams)

        # Verify integrity of file
        self.assertTrue(
            self.redis_client.sismember('file_hashes',system_hash) and 
            self.redis_client.sismember('file_hashes',integrator_hash))

        system_bin_read = open(os.path.join('files',system_hash)).read()
        integ_bin_read = open(os.path.join('files',integrator_hash)).read()
        self.assertEqual(system_bin_read, system_bin)
        self.assertEqual(integ_bin_read, integrator_bin)

        stream_ids = [stream_id1, stream_id2, stream_id3]
        # verify redis entries
        for stream in stream_ids:
            frame_count = self.redis_client.hget('stream:'+stream, 'frames')
            state = self.redis_client.hget('stream:'+stream, 'state')
            test_system_hash = self.redis_client.hget('stream:'+stream,
                                                 'system_hash')
            test_integrator_hash = self.redis_client.hget('stream:'+stream,
                                                 'integrator_hash')
            self.assertEqual(state,'0')
            self.assertEqual(frame_count,'0')
            self.assertEqual(system_hash, test_system_hash)
            self.assertEqual(integrator_hash, test_integrator_hash)

        for stream in stream_ids:
            state_bin_read = open(os.path.join('streams',stream,
                                  'state.xml.gz')).read()
            self.assertEqual(state_bin_read, state_bin)
        
        for stream in stream_ids:
            shutil.rmtree(os.path.join('streams',stream))
        os.remove(os.path.join('files',system_hash))
        os.remove(os.path.join('files',integrator_hash))

    def test_bad_post_stream(self):
        system_bin     = 'system.xml.gz'
        state_bin      = 'state.xml.gz'
        integrator_bin = 'integrator.xml.gz'
        system_hash = hashlib.md5(system_bin).hexdigest()
        integrator_hash = hashlib.md5(integrator_bin).hexdigest()
        # Missing integrator
        message = {
            'frame_format' : 'xtc',
            'system_hash' : system_hash,
        }
        files = { 
            'json' : json.dumps(message),
            'state_bin' : state_bin,
        }
        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)

        self.assertEqual(resp.code, 400)

        # Missing state
        message = {
            'frame_format' : 'xtc',
            'system_hash' : system_hash,
        }
        files = { 
            'json' : json.dumps(message),
            'integrator_bin' : integrator_bin
        }
        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)
        self.assertEqual(resp.code, 400)

    def test_get_stream(self):
        

if __name__ == '__main__':
    unittest.main()