import ws
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

class WSHandlerTestCase(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        ''' Start a single server for all test cases '''
        redis_port = str(6827)
        self.redis_client = ws.init_redis(redis_port)
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

    def test_add_stream(self):
        if not os.path.exists('files'):
            os.makedirs('files')
        if not os.path.exists('streams'):
            os.makedirs('streams')
        
        stream_id = str(uuid.uuid4())

        system_bin     = 'system.xml.tar.gz'
        state_bin      = 'state.xml.tar.gz'
        integrator_bin = 'integrator.xml.tar.gz'

        body = {
            'stream_id'      : stream_id,
            'system_b64'     : system_bin,
            'state_b64'      : state_bin,
            'integrator_b64' : integrator_bin,
        }

        print self.fetch('/stream', method='POST', body=json.dumps(body))

if __name__ == '__main__':
    unittest.main()