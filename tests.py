import ws
import redis
import tornado.ioloop
from tornado.testing import AsyncHTTPTestCase
import unittest
import subprocess
import requests
import json
import time

class HandlerTestCase(AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        ''' Start a single server for all test cases '''
        redis_port = str(6827)
        self.redis_client = ws.init_redis(redis_port)
        self.increment = 14
        self.app = tornado.web.Application([
                        (r'/frame', ws.FrameHandler),
                        (r'/stream', ws.StreamHandler),
                        (r'/heartbeat', ws.HeartbeatHandler, dict(increment=self.increment))
                        ])
        super(AsyncHTTPTestCase, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        ''' Destroy the server '''
        self.redis_client.shutdown()
        tornado.ioloop.IOLoop.instance().stop()
        super(AsyncHTTPTestCase, self).tearDownClass()

    def setUp(self):
        #pcb = tornado.ioloop.PeriodicCallback(ws.check_heartbeats, 10000, 
        #                            tornado.ioloop.IOLoop.instance())
        #pcb.start()
        super(HandlerTestCase, self).setUp()

    def get_app(self):
        return self.app

    def test_heartbeat(self):
        print 'test_heartbeat'
        self.redis_client.flushdb()
        token_id = '3u293e48'
        stream_id = 'n20fj3ma'
        self.redis_client.set('shared_token:'+token_id+':stream', stream_id)
        start_time = time.time()
        response = self.fetch('/heartbeat', method='POST',
                            body=json.dumps({'shared_token' : token_id}))
        hb = self.redis_client.zscore('heartbeats',stream_id)
        self.assertAlmostEqual(start_time+self.increment, hb, places=1)
        print hb-(start_time+self.increment)

    def test_expire(self):
        print 'test expire'

    def tearDown(self):
        print 'TEARING DOWN'

if __name__ == '__main__':
    unittest.main()