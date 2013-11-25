import ws
import redis
import tornado.ioloop
from tornado.testing import AsyncHTTPTestCase
import unittest
import subprocess
import requests
import json

class BasicHandlerTestCase(AsyncHTTPTestCase):
    def setUp(self):
        redis_port = 6827
        self.http_port = 7346
        self.ws_redis = ws.init_redis(str(redis_port))
        self.app = tornado.web.Application([
                        (r'/frame', ws.FrameHandler),
                        (r'/stream', ws.StreamHandler),
                        (r'/heartbeat', ws.HeartbeatHandler, 
                                        dict(redis_port=redis_port))
                        ])
        pcb = tornado.ioloop.PeriodicCallback(ws.check_heartbeats, 10000, 
                                    tornado.ioloop.IOLoop.instance())
        pcb.start()
        super(BasicHandlerTestCase, self).setUp()

    def get_app(self):
        return self.app

    def test_heartbeat(self):
        response = self.fetch('/heartbeat', method='POST', body=json({'core_token' : '12345'}))
        print response

    def tearDown(self):
        print 'TEARING DOWN'
        self.ws_redis.shutdown()
        tornado.ioloop.IOLoop.instance().stop()

if __name__ == '__main__':
    unittest.main()