import ws
import redis
import tornado.ioloop
from tornado.testing import AsyncHTTPTestCase
import unittest
import subprocess
import requests

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
        #self.test_app.listen(self.http_port, '0.0.0.0')
        pcb = tornado.ioloop.PeriodicCallback(ws.check_heartbeats, 10000, 
                                    tornado.ioloop.IOLoop.instance())
        pcb.start()
        super(BasicHandlerTestCase, self).setUp()

    def get_app(self):
        return self.app

    def test_app(self):
        print 'TESTING APP'

    def tearDown(self):
        print 'TEARING DOWN'
        self.ws_redis.shutdown()
        tornado.ioloop.IOLoop.instance().stop()
'''
class BasicHandlerTestCase(unittest.TestCase):
    def setUp(self):
        print 'SETTING UP'
        
        tornado.ioloop.IOLoop.instance().start()


    def test_heartbeat(self):
        print 'testing heartbeat'
        r = requests.get('https://localhost:'+self.http_port+'/heartbeat')

    def tearDown(self):
'''
if __name__ == '__main__':
    unittest.main()