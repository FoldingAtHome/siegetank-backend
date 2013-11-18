import tornado.escape
import tornado.ioloop
import tornado.web
import redis

import threading
# each ws has a uuid

ws_redis = redis.Redis(host='localhost', port=6380)

class FrameHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PUBLIC - Used by Core to add a frame'''
        print 'foo'

class StreamHandler(tornado.web.RequestHandler):
    def post(self):       
        ''' PRIVATE - Add a new stream to WS. The POST method on this URI
            can only be accessed by known CCs (ip restriction) '''
        print self.request.remote_ip

    def get(self):
        ''' PRIVATE - Get a state.xml from the WS for the core. 
            The CC creates a token given to the Core for identification
            purposes.

            RESPONDS with a state.xml '''
        print 'foo'

class QueueHandler(tornado.web.RequestHandler):
    def get(self):
        ''' PRIVATE - Return the idle stream with the most number of frames '''
        print 'foo'

class Listener(threading.Thread):
    def __init__(self, r, channels):
        threading.Thread.__init__(self)
        self.redis = r
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(channels)
    
    def work(self, item):
        print item['channel'], ":", item['data']
    
    def run(self):
        for item in self.pubsub.listen():
            print item['data']

application = tornado.web.Application([
    (r'/frame', FrameHandler),
    (r'/stream', StreamHandler),
    (r'/queue', QueueHandler)
])

if __name__ == "__main__":
    ws_redis.config_set('notify-keyspace-events','Elx')
    queue_updater = Listener(ws_redis, ['__keyevent@0__:expired'])
    queue_updater.daemon = True
    queue_updater.start()

    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()