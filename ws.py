import tornado.escape
import tornado.ioloop
import tornado.web
import redis

import uuid
import random
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
            can only be accessed by known CCs (ip restrziction) '''
        print self.request.remote_ip

    def get(self):
        ''' PRIVATE - Get a state.xml from the WS for the core. 
            The CC creates a token given to the Core for identification
            purposes.

            RESPONDS with a state.xml '''

        head = ws_redis.zrevrange('queue',0,0)[0]
        print head
        ws_redis.zrem('queue',head)
        ws_redis.set('expire:'+head,0)
        ws_redis.expire('expire:'+head,5)

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
    
    def run(self):
        for item in self.pubsub.listen():
            print item['data'], 'expired'
            try:
                stream_id = item['data'][7:]
                score = ws_redis.get('stream:'+stream_id+':frames')
                print stream_id, score
                ws_redis.zadd('queue', stream_id, score)
            except:
                pass

application = tornado.web.Application([
    (r'/frame', FrameHandler),
    (r'/stream', StreamHandler),
    (r'/queue', QueueHandler)
])

if __name__ == "__main__":
    ws_redis.flushdb()
    ws_redis.config_set('notify-keyspace-events','Elx')
    for i in range(10):
        stream_id = str(uuid.uuid4())
        ws_redis.sadd('streams', stream_id)
        ws_redis.set('stream:'+stream_id+':frames', random.randint(0,10))

    streams = ws_redis.smembers('streams')
    for stream_id in streams:
        score = ws_redis.get('stream:'+stream_id+':frames')
        print stream_id, score
        ws_redis.zadd('queue', stream_id, score)

    queue_updater = Listener(ws_redis, ['__keyevent@0__:expired'])
    # needed for child thread to exit when main thread terminates
    queue_updater.daemon = True
    queue_updater.start()

    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()