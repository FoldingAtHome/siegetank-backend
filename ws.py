import tornado.escape
import tornado.ioloop
import tornado.web

from datetime import date

# each ws has a uuid

ws_redis = redis.Redis(host='localhost', port=6380)

class FrameHandler(tornado.webRequestHandler):
    def post(self):
        ''' DI - Used to add a frame'''
        print 'foo'

class StreamHandler(tornado.web.RequestHandler):
    def post(self):       
        ''' PGI - Add a new stream to WS. The POST method on this URI
            can only be accessed by known CCs (ip restriction) '''


application = tornado.web.Application([
    (r'/frame', TargetHandler),
    (r'/stream', StreamHandler)
])
 
if __name__ == "__main__":
    #cc_redis.flushdb()
    application.listen(8888)
    if not os.path.exists('files'):
        os.makedirs('files')
    tornado.ioloop.IOLoop.instance().start()