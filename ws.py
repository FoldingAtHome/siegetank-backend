import tornado.escape
import tornado.ioloop
import tornado.web
import redis

import uuid
import random
import threading
import os
import json
# each ws has a uuid

ws_redis = redis.Redis(host='localhost', port=sys.argv[1])

class FrameHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PUBLIC - Used by Core to add a frame
            POST parameters:

            target_id: uuid #
            stream_id: uuid #
            frame_bin: frame.pb # protocol buffered frame
        '''
        print 'foo'

class StreamHandler(tornado.web.RequestHandler):
    def post(self):       
        ''' PRIVATE - Add a new stream to WS. The POST method on this URI
            can only be accessed by known CCs (ip restricted) 
            Parameters:

            target_id: uuid # 
            stream_id: uuid #
            state_bin: state.xml.tar.gz #

            stored as:
            /targets/target_id/stream_id/state.xml.tar.gz
        '''

        content = json.loads(self.request.body)

        try:
            target_id = content['target_id']
            stream_id = content['stream_id']
            state_bin = content['state_bin']

            path = 'targets/'+target_id+'/'+stream_id

            if not os.path.exists(path):
                try:
                    os.makedirs(path)
                except:
                    pass

            open(path+'/'+'state.xml.tar.gz','w').write(state_bin)

            ws_redis.sadd('targets', target_id)
            ws_redis.sadd('target:'+target_id+':streams', stream_id)
            ws_redis.set('stream:'+stream_id+':frames', 0)
            ws_redis.set('stream:'+stream_id+':target', target_id)

        except Exception as e:
            print e
            return self.write('bad request')

        return self.write('OK')

    def get(self):
        ''' PRIVATE - Assign a job. 
            The CC creates a token given to the Core for identification
            purposes.

            Parameters:

            target_id: uuid #
            stream_id: uuid #

            RESPONDS with a state.xml '''

        content = json.loads(self.request.body)

        pipe = ws_redis.pipeline()
        pipe.zrevrange('target:'+target_id+':queue',0,0)
        pipe.zremrangebyrank('target:'+target_id+':queue',-1,-1)
        result = pipe.execute()
        head = result[0]

        if head:
            stream_id = head[0]
            ws_redis.set('expire:'+stream_id,0)
            ws_redis.expire('expire:'+stream_id,5)
            return self.write('Popped stream_id:' + stream_id)
        else:
            return self.write('No jobs available!')

class QueueHandler(tornado.web.RequestHandler):
    def get(self):
        ''' PRIVATE - Return the idle stream for a given project with the most number of frames '''
        content = json.loads(self.request.body)
        target_id = content['target_id']
        stream_id = ws_redis.zrevrange('target:'+target_id+':queue',0,0)
        return self.write(stream_id)

class Listener(threading.Thread):
    ''' This class subscribes to the ws redis server to listen for expire notifications. Upon a stream expiring,
        a notification is sent, and the stream is added back into the queue whose score is equal to the number
        of frames
    '''
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
                target_id = ws_redis.get('stream:'+stream_id+':target', target_id)
                score = ws_redis.get('stream:'+stream_id+':frames')
                print stream_id, target_id, score
                ws_redis.zadd('target:'+target_id+':queue', stream_id, score)
            except TypeError as e:
                pass

application = tornado.web.Application([
    (r'/frame', FrameHandler),
    (r'/stream', StreamHandler),
    (r'/queue', QueueHandler)
])

if __name__ == "__main__":

    if not os.path.exists('targets'):
        os.makedirs('targets')

    # redis routines
    ws_redis.flushdb()
    ws_redis.config_set('notify-keyspace-events','Elx')
    '''
    for i in range(10):
        stream_id = str(uuid.uuid4())
        ws_redis.sadd('streams', stream_id)
        ws_redis.set('stream:'+stream_id+':frames', random.randint(0,10))

    streams = ws_redis.smembers('streams')
    for stream_id in streams:
        score = ws_redis.get('stream:'+stream_id+':frames')
        print stream_id, score
        ws_redis.zadd('queue', stream_id, score)
    '''
    queue_updater = Listener(ws_redis, ['__keyevent@0__:expired'])
    # needed for child thread to exit when main thread terminates
    queue_updater.daemon = True
    queue_updater.start()

    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()