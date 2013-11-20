import tornado.escape
import tornado.ioloop
import tornado.web
import redis

import uuid
import random
import threading
import os
import json
import requests
import sys
import hashlib

ws_port = sys.argv[1]
ws_redis = redis.Redis(host='localhost', port=int(ws_port))


CCs = {'127.0.0.1' : 'PROTOSS_IS_FOR_NOOBS'}

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
        ''' PRIVATE - Add new stream(s) to WS. The POST method on this URI
            can only be accessed by known CCs (ip restricted) 
            Parameters:

            REQUIRED:
            stream_ids: uuid1 #
            state_bin: state.xml.tar.gz  #
            system_hash or system_bin #

            stored as:
            /targets/target_id/stream_id/state.xml.tar.gz
        '''

        if not self.request.remote_ip in CCs:
            print self.request.remote_ip
            self.set_status(401)
            return self.write('not authorized')

        content = json.loads(self.request.body)
        self.set_status(400)
        
        try:
            stream_id = content['stream_id']
            state_bin = content['state_bin']
            path = 'streams/'+stream_id
            if not os.path.exists(path):
                try:
                    os.makedirs(path)
                except:
                    pass
            open(path+'/'+'state.xml.tar.gz','w').write(state_bin)
            required_strings = ['system','integrator']
            for s in required_strings:
                if s+'_bin' in content:
                    binary = content[s+'_bin']
                    bin_hash = hashlib.md5(binary).hexdigest()
                    if not ws_redis.sismember('file_hashes', bin_hash):
                        ws_redis.sadd('file_hashes', bin_hash)
                        open('files/'+bin_hash, 'w').write(binary)
                    else:
                        print 'found duplicate hash'
                elif s+'_hash' in content: 
                    bin_hash = content[s+'_hash']
                    if not ws_redis.sismember('file_hashes', bin_hash):
                        return self.write('Gave me a hash for a file not in files directory')
                else:
                    return self.write('missing content: '+s+'_bin/hash')
                ws_redis.hset('stream:'+stream_id, s, bin_hash)

            ws_redis.hset('stream:'+stream_id, 'owner', content['owner'])
            ws_redis.hset('stream:'+stream_id, 'frames', 0)
            self.set_status(200)

        except Exception as e:
            print e
            return self.write('bad request')

        return

    def get(self):
        ''' PRIVATE - Assign a job. 
            The CC creates a token given to the Core for identification
            purposes.

            Parameters:

            target_id: uuid #
            stream_id: uuid #

            RESPONDS with a state.xml '''



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

    if not os.path.exists('files'):
        os.makedirs('files')
    if not os.path.exists('streams'):
        os.makedirs('streams')

    # inform the CCs that the WS is now online and ready for work

    ws_uuid = 'firebat'
    try:
        for server_address, secret_key in CCs.iteritems():
            payload = {'cc_key' : secret_key, 'ws_id' : ws_uuid, 'redis_port' : ws_port}
            r = requests.post('http://'+server_address+':8888/add_ws', json.dumps(payload))
            print 'r.text', r.text
    except:
        print 'cc is down'

    # clear db
    # ws_redis.flushdb()


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

    application.listen(8889, '0.0.0.0')
    tornado.ioloop.IOLoop.instance().start()