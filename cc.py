from datetime import date
import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.auth

import hashlib
import json
import os

import redis

cc_redis = redis.Redis(host='localhost', port=6379)

class TargetHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PGI: Post a new target '''
        content = json.loads(self.request.body)
        try:
            system = content['system']
            system_sha = hashlib.sha256(system).hexdigest()
            open('./files/'+system_sha,'w').write(system)
            integrator = content['integrator']
            integrator_sha = hashlib.sha256(integrator).hexdigest()
            open('./files/'+integrator_sha,'w').write(integrator)
            description = content['description']
            description_sha = hashlib.sha256(description).hexdigest()
            creation_time = cc_redis.time()[0]
            target_id = hashlib.sha256(system_sha+integrator_sha+\
                                       description_sha+str(creation_time)).hexdigest()
            owner = 'yutong'

            print target_id

            # store data into redis
            cc_redis.hset(target_id,'system',system_sha)
            cc_redis.hset(target_id,'integrator',integrator_sha)
            cc_redis.hset(target_id,'description',description)
            cc_redis.hset(target_id,'date',creation_time)
            cc_redis.hset(target_id,'owner',owner)

            # add id to the owner's list of targets
            cc_redis.sadd(owner+':targets',target_id)

        except Exception as e:
            print str(e)
            return self.write('bad request')

    def get(self):
        ''' Get a list of stream IDs and its properties '''
        print self.request.body

class StreamHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PGI: Add new streams to an existing target. The input must be compressed
            state.xml files encoded as base64 '''
        print self.request.body

application = tornado.web.Application([
    (r'/target', TargetHandler),
    (r'/stream', StreamHandler)
])
 
if __name__ == "__main__":
    cc_redis.flushdb()
    application.listen(8888)
    if not os.path.exists('files'):
        os.makedirs('files')
    tornado.ioloop.IOLoop.instance().start()