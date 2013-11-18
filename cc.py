import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.auth

import datetime
import hashlib
import json
import os
import uuid
import random

import redis

cc_redis = redis.Redis(host='localhost', port=6379)

class TargetHandler(tornado.web.RequestHandler):
    def post(self):
        ''' PGI: Post a new target '''
        content = json.loads(self.request.body)
        try:
            system = content['system']
            system_sha = hashlib.sha256(system).hexdigest()
            path = './files/'+system_sha
            if not os.path.isfile(path):
                open(path,'w').write(system)
            integrator = content['integrator']
            integrator_sha = hashlib.sha256(integrator).hexdigest()
            path = './files/'+integrator_sha
            if not os.path.isfile(path):
                open(path,'w').write(integrator)
            description = content['description']
            creation_time = cc_redis.time()[0]
            target_id = hashlib.sha256(str(uuid.uuid4())).hexdigest()
            owner = 'yutong'

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

        return self.write('OK')

    def get(self):
        user = 'yutong'
        response = []
        targets = cc_redis.smembers(user+':targets')
        for target_id in targets:
            prop = {}

            stamp = datetime.datetime.fromtimestamp(float(cc_redis.hget(target_id,'date')))
            prop['date'] = stamp.strftime('%m/%d/%Y, %H:%M:%S')
            prop['description'] = cc_redis.hget(target_id,'description')
            prop['frames'] = random.randint(0,200)
            response.append(prop)

        return self.write(json.dumps(response,indent=4, separators=(',', ': ')))

    def delete(self):
        # delete all streams

        # delete the project
        return

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
    #cc_redis.flushdb()
    application.listen(8888)
    if not os.path.exists('files'):
        os.makedirs('files')
    tornado.ioloop.IOLoop.instance().start()