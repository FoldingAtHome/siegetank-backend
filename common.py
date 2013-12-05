import redis
import subprocess
import sys

class RedisMixin():
    def init_redis(self, redis_port, redis_pass=None):
        redis_port = str(redis_port)
        args = ("redis/src/redis-server", "--port", redis_port)
        redis_process = subprocess.Popen(args)
        if redis_process.poll() is not None:
            print 'COULD NOT START REDIS-SERVER, aborting'
            sys.exit(0)
        ws_redis = redis.Redis(host='localhost',password=redis_pass,
                               port=int(redis_port))
        # wait until redis is alive
        alive = False
        while not alive:
            try:
                alive = ws_redis.ping() 
            except:
                pass
        return ws_redis

    def get_db(self):
        return self.db

    def shutdown_redis(self):
        print 'shutting down redis...'
        self.db.shutdown()