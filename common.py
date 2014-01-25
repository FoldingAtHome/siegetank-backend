import redis
import subprocess
import sys
import time
import tornado


def sum_time(time):
    return int(time[0])+float(time[1])/10**6


def init_redis(redis_port, redis_pass=None):
    ''' Spawn a redis subprocess port and returns a redis client.

        Parameters:
        redis_port - port of the redis server
        redis_pass - authenticate token. All other cilents must use
                     this token before they can send messages

    '''
    redis_port = str(redis_port)
    args = ["redis/src/redis-server", "--port", redis_port]
    if redis_pass:
        args.append('--requirepass')
        args.append(str(redis_pass))
    redis_process = subprocess.Popen(args)

    if redis_process.poll() is not None:
        print('Could not start redis server, aborting')
        sys.exit(0)
    redis_client = redis.Redis(host='localhost', password=redis_pass,
                               port=int(redis_port), decode_responses=True)
    # poll until redis server is alive
    alive = False
    start_time = time.time()
    while time.time()-start_time < 15.0:
        try:
            alive = redis_client.ping()
            break
        except Exception:
            pass
    if not alive:
        raise ValueError('Could not start redis')

    return redis_client


class RedisMixin():

    def shutdown_redis(self):
        print('shutting down redis...')
        self.db.shutdown()
        self.db.connection_pool.disconnect()
        #del self.db
        #self.db.reset()

    def shutdown(self, signal_number=None, stack_frame=None, kill=True):
        print('shutting down server...')
        self.shutdown_redis()
        if kill:
            print('stopping tornado ioloop...')
            tornado.ioloop.IOLoop.instance().stop()
            sys.exit(0)
