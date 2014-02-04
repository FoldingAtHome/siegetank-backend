import redis
import subprocess
import sys
import time
import tornado
import ipaddress
import os


def sum_time(time):
    return int(time[0])+float(time[1])/10**6


def is_domain(url):
    """ Returns True if url is a domain """
    # if a port is present in the url, then we extract the base part
    base = url.split(':')[0]
    try:
        ipaddress.ip_address(base)
        return False
    except Exception:
        return True


def init_redis(redis_port, redis_pass=None,
               appendonly=False, appendfilename=None):
    ''' Spawn a redis subprocess port and returns a redis client.

        Parameters:
        redis_port - port of the redis server
        redis_pass - authenticate token. All other cilents must use
                     this token before they can send messages

    '''
    redis_port = str(redis_port)
    redis_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                              '..', 'redis', 'src', 'redis-server')
    args = [redis_path, "--port", redis_port]
    if appendonly:
        args.append('--appendonly')
        args.append('yes')
        if appendfilename:
            args.append('--appendfilename')
            args.append(appendfilename)
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
