import redis
import subprocess
import sys
import time
import redis

# Todo pipelines.. (yuck)

class HashSet(object):
    
    _rc   = None
    _rmaps = []
    
    @classmethod
    def set_redis(cls,rc):
        cls._rc = rc

    @classmethod
    def exists(cls,id):
        return cls._rc.sismember(cls._prefix+'s',id)
    
    @classmethod
    def create(cls,id):
        if cls.exists(id):
            raise KeyError(id,'already exists')
        cls._rc.sadd(cls._prefix+'s',id)
                                      
    @classmethod
    def delete(cls,id):
        if not cls.exists(id):
            raise KeyError('key not found')
        cls._rc.srem(cls._prefix+'s',id)
        # cleanup rmap first
        print 'BAR', cls._rmaps
        for field in cls._rmaps:
            rmap_id = cls._rc.hget(cls._prefix+':'+id, field)
            if rmap_id:
                cls._rc.delete(field+':'+rmap_id+':'+cls._prefix)
        cls._rc.delete(cls._prefix+':'+id)
        
    @classmethod
    def instance(cls,id):
        return cls(id)
    
    @classmethod
    def rmap(cls,field,id):
        if not field in cls._fields:
            raise KeyError('invalid field')
        if not field in cls._rmaps:
            raise KeyError('key not rmapped')
        return cls._rc.get(field+':'+id+':'+cls._prefix)
    
    def hincrby(self, attr, count=1):
        if not attr in self.__class__._fields:
            raise KeyError('invalid field')
        if not self.__class__._fields[attr] is int:
            raise TypeError('can only increment ints')
        return self.__class__._rc.hincrby(self.__class__._prefix+':'+self._id,attr,count)

    def __init__(self,id):
        if not self.__class__.exists(id):
            raise KeyError(id,'has not been created yet')
        self.__dict__['_id'] = id

    def __getattr__(self, attr):
        if not attr in self._fields:
            raise KeyError('invalid field')
        # get value then type cast
        return self.__class__._fields[attr](self.__class__._rc.hget(self.__class__._prefix+':'+self._id, attr))
    
    def __setattr__(self, attr, value):
        if not attr in self._fields:
            raise KeyError('invalid field')
        if not isinstance(value,self._fields[attr]):
            raise TypeError('expected',self.__class__._fields[attr],'got',type(value))
        if attr in self.__class__._rmaps:
            self.__class__._rc.set(attr+':'+value+':'+self.__class__._prefix,self._id)
        self.__class__._rc.hset(self.__class__._prefix+':'+self._id, attr, value)

class RedisMixin():
    def init_redis(self, redis_port, redis_pass=None):
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
            print 'Could not start redis server, aborting'
            sys.exit(0)
        redis_client = redis.Redis(host='localhost',password=redis_pass,
                               port=int(redis_port))
        # poll until redis server is alive
        alive = False
        start_time = time.time()
        while time.time()-start_time < 15.0:
            try:
                alive = redis_client.ping()
                break 
            except Exception as e:
                pass
        if not alive:
            raise ValueError('Could not start redis')

        return redis_client

    def get_db(self):
        return self.db

    def shutdown_redis(self):
        print 'shutting down redis...'
        self.db.shutdown()

    