# Class methods must explicitly pass in a db argument.
# A object relational mapping wrapper for redis.
# Copyright 2012 Yutong Zhao <proteneer@gmail.com>

from functools import wraps


def check_field(func):
    @wraps(func)
    def _wrapper(self_cls, field, *args, **kwargs):
        if not field in self_cls.fields:
            raise TypeError('invalid field')
        return func(self_cls, field, *args, **kwargs)
    return _wrapper

class HashSet(object):
    ''' A HashSet is a class that manages objects stored in redis. A row 
        is represented using a redis hash, and sets. It supports
        reverse mappings, sets, lists, creation and deletion of rows to ensure
        proper cleanup. 

        Currently supported field mappings: int,float,string,sets,

        For example, suppose we wanted to implement a Person db table:

               pkey
        type   str   |      str      |      set     | int | list
        value  name  |      ssn      |      kids    | age | travel_history

        row   'bob'  | '598-20-6839' | 'jane','joe' | 35  | 'usa','canada'  
        row   'jack' | '502-25-4392' | 'eve','abel' | 45  | 'china','africa'

        class Person(HashSet):

            prefix = 'person'
            # name is an implicit id (the primary key)
            fields = {'ssn'            : str,
                       'kids'           : set,
                       'age'            : int,
                       'travel_history' : list
                       }

            lookups = {'ssn'}

            rc = redis.StrictRedis(port=6378)

        bob = Person.create('bob',rc)
        bob['ssn'] = '598-20-6839'
        bob.sadd('kids','jane')
        bob.sadd('kids','joe')
        bob['age'] = 35
        bob.rpush('travel_history','usa')
        bob.rpush('travel_history','canada')

        # reverse lookup - find id of bob given ssn
        Person.lookup('ssn','598-20-6839', rc)

        bob.delete()
        # or Person.delete('bob',rc)
    '''    

    lookups = []

    @classmethod
    def exists(cls,id,db):
        return db.sismember(cls.prefix+'s',id)
    
    @classmethod
    def create(cls,id,db):
        if cls.exists(id,db):
            raise KeyError(id,'already exists')
        db.sadd(cls.prefix+'s',id)
        return cls(id,db)
                                      
    @classmethod
    def delete(cls,id,db):
        if not cls.exists(id,db):
            raise KeyError('key ',id,' not found')
        db.srem(cls.prefix+'s',id)
        # cleanup lookup first
        for field in cls.lookups:
            lookup_id = db.hget(cls.prefix+':'+id, field)
            if lookup_id:
                db.delete(field+':'+lookup_id+':'+cls.prefix)
        # cleanup hash
        db.delete(cls.prefix+':'+id)
        # cleanup sets
        for f_name, f_type in cls.fields.iteritems():
            if f_type is set or f_type is dict:
                db.delete(cls.prefix+':'+id+':'+f_name)

    @classmethod
    def members(cls,db):
        return db.smembers(cls.prefix+'s')

    @classmethod
    def instance(cls,id,db):
        return cls(id,db)
    
    @classmethod
    @check_field
    def lookup(cls,field,id,db):
        if not field in cls.lookups:
            raise KeyError('key not in lookups')
        return db.get(field+':'+id+':'+cls.prefix)

    @check_field
    def sadd(self,field,*values):
        return self._db.sadd(self.__class__.prefix+':'+self._id+':'+field,*values)

    @check_field
    def sismember(self, field, value):
        return self._db.sismember(self.__class__.prefix+':'+self._id+':'+field,value)

    @check_field
    def srem(self, field, *values):
        return self._db.srem(self.__class__.prefix+':'+self._id+':'+field, *values)
    
    @check_field
    def smembers(self,field):
        return self._db.smembers(self.__class__.prefix+':'+self._id+':'+field)
        
    @check_field
    def zadd(self,field,key,score):
        return self._db.zadd(self.__class__.prefix+':'+self._id+':'+field,key,score)

    @check_field
    def zrem(self,field,key):
        return self._db.zrem(self.__class__.prefix+':'+self._id+':'+field,key)

    @check_field
    def hincrby(self, field, count=1):
        return self._db.hincrby(self.__class__.prefix+':'+self._id,field,count)

    def __init__(self,id,db):
        self._db = db
        if not self.__class__.exists(id,db):
            raise KeyError(id,'has not been created yet')
        self.__dict__['_id'] = id
       
    def remove(self):
        self.__class__.delete(self._id, self._db)

    @property
    def id(self):
        return self._id

    @property
    def db(self):
        return self._db
   
    @check_field 
    def __getitem__(self, field):
        if self.fields[field] is set:
            return self._db.smembers(self.__class__.prefix+':'+self._id+':'+field)
        if self.fields[field] is dict:
            return self._db.zrange(self.__class__.prefix+':'+self._id+':'+field, 0, -1)
        else:
        # get value then type cast
            return self.__class__.fields[field](self._db.hget(self.__class__.prefix+':'+self._id, field))
    
    @check_field
    def __setitem__(self, field, value):
        if not isinstance(value,self.fields[field]):
            raise TypeError('expected',self.__class__.fields[field],'got',type(value))  
        # add support for sets
        if isinstance(value,set):
            for element in value:
                self._db.sadd(self.__class__.prefix+':'+self._id+':'+field,element)
        elif isinstance(value,dict):
            for element in value:
                self._db.zadd(self.__class__.prefix+':'+self._id+':'+field,**value)
        else:
            if field in self.__class__.lookups:
                if self._db.exists(field+':'+value+':'+self.__class__.prefix):
                    raise ValueError('FATAL: this value already exists!')
                self._db.set(field+':'+value+':'+self.__class__.prefix,self._id)
            self._db.hset(self.__class__.prefix+':'+self._id, field, value)