# Class methods must explicitly pass in a db argument
from functools import wraps
def check_field(func):
    @wraps(func)
    def _wrapper(self_cls, field, *args, **kwargs):
        if not field in self_cls._fields:
            raise KeyError('invalid field')
        return func(self_cls, field, *args, **kwargs)
    return _wrapper

class HashSet(object):
    
    _rmaps = []
    
    @classmethod
    def exists(cls,id,db):
        return db.sismember(cls._prefix+'s',id)
    
    @classmethod
    def create(cls,id,db):
        if cls.exists(id,db):
            raise KeyError(id,'already exists')
        db.sadd(cls._prefix+'s',id)
                                      
    @classmethod
    def delete(cls,id,db):
        if not cls.exists(id,db):
            raise KeyError('key ',id,' not found')
        db.srem(cls._prefix+'s',id)
        # cleanup rmap first
        for field in cls._rmaps:
            rmap_id = db.hget(cls._prefix+':'+id, field)
            if rmap_id:
                db.delete(field+':'+rmap_id+':'+cls._prefix)
        # cleanup hash
        db.delete(cls._prefix+':'+id)
        # cleanup sets
        for f_name, f_type in cls._fields:
            if f_type is set:
                db.delete(self.__class__._prefix+':'+self._id+':'+field)

    @classmethod
    def members(cls,db):
        return cls._rc.smembers(cls._prefix+'s')

    @classmethod
    def instance(cls,id,db):
        return cls(id,db)
    
    @classmethod
    @check_field
    def rmap(cls,field,id,db):
        if not field in cls._fields:
            raise KeyError('invalid field')
        if not field in cls._rmaps:
            raise KeyError('key not rmapped')
        return db.get(field+':'+id+':'+cls._prefix)

    # Item specific methods
    @check_field
    def sadd(self, field, value):
        
        return

    @check_field
    def sismember(self, field, value):
        return

    @check_field
    def sdel(self, field, value):
        return
    
    @check_field
    def hincrby(self, field, count=1):
        return self._db.hincrby(self.__class__._prefix+':'+self._id,field,count)

    @check_field
    def __init__(self,id,db):
        self._db = db
        if not self.__class__.exists(id,db):
            raise KeyError(id,'has not been created yet')
        self.__dict__['_id'] = id
       
    @check_field 
    def __getitem__(self, field):
        if isinstance(self._fields[field],set):
            return self._db.smembers(self.__class__._prefix+':'+self._id+':'+field)
        else:
        # get value then type cast
            return self.__class__._fields[field](self._db.hget(self.__class__._prefix+':'+self._id, field))
    
    @check_field
    def __setitem__(self, field, value):
        if not isinstance(value,self._fields[field]):
            raise TypeError('expected',self.__class__._fields[field],'got',type(value))
        
        # add support for sets
        if isinstance(self._fields[field],set):
            if field in self.__class__._rmaps:
                raise TypeError('rmaps not supported for set types')
            for element in value:
                self._db.sadd(self.__class__._prefix+':'+self._id+':'+field,value)
        elif:
            if field in self.__class__._rmaps:
                self._db.set(field+':'+value+':'+self.__class__._prefix,self._id)
            self._db.hset(self.__class__._prefix+':'+self._id, field, value)