# Class methods must explicitly pass in a db argument


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
        if not cls.exists(id):
            raise KeyError('key not found')
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
    def rmap(cls,field,id):
        if not field in cls._fields:
            raise KeyError('invalid field')
        if not field in cls._rmaps:
            raise KeyError('key not rmapped')
        return cls._rc.get(field+':'+id+':'+cls._prefix)

    # Item specific type of 

    def sadd(self, attr, value):
        return

    def sismember(self, field, value, query):
        return
    
    def hincrby(self, attr, count=1):
        if not attr in self.__class__._fields:
            raise KeyError('invalid field')
        if not self.__class__._fields[attr] is int:
            raise TypeError('can only increment ints')
        return self.__class__._rc.hincrby(self.__class__._prefix+':'+self._id,attr,count)

    def __init__(self,id,db):
        self._db = db
        if not self.__class__.exists(id):
            raise KeyError(id,'has not been created yet')
        self.__dict__['_id'] = id
        
    def __getitem__(self, attr):
        if not attr in self._fields:
            raise KeyError('invalid field')

        if isinstance(self._fields[attr],set):
            return self.__class__._rc.smembers(self.__class__._prefix+':'+self._id+':'+attr)
        else:
        # get value then type cast
            return self.__class__._fields[attr](self.__class__._rc.hget(self.__class__._prefix+':'+self._id, attr))
    
    def __setitem__(self, attr, value):
        if not value:
            raise ValueError('got NaN')
        if not attr in self._fields:
            raise KeyError('invalid field')
        if not isinstance(value,self._fields[attr]):
            raise TypeError('expected',self.__class__._fields[attr],'got',type(value))
        
        # add support for sets
        if isinstance(self._fields[attr],set):
            if attr in self.__class__._rmaps:
                raise TypeError('rmaps not supported for set types')
            for element in value:
                self.__class__._rc.sadd(self.__class__._prefix+':'+self._id+':'+attr,value)
        elif:
            if attr in self.__class__._rmaps:
                self.__class__._rc.set(attr+':'+value+':'+self.__class__._prefix,self._id)
            self.__class__._rc.hset(self.__class__._prefix+':'+self._id, attr, value)
       