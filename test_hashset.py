import hashset
import common
import unittest
import sys
import uuid

class Person(hashset.HashSet):

    prefix = 'person'
    # name is an implicit id (the primary key)
    fields = {'ssn'   : str,     # string
              'kids'  : set,     # set
              'age'   : int,     # integer
              'tasks' : dict, 
             }

    lookups = {'ssn'}

class TestHashSet(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = common.init_redis('5902')
        cls.db.flushdb()

    def _create_key(self):
        random_name = str(uuid.uuid4())
        person = Person.create(random_name,self.db)
        ssn = str(uuid.uuid4())
        person['ssn'] = ssn
        person['kids'] = {'joe','charlie'}
        person['age'] = 25
        return person

    def test_create_key(self):
        random_name = str(uuid.uuid4())
        person = Person.create(random_name,self.db)
        ssn = str(uuid.uuid4())
        person['ssn'] = ssn
        person['kids'] = {'joe','charlie'}
        person['age'] = 25
        self.assertTrue(self.db.sismember('persons',random_name))
        self.assertEqual(person.id, random_name)
        self.assertEqual(self.db.hget('person:'+person.id,'ssn'),ssn)
        self.assertTrue(self.db.hget('person:'+person.id,'age'),25)
        self.assertEqual(self.db.smembers('person:'+person.id+':kids'), {'joe','charlie'})
        person.remove()

    def test_delete_key(self):
        person = self._create_key()
        ssn = person['ssn']
        person.remove()
        self.assertFalse(self.db.sismember('persons', person.id))
        self.assertFalse(self.db.exists('person:'+person.id))
        self.assertFalse(self.db.exists('ssn:'+ssn+':'+person.prefix))
        self.assertFalse(self.db.exists('ssn:'+ssn+':'+Person.prefix))

    def test_lookup(self):
        person = self._create_key()
        ssn = person['ssn']
        self.assertEqual(Person.lookup('ssn',ssn,person.db),person.id)
        person.remove()

    def test_hash_methods(self):
        person = Person.create(str(uuid.uuid4()),self.db)
        person['age'] = 25
        person.hincrby('age',1)
        self.assertEqual(person['age'],26)
        person.remove()

    def test_set_methods(self):
        person = Person.create(str(uuid.uuid4()),self.db)
        person['kids'] = {'jamie'}
        person.sadd('kids','jackie')
        person.sadd('kids',*['johnny','jenny'])
        self.assertTrue(person.sismember('kids','johnny'))
        self.assertEqual(person.smembers('kids'),{'jamie','jackie','johnny','jenny'})
        person.srem('kids','johnny')
        self.assertEqual(person.smembers('kids'),{'jamie','jackie','jenny'})
        person.remove()

    def test_zset_methods(self):
        person = Person.create(str(uuid.uuid4()),self.db)
        person['tasks'] = { 'mow_lawn' : 3,
                            'groceries' : 5,
                            'sleep': 0 }
        self.assertEqual(person['tasks'],['sleep','mow_lawn','groceries'])
        person.zadd('tasks','tennis',6)
        self.assertEqual(person['tasks'],['sleep','mow_lawn','groceries','tennis'])
        person.zrem('tasks','mow_lawn')
        self.assertEqual(person['tasks'],['sleep','groceries','tennis'])
        person.remove()
        #print person['tasks']

    def test_members(self):
        p1 = self._create_key()
        p2 = self._create_key()
        self.assertEqual(Person.members(self.db),{p1.id,p2.id})
        p1.remove()
        p2.remove()

    @classmethod
    def tearDownClass(cls):
        print cls.db.keys('*')
        cls.db.shutdown()
        pass

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)