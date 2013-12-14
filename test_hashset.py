import hashset
import common
import unittest
import sys
import uuid

class Person(hashset.HashSet):

    prefix = 'person'
    # name is an implicit id (the primary key)
    fields = {'ssn'  : str,
              'kids' : set,
              'age'  : int
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

    def test_members(self):
        p1 = self._create_key()
        p2 = self._create_key()
        self.assertEqual(Person.members(self.db),{p1.id,p2.id})
        p1.remove()
        p2.remove()

    @classmethod
    def tearDownClass(cls):
        cls.db.keys('*')
        cls.db.shutdown()
        pass

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)