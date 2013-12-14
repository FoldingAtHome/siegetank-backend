import hashset
import common
import unittest
import sys

class Person(hashset.HashSet):

    _prefix = 'person'
    # name is an implicit id (the primary key)
    _fields = {'ssn'            : str,
               'kids'           : set,
               'age'            : int
               }

    _lookups = {'ssn'}

class TestHashSet(unittest.TestCase):
	def setUp(self):
		self.db = common.init_redis('5902')

	def test_create_key(self):
		bob = Person.create('bob',self.db)
		bob['ssn'] = '503-20-5930'
		bob['kids'] = {'joe','charlie'}
		bob['age'] = 25

		self.assertTrue(self.db.sismember('persons','bob'))
		self.assertEqual(self.db.hget('person:bob','ssn'),'503-20-5930')
		self.assertTrue(self.db.hget('person:bob','age'),25)
		self.assertEqual(self.db.smembers('person:bob:kids'), {'joe','charlie'})

	def tearDown(self):
		self.db.shutdown()
		pass

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)