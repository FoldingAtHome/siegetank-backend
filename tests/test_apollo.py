# Authors: Yutong Zhao <proteneer@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import unittest
import server.apollo as apollo
import sys
import server.common as common

#redis_client = redis.Redis(decode_responses=True)
#redis_client.ping()

class Person(apollo.Entity):
    prefix = 'person'
    fields = {'age': int,
              'ssn': str,
              'favorite_food': str,
              'emails': {str},
              'favorite_songs': {str},
              'tasks': apollo.zset(str)
              }


class Cat(apollo.Entity):
    prefix = 'cat'
    fields = {'age': int}

Person.add_lookup('ssn')
Person.add_lookup('favorite_food', injective=False)
Person.add_lookup('emails')
Person.add_lookup('favorite_songs', injective=False)

apollo.relate(Person, 'cats', {Cat}, 'owner')
apollo.relate({Person}, 'cats_to_feed', {Cat}, 'caretakers')
apollo.relate({Person}, 'friends', {Person}, 'friends')
apollo.relate(Person, 'best_friend', Person, 'best_friend')
apollo.relate(Person, 'single_cat', Cat, 'single_owner')


class TestApollo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        redis_options = {'port': 29387}
        cls.db = common.init_redis(redis_options=redis_options)
        cls.db.flushdb()

    @classmethod
    def tearDownClass(cls):
        cls.db.shutdown()

    def tearDown(self):
        if self.db.keys('*') != []:
            self.db.flushdb()
            print(self.db.keys('*'))
            self.assertTrue(0)

    def test_sismember(self):
        eve = Person.create('eve', self.db)
        eve.sadd('emails', 'foo@gmail.com', 'bar@gmail.com')
        self.assertTrue(eve.sismember('emails', 'foo@gmail.com'))
        self.assertTrue(eve.sismember('emails', 'bar@gmail.com'))

        eve.delete()

    def test_atomic_creation(self):
        emails = {'eve@eve.com', 'eve@gmail.com'}
        favorite_songs = {'claire de lune'}
        fields = {'ssn': '321-45-6982',
                  'age': 25,
                  'emails': emails,
                  'favorite_songs': favorite_songs}
        eve = Person.create('eve', self.db, fields)
        self.assertEqual(eve.hget('ssn'), '321-45-6982')
        self.assertEqual(eve.hget('age'), 25)
        self.assertEqual(eve.smembers('favorite_songs'), favorite_songs)
        self.assertEqual(eve.smembers('emails'), emails)

        cat_fields = {'age': 33,
                      'owner': eve,
                      'caretakers': {eve}
                      }

        sphinx = Cat.create('sphinx', self.db, cat_fields)

        self.assertEqual(eve.smembers('cats'), {'sphinx'})
        self.assertEqual(eve.smembers('cats_to_feed'), {'sphinx'})
        self.assertEqual(sphinx.smembers('caretakers'), {'eve'})
        self.assertEqual(sphinx.hget('owner'), 'eve')
        self.assertEqual(sphinx.hget('age'), 33)
        eve.delete()
        sphinx.delete()

    def test_lookup_1_to_1(self):
        joe = Person.create('joe', self.db)
        joe.hset('ssn', '123-45-6789')
        self.assertEqual(Person.lookup('ssn', '123-45-6789', self.db), 'joe')

        joe.hdel('ssn')
        self.assertEqual(joe.hget('ssn'), None)
        self.assertEqual(Person.lookup('ssn', '123-45-6789', self.db), None)

        bob = Person.create('bob', self.db)
        bob.hset('ssn', '234-56-7890')
        self.assertEqual(Person.lookup('ssn', '234-56-7890', self.db), 'bob')

        bob.delete()
        self.assertEqual(Person.lookup('ssn', '234-56-7890', self.db), None)

        joe.delete()

    def test_lookup_1_to_1_override(self):
        joe = Person.create('joe', self.db)
        joe.hset('ssn', '123-45-6789')
        bob = Person.create('bob', self.db)
        bob.hset('ssn', '123-45-6789')

        self.assertEqual(Person.lookup('ssn', '123-45-6789', self.db), 'bob')

        joe.delete()
        bob.delete()

    def test_lookup_n_to_1(self):
        joe = Person.create('joe', self.db)
        joe.hset('favorite_food', 'pizza')
        bob = Person.create('bob', self.db)
        bob.hset('favorite_food', 'pizza')
        self.assertSetEqual(Person.lookup('favorite_food', 'pizza', self.db),
                            {'joe', 'bob'})

        bob.delete()
        self.assertSetEqual(Person.lookup('favorite_food', 'pizza', self.db),
                            {'joe'})

        joe.delete()
        self.assertSetEqual(Person.lookup('favorite_food', 'pizza', self.db),
                            set())

    def test_lookup_n_to_1_override(self):
        joe = Person.create('joe', self.db)
        joe.hset('favorite_food', 'pizza')
        bob = Person.create('bob', self.db)
        bob.hset('favorite_food', 'pizza')
        self.assertSetEqual(Person.lookup('favorite_food', 'pizza', self.db),
                            {'joe', 'bob'})

        bob.hset('favorite_food', 'hotdog')
        self.assertSetEqual(Person.lookup('favorite_food', 'pizza', self.db),
                            {'joe'})
        self.assertSetEqual(Person.lookup('favorite_food', 'hotdog', self.db),
                            {'bob'})

        joe.delete()
        bob.delete()

    def test_lookup_1_to_n(self):
        joe = Person.create('joe', self.db)
        joe.sadd('emails', 'joe@gmail.com')
        joe.sadd('emails', 'joe@hotmail.com')
        self.assertEqual(Person.lookup('emails', 'joe@gmail.com', self.db),
                         'joe')
        self.assertEqual(Person.lookup('emails', 'joe@hotmail.com', self.db),
                         'joe')

        joe.srem('emails', 'joe@gmail.com')
        self.assertEqual(Person.lookup('emails', 'joe@gmail.com', self.db),
                         None)

        joe.delete()
        self.assertEqual(Person.lookup('emails', 'joe@hotmail.com', self.db),
                         None)

    def test_lookup_1_to_n_override(self):
        eve = Person.create('eve', self.db)
        eve.sadd('emails', 'eve@gmail.com')

        bob = Person.create('bob', self.db)
        bob.sadd('emails', 'eve@gmail.com')
        self.assertEqual(Person.lookup('emails', 'eve@gmail.com', self.db),
                         'bob')

        # this should do nothing since eve@gmail.com no longer belongs to eve
        try:
            eve.srem('emails', 'eve@gmail.com')
            raise(Exception('Expected an error to be raised here...'))
        except ValueError:
            pass

        self.assertEqual(Person.lookup('emails', 'eve@gmail.com', self.db),
                         'bob')

        eve.delete()
        bob.delete()

    def test_lookup_1_to_n_switch(self):
        joe = Person.create('joe', self.db)
        joe.sadd('emails', 'joe@gmail.com', 'cool_dude@gmail.com')

        bob = Person.create('bob', self.db)
        bob.sadd('emails', 'cool_dude@gmail.com')

        self.assertEqual(Person.lookup('emails', 'cool_dude@gmail.com',
                                       self.db), 'bob')

        self.assertEqual(joe.smembers('emails'), {'joe@gmail.com'})

        joe.delete()
        bob.delete()

    def test_lookup_n_to_n(self):
        joe = Person.create('joe', self.db)
        bob = Person.create('bob', self.db)

        joe.sadd('favorite_songs', 'prelude')
        joe.sadd('favorite_songs', 'nocturne')
        bob.sadd('favorite_songs', 'nocturne')
        bob.sadd('favorite_songs', 'concerto')

        self.assertEqual(Person.lookup('favorite_songs', 'nocturne', self.db),
                         {'joe', 'bob'})
        self.assertEqual(Person.lookup('favorite_songs', 'prelude', self.db),
                         {'joe'})
        self.assertEqual(Person.lookup('favorite_songs', 'concerto', self.db),
                         {'bob'})

        joe.srem('favorite_songs', 'nocturne')
        self.assertEqual(Person.lookup('favorite_songs', 'nocturne', self.db),
                         {'bob'})

        joe.delete()
        bob.delete()

    def test_hincrby(self):
        joe = Person.create('joe', self.db)
        joe.hset('age', 25)
        joe.hincrby('age')
        self.assertEqual(joe.hget('age'), 26)
        joe.delete()

    def test_hset_hget(self):
        joe = Person.create('joe', self.db)
        age = 25
        ssn = '123-45-6789'
        joe.hset('age', 25)
        self.assertEqual(joe.hget('age'), age)
        joe.hset('ssn', '123-45-6789')
        self.assertEqual(joe.hget('ssn'), ssn)
        joe.delete()

    def test_hdel(self):
        joe = Person.create('joe', self.db)
        age = 25
        joe.hset('age', 25)
        self.assertEqual(joe.hget('age'), age)
        joe.hdel('age')
        self.assertEqual(joe.hget('age'), None)

        sphinx = Cat.create('sphinx', self.db)
        joe.hset('single_cat', sphinx)
        self.assertEqual(sphinx.hget('single_owner'), 'joe')
        joe.hdel('single_cat')
        self.assertEqual(sphinx.hget('single_owner'), None)

        joe.delete()
        sphinx.delete()

    def test_srem(self):
        joe = Person.create('joe', db=self.db)
        joe.sadd('emails', 'joe@gmail.com')
        joe.sadd('emails', 'joe@hotmail.com')
        self.assertEqual(joe.smembers('emails'), {'joe@gmail.com',
                                                  'joe@hotmail.com'})
        joe.srem('emails', 'joe@gmail.com')
        self.assertEqual(joe.smembers('emails'), {'joe@hotmail.com'})

        joe.delete()

    def test_1_to_1_relations(self):
        joe = Person.create('joe', self.db)
        sphinx = Cat.create('sphinx', self.db)
        joe.hset('single_cat', sphinx)
        self.assertEqual(joe.hget('single_cat'), 'sphinx')
        self.assertEqual(sphinx.hget('single_owner'), 'joe')
        # change owners
        bob = Person.create('bob', self.db)
        bob.hset('single_cat', sphinx)
        self.assertEqual(bob.hget('single_cat'), 'sphinx')
        self.assertEqual(sphinx.hget('single_owner'), 'bob')
        self.assertEqual(joe.hget('single_cat'), None)

        polly = Cat.create('polly', self.db)
        polly.hset('single_owner', bob)
        self.assertEqual(bob.hget('single_cat'), 'polly')
        self.assertEqual(sphinx.hget('single_owner'), None)

        polly.delete()
        self.assertEqual(bob.hget('single_cat'), None)

        joe.delete()
        sphinx.delete()
        bob.delete()

    def test_1_to_n_relations_override(self):
        joe = Person.create('joe', self.db)
        sphinx = Cat.create('sphinx', self.db)
        bob = Person.create('bob', self.db)

        joe.sadd('cats', sphinx)
        self.assertEqual(joe.smembers('cats'), {'sphinx'})

        bob.sadd('cats', sphinx)
        self.assertEqual(joe.smembers('cats'), set())
        self.assertEqual(bob.smembers('cats'), {'sphinx'})
        self.assertEqual(sphinx.hget('owner'), 'bob')

        try:
            joe.srem('cats', sphinx)
            raise Exception('expected exception to be thrown here')
        except Exception:
            pass
        self.assertEqual(joe.smembers('cats'), set())
        self.assertEqual(bob.smembers('cats'), {'sphinx'})
        self.assertEqual(sphinx.hget('owner'), 'bob')

        sphinx.delete()
        joe.delete()
        bob.delete()

    def test_1_to_n_relations(self):
        joe = Person.create('joe', self.db)
        sphinx = Cat.create('sphinx', self.db)
        #joe.sadd('cats', sphinx)
        sphinx.hset('owner', joe)
        self.assertEqual(sphinx.hget('owner'), 'joe')

        polly = Cat.create('polly', self.db)
        polly.hset('owner', joe)
        self.assertEqual(polly.hget('owner'), 'joe')
        self.assertSetEqual(joe.smembers('cats'), {'sphinx', 'polly'})

        # change of ownership
        bob = Person.create('bob', self.db)
        sphinx.hset('owner', bob)
        self.assertEqual(sphinx.hget('owner'), 'bob')
        self.assertEqual(joe.smembers('cats'), {'polly'})
        self.assertEqual(bob.smembers('cats'), {'sphinx'})

        bob.sadd('cats', polly)
        self.assertEqual(bob.smembers('cats'), {'sphinx', 'polly'})
        self.assertEqual(joe.smembers('cats'), set())

        bob.srem('cats', sphinx)
        self.assertEqual(bob.smembers('cats'), {'polly'})
        self.assertEqual(sphinx.hget('owner'), None)

        joe.delete()
        sphinx.delete()
        bob.delete()
        polly.delete()

    def test_remove_item(self):
        joe = Person.create('joe', self.db)
        sphinx = Cat.create('sphinx', self.db)
        sphinx.hset('owner', joe)
        # test deletion
        joe.srem('cats', sphinx)
        self.assertEqual(joe.smembers('cats'), set())
        self.assertEqual(sphinx.hget('owner'), None)

        joe.delete()
        sphinx.delete()

    def test_n_to_n_relations(self):
        joe = Person.create('joe', self.db)
        bob = Person.create('bob', self.db)
        sphinx = Cat.create('sphinx', self.db)
        polly = Cat.create('polly', self.db)

        sphinx.sadd('caretakers', joe)
        polly.sadd('caretakers', bob)
        self.assertSetEqual(sphinx.smembers('caretakers'), {'joe'})
        self.assertSetEqual(polly.smembers('caretakers'), {'bob'})
        self.assertSetEqual(joe.smembers('cats_to_feed'), {'sphinx'})
        self.assertSetEqual(bob.smembers('cats_to_feed'), {'polly'})

        sphinx.sadd('caretakers', bob)
        self.assertSetEqual(joe.smembers('cats_to_feed'), {'sphinx'})
        self.assertSetEqual(bob.smembers('cats_to_feed'), {'polly', 'sphinx'})
        self.assertSetEqual(polly.smembers('caretakers'), {'bob'})
        self.assertSetEqual(sphinx.smembers('caretakers'), {'joe', 'bob'})

        sphinx.srem('caretakers', bob)
        self.assertSetEqual(sphinx.smembers('caretakers'), {'joe'})
        self.assertSetEqual(bob.smembers('cats_to_feed'), {'polly'})

        joe.delete()
        bob.delete()
        sphinx.delete()
        polly.delete()

    def test_self_1_to_1(self):
        joe = Person.create('joe', self.db)
        bob = Person.create('bob', self.db)

        joe.hset('best_friend', bob)
        self.assertEqual(joe.hget('best_friend'), 'bob')
        self.assertEqual(bob.hget('best_friend'), 'joe')

        eve = Person.create('eve', self.db)
        eve.hset('best_friend', bob)
        self.assertEqual(joe.hget('best_friend'), None)
        self.assertEqual(eve.hget('best_friend'), 'bob')
        self.assertEqual(bob.hget('best_friend'), 'eve')

        joe.delete()
        eve.delete()
        self.assertEqual(bob.hget('best_friend'), None)
        bob.delete()

    def test_self_n_to_n(self):
        joe = Person.create('joe', self.db)
        bob = Person.create('bob', self.db)

        joe.sadd('friends', bob)
        self.assertSetEqual(joe.smembers('friends'), {'bob'})
        self.assertSetEqual(bob.smembers('friends'), {'joe'})

        joe.srem('friends', bob)
        self.assertSetEqual(joe.smembers('friends'), set())
        self.assertSetEqual(bob.smembers('friends'), set())

        joe.delete()
        bob.delete()

    def test_delete_entity(self):
        joe = Person.create('joe', self.db)
        joe.delete()
        self.assertListEqual(self.db.keys('*'), [])

        joe = Person.create('joe', self.db)
        sphinx = Cat.create('sphinx', self.db)
        polly = Cat.create('polly', self.db)

        joe.sadd('cats', sphinx)
        joe.sadd('cats', polly)
        self.assertSetEqual(joe.smembers('cats'), {'sphinx', 'polly'})

        polly.delete()
        self.assertSetEqual(joe.smembers('cats'), {'sphinx'})
        self.assertEqual(sphinx.hget('owner'), 'joe')

        joe.delete()
        self.assertEqual(sphinx.hget('owner'), None)
        self.assertListEqual(self.db.keys('*'), ['cats'])

        sphinx.delete()
        self.assertListEqual(self.db.keys('*'), [])

    def test_basic_sorted_set(self):
        joe = Person.create('joe', self.db)
        joe.zadd('tasks', 'sleep', 5)
        joe.zadd('tasks', 'eat', 1)
        joe.zadd('tasks', 'drink', 3)
        self.assertListEqual(joe.zrange('tasks', 0, -1),
                             ['eat', 'drink', 'sleep'])
        joe.zrem('tasks', 'drink')
        self.assertListEqual(joe.zrange('tasks', 0, -1), ['eat', 'sleep'])

        joe.delete()

    def test_pipeline_zadd(self):
        joe = Person.create('joe', self.db)
        pipeline = self.db.pipeline()
        joe.zadd('tasks', 'sleep', 5, pipeline=pipeline)
        joe.zadd('tasks', 'eat', 1, pipeline=pipeline)
        joe.zadd('tasks', 'drink', 3, pipeline=pipeline)
        pipeline.execute()
        self.assertListEqual(joe.zrange('tasks', 0, -1),
                             ['eat', 'drink', 'sleep'])
        joe.delete()

    def test_zrevpop(self):
        joe = Person.create('joe', self.db)
        joe.zadd('tasks', 'sleep', 5)
        joe.zadd('tasks', 'eat', 1)
        joe.zadd('tasks', 'drink', 3)
        joe.zadd('tasks', 'work', 0)

        self.assertEqual(joe.zrevpop('tasks'), 'sleep')
        self.assertEqual(joe.zrevpop('tasks'), 'drink')
        self.assertEqual(joe.zrevpop('tasks'), 'eat')
        self.assertEqual(joe.zrevpop('tasks'), 'work')
        self.assertEqual(joe.zrevpop('tasks'), None)

        joe.delete()

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)
