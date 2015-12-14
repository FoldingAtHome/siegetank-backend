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

import tornado.testing

import os
import shutil
import unittest
import sys
import json
import time
import random
import pymongo

import cc.cc as cc
import tests.utils


class TestCommandCenter(tornado.testing.AsyncHTTPTestCase):

    def setUp(self):
        self.mongo_options = {'host': 'localhost', 'port': 27017}
        self.mdb = pymongo.MongoClient('localhost', 27017)
        for db_name in self.mdb.database_names():
            self.mdb.drop_database(db_name)
        super(TestCommandCenter, self).setUp()

    def tearDown(self):
        for db_name in self.mdb.database_names():
            self.mdb.drop_database(db_name)
        super(TestCommandCenter, self).tearDown()
        shutil.rmtree(self.cc.data_folder)

    def get_app(self):
        redis_options = {'port': 3828, 'logfile': os.devnull}
        self.cc = cc.CommandCenter(name='test_cc',
                                   redis_options=redis_options,
                                   mongo_options=self.mongo_options)
        self.cc.initialize_motor()
        return self.cc

    def _add_user(self, *args, **kwargs):
        return tests.utils.add_user(*args, **kwargs)

    def _post_target(
            self,
            auth,
            expected_code=200,
            engines=None,
            stage='private'):
        if engines is None:
            engines = ['openmm_opencl', 'openmm_cuda']
        headers = {'Authorization': auth}
        options = {
            'description': "Diwakar and John's top secret project",
            'steps_per_frame': 50000,
            'stage': stage
        }
        body = {'engines': engines, 'options': options}
        reply = self.fetch('/targets', method='POST', headers=headers,
                           body=json.dumps(body))
        self.assertEqual(reply.code, expected_code)
        if expected_code != 200:
            return
        self.cc._cache_shards()
        target_id = json.loads(reply.body.decode())['target_id']
        reply = self.fetch('/targets/info/' + target_id)
        self.assertEqual(reply.code, expected_code)
        content = json.loads(reply.body.decode())
        self.assertTrue(float(content['creation_date']) - time.time() < 2)
        self.assertEqual(content['stage'], 'private')
        self.assertEqual(content['engines'], ['openmm_opencl', 'openmm_cuda'])
        self.assertEqual(content['options'], options)
        content['target_id'] = target_id
        return content

    def _add_core_key(self, auth, expected_code=200):
        headers = {'Authorization': auth}
        body = {'engine': 'openmm', 'description': 'testing'}
        reply = self.fetch('/engines/keys', method='POST', headers=headers,
                           body=json.dumps(body))
        self.assertEqual(reply.code, expected_code)
        if expected_code == 200:
            return json.loads(reply.body.decode())

    def _delete_core_key(self, auth, core_key, expected_code=200):
        headers = {'Authorization': auth}
        reply = self.fetch('/engines/keys/delete/' + core_key, method='PUT',
                           headers=headers, body='')
        self.assertEqual(reply.code, expected_code)

    def _load_core_keys(self, auth, expected_code=200):
        headers = {'Authorization': auth}
        reply = self.fetch('/engines/keys', headers=headers)
        self.assertEqual(reply.code, expected_code)
        if expected_code == 200:
            return json.loads(reply.body.decode())

    # def test_user_auth(self):
    #     result = self._add_user()
    #     username = result['username']
    #     password = result['password']
    #     for i in range(10):
    #         print(i)
    #         body = {'username': username, 'password': password}
    #         rep = self.fetch('/users/auth', method='POST',
    #                          body=json.dumps(body))
    #         self.assertEqual(rep.code, 200)
    #         reply_token = json.loads(rep.body.decode())['token']
    #         query = self.mdb.users.all.find_one({'_id': username})
    #         stored_token = query['token']
    #         self.assertEqual(reply_token, stored_token)

    def test_user_verify(self):
        result = self._add_user()
        token = result['token']
        headers = {'Authorization': token}
        reply = self.fetch('/users/verify', headers=headers)
        self.assertEqual(reply.code, 200)

    # def test_add_donor(self):
    #     username = 'jesse_v'
    #     email = 'jv@jv.com'
    #     password = 'test_pw'
    #     body = {
    #         'username': username,
    #         'email': email,
    #         'password': password
    #     }
    #     rep = self.fetch('/donors', method='POST', body=json.dumps(body))
    #     self.assertEqual(rep.code, 200)
    #     query = self.mdb.community.donors.find_one({'_id': username})
    #     stored_hash = query['password_hash']
    #     stored_token = query['token']
    #     self.assertEqual(stored_hash,
    #                      bcrypt.hashpw(password.encode(), stored_hash))
    #     reply_token = json.loads(rep.body.decode())['token']
    #     self.assertEqual(stored_token, reply_token)

    #     # test auth
    #     for i in range(10):
    #         print(i)
    #         body = {
    #             'username': username,
    #             'password': password
    #         }
    #         rep = self.fetch('/donors/auth', method='POST',
    #                          body=json.dumps(body))
    #         self.assertEqual(rep.code, 200)
    #         reply_token = json.loads(rep.body.decode())['token']
    #         query = self.mdb.community.donors.find_one({'_id': username})
    #         stored_token = query['token']
    #         self.assertEqual(reply_token, stored_token)

    #     # make sure duplicate email throws error
    #     body = {
    #         'username': 'joe_bob',
    #         'email': email,
    #         'password': password
    #     }
    #     rep = self.fetch('/donors', method='POST', body=json.dumps(body))
    #     self.assertEqual(rep.code, 400)

    #     # make sure duplicate username throws error
    #     body = {
    #         'username': username,
    #         'email': 'test_email',
    #         'password': 'test_pw'
    #     }
    #     rep = self.fetch('/donors', method='POST', body=json.dumps(body))
    #     self.assertEqual(rep.code, 400)

    # def test_add_manager(self):
    #     result = self._add_manager()
    #     email = result['email']
    #     password = result['password']
    #     token = result['token']
    #     query = self.mdb.users.managers.find_one({'_id': email})
    #     stored_hash = query['password_hash']
    #     stored_token = query['token']
    #     stored_role = query['role']
    #     self.assertEqual(stored_hash,
    #                      bcrypt.hashpw(password.encode(), stored_hash))
    #     self.assertEqual(stored_token, token)
    #     self.assertEqual(stored_role, 'manager')

    #     # test auth
    #     for i in range(5):
    #         body = {
    #             'email': email,
    #             'password': password
    #         }
    #         rep = self.fetch('/managers/auth', method='POST',
    #                          body=json.dumps(body))
    #         self.assertEqual(rep.code, 200)
    #         reply_token = json.loads(rep.body.decode())['token']
    #         query = self.mdb.users.managers.find_one({'_id': email})
    #         stored_token = query['token']
    #         self.assertEqual(reply_token, stored_token)

    #     body = {
    #         'email': 'admin@gmail.com',
    #         'password': 'some_pass',
    #         'role': 'admin',
    #         'weight': 1,
    #     }

    #     reply = self.fetch('/managers', method='POST', body=json.dumps(body))
    #     self.assertEqual(reply.code, 200)
    #     reply_token = json.loads(reply.body.decode())['token']
    #     headers = {'Authorization': reply_token}
    #     body = {
    #         'email': 'test_user@gmail.com',
    #         'password': 'some_pass',
    #         'role': 'manager',
    #         'weight': 1
    #     }
    #     reply = self.fetch('/managers', method='POST', body=json.dumps(body),
    #                        headers=headers)
    #     reply_token = json.loads(reply.body.decode())['token']
    #     headers = {'Authorization': reply_token}
    #     self.assertEqual(reply.code, 200)

    #     # Try posting as a manager
    #     body = {
    #         'email': 'test_user2@gmail.com',
    #         'password': 'some_pass2',
    #         'role': 'manager',
    #         'weight': 1
    #     }
    #     reply = self.fetch('/managers', method='POST', body=json.dumps(body),
    #                        headers=headers)
    #     self.assertEqual(reply.code, 401)

    def test_post_target(self):
        result = self._add_user(user='joebob', manager=True)
        user = result['user']
        auth = result['token']
        result = self._post_target(auth)
        self.assertEqual(result['owner'], user)
        # make sure normal user can't post targets
        result = self._add_user(user='bobjoe', manager=False)
        auth = result['token']
        result = self._post_target(auth, expected_code=401)

    def test_get_targets(self):
        result = self._add_user(user="joe", manager=True)
        headers = {'Authorization': result['token']}
        result = self._post_target(result['token'])
        target_id = result['target_id']
        # fetch with authorization
        response = self.fetch('/targets', headers=headers)
        self.assertEqual(response.code, 200)
        targets = json.loads(response.body.decode())['targets']
        self.assertEqual([target_id], targets)
        result = self._add_user(user="bob", manager=True)
        token2 = result['token']
        headers = {'Authorization': token2}
        # fetch using another manager's auth
        response = self.fetch('/targets', headers=headers)
        self.assertEqual(response.code, 200)
        targets = json.loads(response.body.decode())['targets']
        self.assertEqual([], targets)
        # fetch without authorization
        response = self.fetch('/targets')
        self.assertEqual(response.code, 200)
        targets = json.loads(response.body.decode())['targets']
        self.assertEqual([], targets)
        # second manager posts a target
        result = self._post_target(token2)
        target_id2 = result['target_id']
        # fetch without authorization
        response = self.fetch('/targets')
        self.assertEqual(response.code, 200)
        targets = json.loads(response.body.decode())['targets']
        self.assertEqual([], targets)
        # fetch with authorization
        response = self.fetch('/targets', headers=headers)
        self.assertEqual(response.code, 200)
        targets = json.loads(response.body.decode())['targets']
        self.assertEqual([target_id2], targets)

    def test_update_targets(self):
        result = self._add_user(manager=True)
        auth = result['token']
        user = result['user']
        headers = {'Authorization': auth}
        result = self._post_target(auth)
        target_id = result['target_id']
        options = result['options']
        # update using a valid target_id
        new_engines = ['onetwo', 'threefour', 'fivesix']
        body = {
            'stage': 'public',
            'engines': new_engines,
        }
        reply = self.fetch('/targets/update/' + target_id, method='PUT',
                           headers=headers, body=json.dumps(body))
        self.assertEqual(reply.code, 200)
        reply = self.fetch('/targets/info/' + target_id)
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())
        self.assertEqual(content['owner'], user)
        self.assertEqual(content['stage'], 'public')
        self.assertEqual(content['engines'], new_engines)
        self.assertEqual(content['options'], options)
        options['description'] = 'ram'
        body = {'options': options}
        reply = self.fetch('/targets/update/' + target_id, method='PUT',
                           headers=headers, body=json.dumps(body))
        self.assertEqual(reply.code, 200)
        reply = self.fetch('/targets/info/' + target_id)
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())
        self.assertEqual(content['options'], options)
        # update using an invalid target_id
        reply = self.fetch('/targets/update/bad_id', method='PUT',
                           headers=headers, body=json.dumps(body))
        self.assertEqual(reply.code, 400)
        # update using another managers token
        result = self._add_user(user='baduser', manager=True)
        auth = result['token']
        headers = {'Authorization': auth}
        reply = self.fetch('/targets/update/' + target_id, method='PUT',
                           headers=headers, body=json.dumps(body))
        self.assertEqual(reply.code, 401)

    def test_core_keys(self):
        bad_token = self._add_user()['token']
        self._add_core_key(bad_token, 401)
        bad_token = self._add_user(user='bad_user2', manager=True)['token']
        self._add_core_key(bad_token, 401)
        result = self._add_user(user='good_user', admin=True)
        good_token = result['token']
        keys = []
        content = self._add_core_key(good_token)
        keys.append(content['key'])
        for i in range(5):
            content = self._add_core_key(good_token)
            keys.append(content['key'])
        self._load_core_keys(bad_token, 401)
        content = self._load_core_keys(good_token)
        self.assertEqual(set(content.keys()), set(keys))
        random_key = random.choice(keys)
        self._delete_core_key(bad_token, random_key, 401)
        self._delete_core_key(good_token, random_key)
        keys.remove(random_key)
        content = self._load_core_keys(good_token)
        self.assertEqual(set(content.keys()), set(keys))
        self._delete_core_key(good_token, '1234', 400)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)
