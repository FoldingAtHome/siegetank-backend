import tornado.testing

import os
import shutil
import unittest
import sys
import json
import time
import bcrypt
import uuid

import server.cc as cc


class TestCommandCenter(tornado.testing.AsyncHTTPTestCase):
    def setUp(self):
        self.increment = 3
        redis_options = {'port': 3828, 'logfile': os.devnull}
        mongo_options = {'host': 'localhost', 'port': 27017}
        self.cc = cc.CommandCenter(name='test_cc',
                                   external_host='localhost',
                                   redis_options=redis_options,
                                   mongo_options=mongo_options)
        super(TestCommandCenter, self).setUp()

    def tearDown(self):
        self.cc.db.flushdb()
        self.cc.shutdown_redis()
        for db_name in self.cc.mdb.database_names():
            self.cc.mdb.drop_database(db_name)
        shutil.rmtree(self.cc.data_folder)
        super(TestCommandCenter, self).tearDown()

    def _add_manager(self, email='test@gm.com', role='manager', auth=None):
        password = str(uuid.uuid4())
        body = {
            'email': email,
            'password': password,
            'role': role,
            'weight': 1
        }
        if auth is not None:
            headers = {'Authorization': auth}
        else:
            headers = None
        reply = self.fetch('/managers', method='POST', body=json.dumps(body),
                           headers=headers)
        self.assertEqual(reply.code, 200)
        token = json.loads(reply.body.decode())['token']
        body['token'] = token
        return body

    def _post_target(self, auth):
        headers = {'Authorization': auth}
        description = "Diwakar and John's top secret project"
        options = {'steps_per_frame': 50000}
        body = {
            'description': description,
            'engine': 'openmm',
            'engine_versions': ['6.0'],
            'options': options
            }
        reply = self.fetch('/targets', method='POST', headers=headers,
                           body=json.dumps(body))
        self.assertEqual(reply.code, 200)
        target_id = json.loads(reply.body.decode())['target_id']
        reply = self.fetch('/targets/info/'+target_id)
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())
        self.assertEqual(content['description'], description)
        self.assertEqual(content['stage'], 'private')
        self.assertEqual(content['engine'], 'openmm')
        self.assertEqual(content['engine_versions'], ['6.0'])
        self.assertEqual(content['options'], options)
        self.assertAlmostEqual(content['creation_date'], time.time(), 0)
        content['target_id'] = target_id
        return content

    def test_add_donor(self):
        username = 'jesse_v'
        email = 'jv@jv.com'
        password = 'test_pw'
        body = {
            'username': username,
            'email': email,
            'password': password
        }
        rep = self.fetch('/donors', method='POST', body=json.dumps(body))
        self.assertEqual(rep.code, 200)
        query = self.cc.mdb.community.donors.find_one({'_id': username})
        stored_hash = query['password_hash']
        stored_token = query['token']
        self.assertEqual(stored_hash,
                         bcrypt.hashpw(password.encode(), stored_hash))
        reply_token = json.loads(rep.body.decode())['token']
        self.assertEqual(stored_token, reply_token)

        # test auth
        for i in range(5):
            body = {
                'username': username,
                'password': password
            }
            rep = self.fetch('/donors/auth', method='POST',
                             body=json.dumps(body))
            self.assertEqual(rep.code, 200)
            reply_token = json.loads(rep.body.decode())['token']
            query = self.cc.mdb.community.donors.find_one({'_id': username})
            stored_token = query['token']
            self.assertEqual(reply_token, stored_token)

        # make sure duplicate email throws error
        body = {
            'username': 'joe_bob',
            'email': email,
            'password': password
        }
        rep = self.fetch('/donors', method='POST', body=json.dumps(body))
        self.assertEqual(rep.code, 400)

        # make sure duplicate username throws error
        body = {
            'username': username,
            'email': 'test_email',
            'password': 'test_pw'
        }
        rep = self.fetch('/donors', method='POST', body=json.dumps(body))
        self.assertEqual(rep.code, 400)

    def test_add_manager(self):
        result = self._add_manager()
        email = result['email']
        password = result['password']
        token = result['token']
        query = self.cc.mdb.users.managers.find_one({'_id': email})
        stored_hash = query['password_hash']
        stored_token = query['token']
        stored_role = query['role']
        self.assertEqual(stored_hash,
                         bcrypt.hashpw(password.encode(), stored_hash))
        self.assertEqual(stored_token, token)
        self.assertEqual(stored_role, 'manager')

        # test auth
        for i in range(5):
            body = {
                'email': email,
                'password': password
            }
            rep = self.fetch('/managers/auth', method='POST',
                             body=json.dumps(body))
            self.assertEqual(rep.code, 200)
            reply_token = json.loads(rep.body.decode())['token']
            query = self.cc.mdb.users.managers.find_one({'_id': email})
            stored_token = query['token']
            self.assertEqual(reply_token, stored_token)

        body = {
            'email': 'admin@gmail.com',
            'password': 'some_pass',
            'role': 'admin',
            'weight': 1,
        }

        reply = self.fetch('/managers', method='POST', body=json.dumps(body))
        self.assertEqual(reply.code, 200)
        reply_token = json.loads(reply.body.decode())['token']
        headers = {'Authorization': reply_token}
        body = {
            'email': 'test_user@gmail.com',
            'password': 'some_pass',
            'role': 'manager',
            'weight': 1
        }
        reply = self.fetch('/managers', method='POST', body=json.dumps(body),
                           headers=headers)
        reply_token = json.loads(reply.body.decode())['token']
        headers = {'Authorization': reply_token}
        self.assertEqual(reply.code, 200)

        # Try posting as a manager
        body = {
            'email': 'test_user2@gmail.com',
            'password': 'some_pass2',
            'role': 'manager',
            'weight': 1
        }
        reply = self.fetch('/managers', method='POST', body=json.dumps(body),
                           headers=headers)
        self.assertEqual(reply.code, 401)

    def test_post_target(self):
        result = self._add_manager()
        email = result['email']
        auth = result['token']
        result = self._post_target(auth)
        self.assertEqual(result['owner'], email)

    def test_get_targets_handler(self):
        result = self._add_manager(email="joe@gmail.com")
        headers = {'Authorization': result['token']}
        result = self._post_target(result['token'])
        target_id = result['target_id']
        # fetch with authorization
        response = self.fetch('/targets', headers=headers)
        self.assertEqual(response.code, 200)
        targets = json.loads(response.body.decode())['targets']
        self.assertEqual([target_id], targets)
        result = self._add_manager(email="bob@gmail.com")
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
        self.assertEqual([target_id], targets)
        # second manager posts a target
        result = self._post_target(token2)
        target_id2 = result['target_id']
        # fetch without authorization
        response = self.fetch('/targets')
        self.assertEqual(response.code, 200)
        targets = json.loads(response.body.decode())['targets']
        self.assertEqual({target_id2, target_id}, set(targets))
        # fetch with authorization
        response = self.fetch('/targets', headers=headers)
        self.assertEqual(response.code, 200)
        targets = json.loads(response.body.decode())['targets']
        self.assertEqual([target_id2], targets)

    def test_update_targets(self):
        result = self._add_manager()
        auth = result['token']
        email = result['email']
        headers = {'Authorization': auth}
        result = self._post_target(auth)
        target_id = result['target_id']
        options = result['options']
        # update using a valid target_id
        description2 = 'hahah'
        body = {
            'description': description2,
            'stage': 'public',
            'engine_versions': ['9.9', '5.0']
        }
        reply = self.fetch('/targets/update/'+target_id, method='PUT',
                           headers=headers, body=json.dumps(body))
        self.assertEqual(reply.code, 200)
        reply = self.fetch('/targets/info/'+target_id)
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())
        self.assertEqual(content['description'], description2)
        self.assertEqual(content['owner'], email)
        self.assertEqual(content['stage'], 'public')
        self.assertEqual(content['engine'], 'openmm')
        self.assertEqual(set(content['engine_versions']), set(['9.9', '5.0']))
        self.assertEqual(content['options'], options)

        # update using an invalid target_id
        reply = self.fetch('/targets/update/bad_id', method='PUT',
                           headers=headers, body=json.dumps(body))
        self.assertEqual(reply.code, 400)

    def get_app(self):
        return self.cc

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)
