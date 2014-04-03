import tornado.testing
import os
import shutil
import unittest
import sys
import json
import time
import bcrypt

import server.cc as cc


class TestCommandCenter(tornado.testing.AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        self.increment = 3
        redis_options = {'port': 3828, 'logfile': os.devnull}
        mongo_options = {'host': 'localhost', 'port': 27017}
        self.cc = cc.CommandCenter(name='test_cc',
                                   external_host='localhost',
                                   redis_options=redis_options,
                                   mongo_options=mongo_options)
        super(TestCommandCenter, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.cc.shutdown_redis()
        shutil.rmtree(self.cc.data_folder)
        super(TestCommandCenter, self).tearDownClass()

    def tearDown(self):
        self.cc.db.flushdb()
        for db_name in self.cc.mdb.database_names():
            self.cc.mdb.drop_database(db_name)

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
        email = 'proteneer@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password,
            'role': 'manager'
        }
        rep = self.fetch('/managers', method='POST', body=json.dumps(body))
        self.assertEqual(rep.code, 200)
        query = self.cc.mdb.users.managers.find_one({'_id': email})
        stored_hash = query['password_hash']
        stored_token = query['token']
        stored_role = query['role']
        self.assertEqual(stored_hash,
                         bcrypt.hashpw(password.encode(), stored_hash))
        reply_token = json.loads(rep.body.decode())['token']
        self.assertEqual(stored_token, reply_token)
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
            'role': 'admin'
        }

        reply = self.fetch('/managers', method='POST', body=json.dumps(body))
        self.assertEqual(reply.code, 200)
        reply_token = json.loads(reply.body.decode())['token']
        headers = {'Authorization': reply_token}
        body = {
            'email': 'test_user@gmail.com',
            'password': 'some_pass',
            'role': 'manager'
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
            'role': 'manager'
        }
        reply = self.fetch('/managers', method='POST', body=json.dumps(body),
                           headers=headers)
        self.assertEqual(reply.code, 401)

    # def test_register_cc(self):
    #     ws_name = 'ramanujan'
    #     ext_http_port = 5829
    #     ws_redis_port = 1234
    #     ws_redis_pass = 'blackmill'

    #     body = {'name': ws_name,
    #             'url': '127.0.0.1',
    #             'http_port': ext_http_port,
    #             'redis_port': ws_redis_port,
    #             'redis_pass': ws_redis_pass,
    #             'auth': self.cc_auth
    #             }

    #     headers = {'Authorization': self.cc_auth}

    #     reply = self.fetch('/ws/register', method='PUT', body=json.dumps(body),
    #                        headers=headers)
    #     self.assertEqual(reply.code, 200)

    #     ws = cc.WorkServer(ws_name, self.cc.db)
    #     self.assertEqual(ws.hget('url'), '127.0.0.1')
    #     self.assertEqual(ws.hget('http_port'), ext_http_port)

    def test_post_target(self):
        email = 'proteneer@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password,
            'role': 'manager'
        }
        reply = self.fetch('/managers', method='POST', body=json.dumps(body))
        auth = json.loads(reply.body.decode())['token']
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
        self.assertEqual(content['owner'], email)
        self.assertEqual(content['stage'], 'private')
        self.assertEqual(content['engine'], 'openmm')
        self.assertEqual(content['engine_versions'], ['6.0'])
        self.assertEqual(content['options'], options)
        self.assertAlmostEqual(content['creation_date'], time.time(), 1)

    def test_update_targets(self):
        email = 'proteneer@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password,
            'role': 'manager'
        }
        reply = self.fetch('/managers', method='POST', body=json.dumps(body))
        auth = json.loads(reply.body.decode())['token']
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
