import tornado.testing
import os
import shutil
import unittest
import sys
import json
import base64
import bcrypt

import server.cc as cc
import server.common as common


class TestCommandCenter(tornado.testing.AsyncHTTPTestCase):
    @classmethod
    def setUpClass(self):
        self.increment = 3
        self.cc_auth = '5lik2j3l4'
        redis_options = {'port': 3828, 'logfile': os.devnull}
        mongo_options = {'host': 'localhost',
                         'port': 27017}
        self.cc = cc.CommandCenter(cc_name='test_cc',
                                   cc_pass=self.cc_auth,
                                   redis_options=redis_options,
                                   mongo_options=mongo_options,
                                   targets_folder='cc_targets')
        super(TestCommandCenter, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        self.cc.db.flushdb()
        self.cc.shutdown_redis()
        folders = [self.cc.targets_folder]
        for folder in folders:
            if os.path.exists(folder):
                shutil.rmtree(folder)
        super(TestCommandCenter, self).tearDownClass()

    def tearDown(self):
        self.cc.mdb.drop_database('users')
        self.cc.mdb.drop_database('community')
        self.cc.mdb.drop_database('targets')

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

    def test_register_cc(self):
        ws_name = 'ramanujan'
        ext_http_port = 5829
        ws_redis_port = 1234
        ws_redis_pass = 'blackmill'

        redis_options = {
            'port': ws_redis_port,
            'requirepass': ws_redis_pass,
            'logfile': os.devnull
        }

        test_db = common.init_redis(redis_options)
        test_db.ping()

        body = {'name': ws_name,
                'url': '127.0.0.1',
                'http_port': ext_http_port,
                'redis_port': ws_redis_port,
                'redis_pass': ws_redis_pass,
                'auth': self.cc_auth
                }

        headers = {'Authorization': self.cc_auth}

        reply = self.fetch('/ws/register', method='PUT', body=json.dumps(body),
                           headers=headers)
        self.assertEqual(reply.code, 200)

        ws = cc.WorkServer(ws_name, self.cc.db)
        self.assertEqual(ws.hget('url'), '127.0.0.1')
        self.assertEqual(ws.hget('http_port'), ext_http_port)
        test_db.shutdown()

    def test_post_target(self):
        email = 'proteneer@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password,
            'role': 'manager'
        }
        rep = self.fetch('/managers', method='POST', body=json.dumps(body))
        auth = json.loads(rep.body.decode())['token']
        headers = {'Authorization': auth}

        fb1, fb2, fb3, fb4 = (base64.b64encode(os.urandom(1024)).decode()
                              for i in range(4))

        description = "Diwakar and John's top secret project"

        body = {
            'description': description,
            'files': {'system.xml.gz.b64': fb1, 'integrator.xml.gz.b64': fb2},
            'steps_per_frame': 50000,
            'engine': 'openmm',
            'engine_versions': ['6.0'],
            }

        reply = self.fetch('/targets', method='POST', headers=headers,
                           body=json.dumps(body))
        self.assertEqual(reply.code, 200)
        target_id = json.loads(reply.body.decode())['target_id']
        system_path = os.path.join(self.cc.targets_folder, target_id,
                                   'system.xml.gz.b64')
        self.assertEqual(open(system_path, 'rb').read().decode(), fb1)
        intg_path = os.path.join(self.cc.targets_folder, target_id,
                                 'integrator.xml.gz.b64')
        self.assertEqual(open(intg_path, 'rb').read().decode(), fb2)
        self.assertTrue(cc.Target.exists(target_id, self.cc.db))
        target = cc.Target(target_id, self.cc.db)
        self.assertEqual(target.hget('description'), description)
        self.assertEqual(target.hget('steps_per_frame'), 50000)
        self.assertEqual(target.hget('stage'), 'private')
        self.assertEqual(target.hget('engine'), 'openmm')
        self.assertEqual(target.hget('owner'), email)
        self.assertEqual(target.smembers('engine_versions'), {'6.0'})
        self.assertEqual(target.smembers('files'), {'system.xml.gz.b64',
                                                    'integrator.xml.gz.b64'})

        query = self.cc.mdb.data.targets.find_one({'_id': target_id},
                                                  fields=['owner'])
        self.assertEqual(query['_id'], target_id)
        self.assertEqual(query['owner'], email)

        body = {
            'description': description,
            'files': {'system.xml.gz.b64': fb1, 'integrator.xml.gz.b64': fb2},
            'steps_per_frame': 50000,
            'engine': 'openmm',
            'engine_versions': ['6.0'],
            'stage': 'public',
            }

        reply = self.fetch('/targets', method='POST', headers=headers,
                           body=json.dumps(body))
        self.assertEqual(reply.code, 200)
        target_id = json.loads(reply.body.decode())['target_id']
        target = cc.Target(target_id, self.cc.db)
        self.assertEqual(target.hget('stage'), 'public')

        # test download the file
        reply = self.fetch('/targets/'+target_id+'/system.xml.gz.b64',
                           headers=headers)
        self.assertEqual(reply.body.decode(), fb1)
        reply = self.fetch('/targets/'+target_id+'/integrator.xml.gz.b64',
                           headers=headers)
        self.assertEqual(reply.body.decode(), fb2)

    def test_get_targets(self):
        email = 'eisley@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password,
            'role': 'manager'
        }
        rep = self.fetch('/managers', method='POST', body=json.dumps(body))
        auth = json.loads(rep.body.decode())['token']
        headers = {'Authorization': auth}

        # post a bunch of targets
        target_ids = []
        for i in range(4):
            fb1 = base64.b64encode(os.urandom(1024)).decode()
            fb2 = base64.b64encode(os.urandom(1024)).decode()
            description = "Diwakar and John's top secret project"
            body = {
                'description': description,
                'files': {'system.xml.gz.b64': fb1,
                          'integrator.xml.gz.b64': fb2},
                'steps_per_frame': 50000,
                'engine': 'openmm',
                'engine_versions': ['6.0'],
                }

            reply = self.fetch('/targets', method='POST', headers=headers,
                               body=json.dumps(body))
            self.assertEqual(reply.code, 200)
            target_ids.append(json.loads(reply.body.decode())['target_id'])
        # fetch all the targets
        reply = self.fetch('/targets', headers=headers)
        r_targets = json.loads(reply.body.decode())['targets']
        self.assertEqual(set(r_targets), set(target_ids))

        reply = self.fetch('/targets/info/'+target_ids[0])
        self.assertEqual(reply.code, 200)

        # add a new manager and post a bunch of targets
        email = 'diwakar@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password,
            'role': 'manager'
        }
        rep = self.fetch('/managers', method='POST', body=json.dumps(body))
        eisley_auth = json.loads(rep.body.decode())['token']
        headers = {'Authorization': eisley_auth}

        # post a bunch of targets
        e_target_ids = []
        for i in range(4):
            fb1 = base64.b64encode(os.urandom(1024)).decode()
            fb2 = base64.b64encode(os.urandom(1024)).decode()
            description = "Eisley project"
            body = {
                'description': description,
                'files': {'system.xml.gz.b64': fb1,
                          'integrator.xml.gz.b64': fb2},
                'steps_per_frame': 50000,
                'engine': 'openmm',
                'engine_versions': ['6.0'],
                }

            reply = self.fetch('/targets', method='POST', headers=headers,
                               body=json.dumps(body))
            self.assertEqual(reply.code, 200)
            e_target_ids.append(json.loads(reply.body.decode())['target_id'])

        # fetch all the targets
        reply = self.fetch('/targets')
        self.assertEqual(reply.code, 200)
        r_targets = json.loads(reply.body.decode())['targets']
        self.assertEqual(set(r_targets), set(target_ids+e_target_ids))

        reply = self.fetch('/targets', headers={'Authorization': auth})
        self.assertEqual(reply.code, 200)
        r_targets = json.loads(reply.body.decode())['targets']
        self.assertEqual(set(r_targets), set(target_ids))

        reply = self.fetch('/targets', headers={'Authorization': eisley_auth})
        self.assertEqual(reply.code, 200)
        r_targets = json.loads(reply.body.decode())['targets']
        self.assertEqual(set(r_targets), set(e_target_ids))

    def get_app(self):
        return self.cc

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)
