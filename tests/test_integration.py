import tornado
import tornado.web
import tornado.httpclient
import tornado.httpserver
import tornado.testing
import tornado.gen

import unittest
import os
import shutil
import uuid
import urllib
import itertools

import server.scv as scv
import server.cc as cc
import server.common as common
import sys
import random
import base64
import json
import time
import tornado.ioloop


class TestSimple(tornado.testing.AsyncTestCase):
    @classmethod
    def setUpClass(self):
        super(TestSimple, self).setUpClass()
        io_loop = tornado.ioloop.IOLoop.instance()
        mongo_options = {'host': 'localhost'}
        redis_options = {'port': 2733, 'logfile': os.devnull}
        self.cc_host = '127.0.0.1:7654'
        self.cc = cc.CommandCenter(name='goliath',
                                   external_host=self.cc_host,
                                   redis_options=redis_options,
                                   mongo_options=mongo_options)
        self.cc_server = tornado.httpserver.HTTPServer(
            self.cc,
            io_loop=io_loop,
            ssl_options={'certfile': 'certs/public.crt',
                         'keyfile': 'certs/private.pem'})
        self.cc_server.listen(7654)
        self.scvs = []
        for i in range(5):
            redis_options = {'port': 2739+i, 'logfile': os.devnull}
            prop = {}
            name = 'mengsk'+str(i)
            host = '127.0.0.1:'+str(3764+i)
            prop['host'] = host
            prop['app'] = scv.SCV(name=name,
                                  external_host=host,
                                  redis_options=redis_options,
                                  mongo_options=mongo_options)
            prop['server'] = tornado.httpserver.HTTPServer(
                prop['app'], io_loop=io_loop,
                ssl_options={'certfile': 'certs/public.crt',
                             'keyfile': 'certs/private.pem'})
            prop['server'].listen(3764+i)
            self.scvs.append(prop)
        self.client = tornado.httpclient.AsyncHTTPClient(io_loop=io_loop)

    @classmethod
    def tearDownClass(self):
        super(TestSimple, self).tearDownClass()
        self.cc.db.flushdb()
        for db_name in self.cc.mdb.database_names():
            self.cc.mdb.drop_database(db_name)
        self.cc_server.stop()
        self.cc.shutdown(kill=False)
        shutil.rmtree(self.cc.data_folder)
        for scv in self.scvs:
            scv['server'].stop()
            scv['app'].db.flushdb()
            scv['app'].shutdown(kill=False)
            shutil.rmtree(scv['app'].data_folder)

    def get_new_ioloop(self):
        return tornado.ioloop.IOLoop.instance()

    def setUp(self):
        super(TestSimple, self).setUp()
        for scv in self.scvs:
            self.cc._cache_scv(scv['app'].name, scv['host'])
        token = str(uuid.uuid4())
        test_manager = "test_ws@gmail.com"
        db_body = {'_id': test_manager,
                   'token': token,
                   'role': 'manager',
                   'weight': 1
                   }
        managers = self.cc.mdb.users.managers
        managers.insert(db_body)
        self.auth_token = token
        self.manager = test_manager

    def tearDown(self):
        super(TestSimple, self).tearDown()
        self.cc.db.flushdb()
        for db_name in self.cc.mdb.database_names():
            self.cc.mdb.drop_database(db_name)
        for scv in self.scvs:
            scv['app'].db.flushdb()
            test_folder = scv['app'].streams_folder
            if os.path.exists(test_folder):
                shutil.rmtree(test_folder)

    def fetch(self, host, path, **kwargs):
        uri = 'https://'+host+path
        kwargs['validate_cert'] = common.is_domain(host)
        self.client.fetch(uri, self.stop, **kwargs)
        return self.wait()

    def _add_donor(self):
        username = 'jesse_v'
        email = 'jv@jv.com'
        password = 'test_pw'
        body = {
            'username': username,
            'email': email,
            'password': password
        }
        reply = self.fetch(self.cc_host, '/donors', method='POST',
                           body=json.dumps(body))
        self.assertEqual(reply.code, 200)
        body['token'] = json.loads(reply.body.decode())['token']
        return body

    def _post_target(self, host, engine='openmm', stage='public',
                     engine_versions=None, weight=1):
        if engine_versions is None:
            engine_versions = ['6.0']
        headers = {'Authorization': self.auth_token}
        options = {'steps_per_frame': 50000}
        body = {
            'description': 'test project',
            'engine': engine,
            'engine_versions': engine_versions,
            'stage': stage,
            'options': options,
            'weight': weight
        }
        reply = self.fetch(self.cc_host, '/targets', method='POST',
                           body=json.dumps(body), headers=headers)

        self.assertEqual(reply.code, 200)
        target_id = json.loads(reply.body.decode())['target_id']
        body['target_id'] = target_id
        return body

    def _post_stream(self, host, target_id):
        headers = {'Authorization': self.auth_token}
        rand_bin = base64.b64encode(os.urandom(1024)).decode()
        body = json.dumps({
            'target_id': target_id,
            'files': {"state.xml.gz.b64": rand_bin}
        })
        reply = self.fetch(host, '/streams', method='POST', body=body,
                           headers=headers)
        self.assertEqual(reply.code, 200)
        return json.loads(reply.body.decode())

    def _assign(self, host, target_id=None, engine='openmm',
                engine_version='6.0', donor_token=None, expected_code=200):
        body = {
            'engine': engine,
            'engine_version': engine_version,
            }
        if donor_token:
            body['donor_token'] = donor_token
        if target_id:
            body['target_id'] = target_id
        reply = self.fetch(host, '/core/assign', method='POST',
                           body=json.dumps(body))
        self.assertEqual(reply.code, expected_code)
        return json.loads(reply.body.decode())

    def _core_start(self, full_path, token):
        host = urllib.parse.urlparse(full_path).netloc
        path = urllib.parse.urlparse(full_path).path
        reply = self.fetch(host, path,
                           headers={'Authorization': token})
        self.assertEqual(reply.code, 200)
        return json.loads(reply.body.decode())

    def _core_stop(self, host, token):
        reply = self.fetch(host, '/core/stop', method='PUT', body='{}',
                           headers={'Authorization': token})
        self.assertEqual(reply.code, 200)

    def _get_target_info(self, host, target_id):
        reply = self.fetch(host, '/targets/info/'+target_id)
        self.assertEqual(reply.code, 200)
        return json.loads(reply.body.decode())

    def test_post_stream(self):
        target_id = self._post_target(self.cc_host)['target_id']
        self._post_stream(self.cc_host, target_id)
        info = self._get_target_info(self.cc_host, target_id)
        self.assertTrue(info['shards'][0] in
                        [k['app'].name for k in self.scvs])

    def test_assign_target(self):
        target_id = self._post_target(self.cc_host)['target_id']
        self._post_stream(self.cc_host, target_id)
        content = self._assign(self.cc_host, target_id)
        content = self._core_start(content['url'], content['token'])
        self.assertEqual(content['target_id'], target_id)

    def test_assign_private(self):
        content = self._post_target(self.cc_host, stage='private')
        target_id = content['target_id']
        self._post_stream(self.cc_host, target_id)
        self._assign(self.cc_host, expected_code=400)
        self._assign(self.cc_host, target_id, expected_code=200)

    def test_assign(self):
        target_id = self._post_target(self.cc_host)['target_id']
        for i in range(10):
            self._post_stream(self.cc_host, target_id)
        content = self._assign(self.cc_host)
        token, url = content['token'], content['url']
        self._core_start(url, token)
        host = urllib.parse.urlparse(url).netloc
        self._core_stop(host, token)

    def test_assign_donor(self):
        content = self._add_donor()
        token = content['token']
        target_id = self._post_target(self.cc_host)['target_id']
        self._post_stream(self.cc_host, target_id)
        self._assign(self.cc_host, donor_token=token)
        self._assign(self.cc_host, donor_token='garbage', expected_code=400)

    def test_assign_weight(self):
        weights = {}
        counters = {}
        control = [1, 6, 12]
        for w in control:
            target_id = self._post_target(self.cc_host, weight=w)['target_id']
            self._post_stream(self.cc_host, target_id)
            weights[target_id] = w
            counters[target_id] = 0
        for i in range(100):
            content = self._assign(self.cc_host)
            token, url = content['token'], content['url']
            content = self._core_start(url, token)
            target_id = content['target_id']
            host = urllib.parse.urlparse(url).netloc
            self._core_stop(host, token)
            counters[target_id] += 1
        for comb in itertools.combinations(counters, 2):
            if weights[comb[0]] > weights[comb[1]]:
                self.assertTrue(counters[comb[0]] > counters[comb[1]])
            else:
                self.assertTrue(counters[comb[0]] < counters[comb[1]])

    def test_stream_shards(self):
        k = 20
        target_id = self._post_target(self.cc_host)['target_id']
        stream_ids = set()
        for i in range(k*len(self.scvs)):
            content = self._post_stream(self.cc_host, target_id)
            stream_ids.add(content['stream_id'])
        info = self._get_target_info(self.cc_host, target_id)
        self.assertEqual(set(info['shards']),
                         set(i['app'].name for i in self.scvs))
        headers = {'Authorization': self.auth_token}
        reply = self.fetch(self.cc_host, '/targets/streams/'+target_id,
                           headers=headers)
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())
        print('DEBUG:', content)
        self.assertEqual(set(content['streams']), stream_ids)

# class TestMultiWS(tornado.testing.AsyncTestCase):
#     @classmethod
#     def setUpClass(cls):
#         super(TestMultiWS, cls).setUpClass()
#         cls.workservers = {}
#         cls.workservers['flash'] = {}
#         cls.workservers['jaedong'] = {}
#         cls.workservers['bisu'] = {}

#         rport_start = 2398
#         hport_start = 9001

#         for k, v in cls.workservers.items():
#             v['hport'] = hport_start
#             targets_folder = 'targets_folder_'+k
#             streams_folder = 'streams_folder_'+k

#             redis_options = {'port': rport_start, 'logfile': os.devnull}
#             external_options = {'external_http_port': hport_start}

#             v['ws'] = ws.WorkServer(k, redis_options=redis_options,
#                                     external_options=external_options,
#                                     targets_folder=targets_folder,
#                                     streams_folder=streams_folder)
#             rport_start += 1
#             hport_start += 1

#         cc_rport = 5872
#         cls.cc_hport = 8342

#         redis_options = {'port': cc_rport, 'logfile': os.devnull}

#         mongo_options = {
#             'host': 'localhost',
#             'port': 27017
#         }

#         cls.cc = cc.CommandCenter(cc_name='goliath',
#                                   cc_pass=None,
#                                   redis_options=redis_options,
#                                   mongo_options=mongo_options,
#                                   targets_folder='cc_targets')

#     def setUp(self):
#         super(TestMultiWS, self).setUp()
#         self.cc.mdb.users.managers.drop()
#         self.cc.mdb.community.donors.drop()
#         for k, v in self.workservers.items():
#             self.cc.add_ws(k, '127.0.0.1', v['hport'])
#             v['httpserver'] = tornado.httpserver.HTTPServer(
#                 v['ws'],
#                 io_loop=self.io_loop,
#                 ssl_options={'certfile': 'certs/public.crt',
#                              'keyfile': 'certs/private.pem'})
#             v['httpserver'].listen(v['hport'])

#         self.cc_httpserver = tornado.httpserver.HTTPServer(
#             self.cc,
#             io_loop=self.io_loop,
#             ssl_options={'certfile': 'certs/public.crt',
#                          'keyfile': 'certs/private.pem'})
#         self.cc_httpserver.listen(self.cc_hport)

#         # register a manager account
#         client = tornado.httpclient.AsyncHTTPClient(io_loop=self.io_loop)
#         url = '127.0.0.1'
#         email = 'proteneer@gmail.com'
#         password = 'test_pw_me'
#         body = {
#             'email': email,
#             'password': password,
#             'role': 'manager'
#         }

#         uri = 'https://'+url+':'+str(self.cc_hport)+'/managers'
#         client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
#                      validate_cert=common.is_domain(url))

#         rep = self.wait()
#         self.assertEqual(rep.code, 200)
#         auth = json.loads(rep.body.decode())['token']

#         self.auth, self.url, self.client = auth, url, client

#     def tearDown(self):
#         for k, v in self.workservers.items():
#             v['httpserver'].stop()
#             v['ws'].db.flushdb()
#         self.cc_httpserver.stop()
#         self.cc.mdb.users.managers.drop()

#     def test_workserver_status(self):
#         client = self.client
#         client.fetch('https://127.0.0.1:'+str(self.cc_hport)+'/ws/status',
#                      self.stop, validate_cert=False)
#         reply = self.wait()
#         self.assertEqual(reply.code, 200)
#         content = json.loads(reply.body.decode())
#         rep_servers = content.keys()
#         self.assertEqual(rep_servers, self.workservers.keys())

#     def test_specify_target(self):
#         headers = {'Authorization': self.auth}
#         client = self.client
#         url = self.url

#         available_targets = []

#         for i in range(25):
#             # post a target
#             fb1, fb2, fb3, fb4 = (base64.b64encode(os.urandom(1024)).decode()
#                                   for i in range(4))
#             description = "Diwakar and John's top secret project"
#             body = {
#                 'description': description,
#                 'files': {'system.xml.gz.b64': fb1,
#                           'integrator.xml.gz.b64': fb2},
#                 'steps_per_frame': 50000,
#                 'engine': 'openmm',
#                 'engine_versions': ['6.0'],
#                 # we should be able to get a target regardless of stage
#                 }
#             uri = 'https://127.0.0.1:'+str(self.cc_hport)+'/targets'
#             client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
#                          validate_cert=common.is_domain(url), headers=headers)
#             reply = self.wait()
#             self.assertEqual(reply.code, 200)
#             target_id = json.loads(reply.body.decode())['target_id']
#             available_targets.append(target_id)
#             # post a stream
#             uri = 'https://'+url+':'+str(self.cc_hport)+'/streams'
#             rand_bin = base64.b64encode(os.urandom(1024)).decode()
#             body = {'target_id': target_id,
#                     'files': {"state.xml.gz.b64": rand_bin}
#                     }

#             client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
#                          validate_cert=common.is_domain(url),
#                          headers=headers)
#             reply = self.wait()
#             self.assertEqual(reply.code, 200)

#         # check to make sure that the stream we activate corresponds to the
#         # target we specify
#         random.shuffle(available_targets)
#         for specific_target in available_targets:
#             body = {
#                 'engine': 'openmm',
#                 'engine_version': '6.0',
#                 'target_id': specific_target
#             }

#             uri = 'https://'+url+':'+str(self.cc_hport)+'/core/assign'
#             client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
#                          body=json.dumps(body), method='POST')
#             reply = self.wait()
#             self.assertEqual(reply.code, 200)
#             content = json.loads(reply.body.decode())
#             core_token = content['token']
#             uri = content['uri']

#             ws_headers = {'Authorization': core_token}
#             client.fetch(uri, self.stop, headers=ws_headers,
#                          validate_cert=common.is_domain(url))
#             reply = self.wait()
#             self.assertEqual(reply.code, 200)

#             content = json.loads(reply.body.decode())
#             self.assertEqual(content['target_id'], specific_target)

#     def test_post_target_restricted(self):
#         auth, url, client = self.auth, self.url, self.client
#         headers = {'Authorization': auth}
#         fb1, fb2, fb3, fb4 = (base64.b64encode(os.urandom(1024)).decode()
#                               for i in range(4))
#         description = "Diwakar and John's top secret project"
#         body = {
#             'description': description,
#             'files': {'system.xml.gz.b64': fb1, 'integrator.xml.gz.b64': fb2},
#             'steps_per_frame': 50000,
#             'engine': 'openmm',
#             'engine_versions': ['6.0'],
#             'allowed_ws': ['flash', 'jaedong'],
#             'stage': 'public'
#             }
#         uri = 'https://'+url+':'+str(self.cc_hport)+'/targets'

#         client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
#                      validate_cert=common.is_domain(url), headers=headers)
#         reply = self.wait()
#         self.assertEqual(reply.code, 200)

#         target_id = json.loads(reply.body.decode())['target_id']
#         # test POST 20 streams
#         post_streams = set()
#         for i in range(20):
#             uri = 'https://'+url+':'+str(self.cc_hport)+'/streams'
#             rand_bin = base64.b64encode(os.urandom(1024)).decode()
#             body = {'target_id': target_id,
#                     'files': {"state.xml.gz.b64": rand_bin}
#                     }
#             client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
#                          validate_cert=common.is_domain(url),
#                          headers=headers)
#             reply = self.wait()
#             self.assertEqual(reply.code, 200)
#             post_streams.add(json.loads(reply.body.decode())['stream_id'])

#         # test GET the streams
#         uri = 'https://'+url+':'+str(self.cc_hport)+'/targets/streams/'\
#               +target_id
#         client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
#                      headers=headers)
#         reply = self.wait()
#         self.assertEqual(reply.code, 200)

#         body = json.loads(reply.body.decode())
#         streams = set()
#         striated_servers = set()

#         for stream_name in body:
#             ws_name = stream_name.split(':')[1]
#             striated_servers.add(ws_name)
#             streams.add(stream_name)
#         self.assertEqual(streams, post_streams)
#         self.assertEqual(striated_servers, {'flash', 'jaedong'})

#         # test assigning
#         for i in range(5):
#             print('.', end='')
#             sys.stdout.flush()
#             body = {
#                 'engine': 'openmm',
#                 'engine_version': '6.0'
#             }
#             uri = 'https://'+url+':'+str(self.cc_hport)+'/core/assign'
#             client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
#                          body=json.dumps(body), method='POST')
#             reply = self.wait()
#             self.assertEqual(reply.code, 200)
#             content = json.loads(reply.body.decode())
#             token = content['token']
#             uri = content['uri']

#             # fetch from the WS
#             ws_headers = {'Authorization': token}
#             client.fetch(uri, self.stop, headers=ws_headers,
#                          validate_cert=common.is_domain(url))
#             rep = self.wait()
#             self.assertEqual(rep.code, 200)
#             time.sleep(1)

#         # test deleting the target
#         uri = 'https://'+url+':'+str(self.cc_hport)+'/targets/delete/'\
#               +target_id
#         client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
#                      headers=headers, method='PUT', body='')
#         reply = self.wait()
#         self.assertEqual(reply.code, 200)

#         # test GET the targets
#         uri = 'https://'+url+':'+str(self.cc_hport)+'/targets'
#         client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
#                      headers=headers)
#         reply = self.wait()
#         self.assertEqual(reply.code, 200)

#         reply_target = json.loads(reply.body.decode())['targets']
#         self.assertEqual(reply_target, [])

#         self.assertEqual(set(self.cc.db.keys('*')),
#                          set(['ws:jaedong', 'ws:flash', 'wss', 'ws:bisu']))

#         for ws in self.workservers:
#             self.assertEqual(self.workservers[ws]['ws'].db.keys('*'), [])

#     @classmethod
#     def tearDownClass(cls):
#         super(TestMultiWS, cls).tearDownClass()
#         cls.cc.db.flushdb()
#         cls.cc.shutdown(kill=False)

#         shutil.rmtree(cls.cc.data_folder)
#         for k, v in cls.workservers.items():
#             v['ws'].db.flushdb()
#             v['ws'].shutdown(kill=False)
#             shutil.rmtree(v['ws'].data_folder)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)
