import tornado
import tornado.web
import tornado.httpclient
import tornado.httpserver
import tornado.testing
import tornado.gen


import unittest
import os
import shutil

import server.ws as ws
import server.cc as cc
import server.common as common
import sys
import random
import base64
import json
import time


class Test(tornado.testing.AsyncTestCase):

    @classmethod
    def setUpClass(cls):
        super(Test, cls).setUpClass()
        cls.ws_rport = 2399
        cls.ws_hport = 9029
        cls.cc_rport = 5873
        cls.cc_hport = 8343

        redis_options = {'port': cls.ws_rport, 'logfile': os.devnull}
        external_options = {'external_http_port': cls.ws_hport}
        mongo_options = {
            'host': 'localhost',
            'port': 27017
        }

        cls.ws = ws.WorkServer(ws_name='mengsk',
                               external_options=external_options,
                               redis_options=redis_options,
                               mongo_options=mongo_options,
                               targets_folder='ws_targets',
                               streams_folder='ws_streams')

        redis_options = {'port': cls.cc_rport, 'logfile': os.devnull}

        cls.cc = cc.CommandCenter(cc_name='goliath',
                                  cc_pass=None,
                                  redis_options=redis_options,
                                  mongo_options=mongo_options,
                                  targets_folder='cc_targets')

    def setUp(self):
        super(Test, self).setUp()
        self.cc.mdb.users.managers.drop()
        self.cc.mdb.community.donors.drop()
        self.cc.add_ws('mengsk', '127.0.0.1', self.ws_hport)
        self.cc_httpserver = tornado.httpserver.HTTPServer(
            self.cc,
            io_loop=self.io_loop,
            ssl_options={'certfile': 'certs/public.crt',
                         'keyfile': 'certs/private.pem'})
        self.ws_httpserver = tornado.httpserver.HTTPServer(
            self.ws,
            io_loop=self.io_loop,
            ssl_options={'certfile': 'certs/public.crt',
                         'keyfile': 'certs/private.pem'})
        self.cc_httpserver.listen(self.cc_hport)
        self.ws_httpserver.listen(self.ws_hport)

        # register a manager account
        client = tornado.httpclient.AsyncHTTPClient(io_loop=self.io_loop)
        url = '127.0.0.1'
        email = 'proteneer@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password,
            'role': 'manager'
        }
        uri = 'https://'+url+':'+str(self.cc_hport)+'/managers'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url))
        rep = self.wait()
        self.assertEqual(rep.code, 200)
        auth = json.loads(rep.body.decode())['token']

        self.auth_token = auth
        self.client = client
        self.url = '127.0.0.1'

    def tearDown(self):
        self.ws.db.flushdb()
        self.cc.db.flushdb()
        self.cc_httpserver.stop()
        self.ws_httpserver.stop()

    def test_donor_token_assign(self):
        headers = {'Authorization': self.auth_token}
        client = self.client
        url = self.url

        fb1, fb2, fb3, fb4 = (base64.b64encode(os.urandom(1024)).decode()
                              for i in range(4))
        description = "Diwakar and John's top secret project"
        body = {
            'description': description,
            'files': {'system.xml.gz.b64': fb1, 'integrator.xml.gz.b64': fb2},
            'steps_per_frame': 50000,
            'engine': 'openmm',
            'engine_versions': ['6.0'],
            'stage': 'public'
            }
        uri = 'https://127.0.0.1:'+str(self.cc_hport)+'/targets'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url), headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        target_id = json.loads(reply.body.decode())['target_id']
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets'
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        target_ids = set(json.loads(reply.body.decode())['targets'])
        self.assertEqual(target_ids, {target_id})

        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets/info/'+target_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        # test POSTing 5 streams
        post_streams = set()

        stream_binaries = {}

        for i in range(5):
            uri = 'https://'+url+':'+str(self.cc_hport)+'/streams'
            rand_bin = base64.b64encode(os.urandom(1024)).decode()
            body = {'target_id': target_id,
                    'files': {"state.xml.gz.b64": rand_bin}
                    }

            client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                         validate_cert=common.is_domain(url),
                         headers=headers)
            reply = self.wait()
            self.assertEqual(reply.code, 200)
            stream_id = json.loads(reply.body.decode())['stream_id']
            post_streams.add(stream_id)
            stream_binaries[stream_id] = rand_bin

        ######################################
        # test assigning using a donor token #
        ######################################

        # register a donor account
        client = tornado.httpclient.AsyncHTTPClient(io_loop=self.io_loop)
        username = 'random_donor'
        email = 'random_email@gmail.com'
        password = 'test_password'
        body = {
            'username': username,
            'email': email,
            'password': password
        }
        uri = 'https://'+url+':'+str(self.cc_hport)+'/donors'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url))
        rep = self.wait()
        self.assertEqual(rep.code, 200)
        auth = json.loads(rep.body.decode())['token']
        # test good donor token
        body = {
            'donor_token': auth,
            'engine': 'openmm',
            'engine_version': '6.0'
        }
        uri = 'https://'+url+':'+str(self.cc_hport)+'/core/assign'
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     body=json.dumps(body), method='POST')
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        # test bad donor token
        body = {
            'donor_token': '23p5oi235opigibberish',
            'engine': 'openmm',
            'engine_version': '6.0'
        }
        uri = 'https://'+url+':'+str(self.cc_hport)+'/core/assign'
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     body=json.dumps(body), method='POST')
        reply = self.wait()
        self.assertEqual(reply.code, 400)

    def test_private_target(self):
        client = self.client
        url = self.url
        headers = {'Authorization': self.auth_token}
        # test to make sure that we can't retrieve a private target
        fb1, fb2, fb3, fb4 = (base64.b64encode(os.urandom(1024)).decode()
                              for i in range(4))
        description = "Diwakar and John's top secret project"
        body = {
            'description': description,
            'files': {'system.xml.gz.b64': fb1, 'integrator.xml.gz.b64': fb2},
            'steps_per_frame': 50000,
            'engine': 'openmm',
            'engine_versions': ['6.0'],
            'stage': 'private'
            }
        uri = 'https://127.0.0.1:'+str(self.cc_hport)+'/targets'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url), headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        target_id_private = json.loads(reply.body.decode())['target_id']

        core_body = {
            'engine': 'openmm',
            'engine_version': '6.0'
        }

        uri = 'https://'+url+':'+str(self.cc_hport)+'/core/assign'
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     body=json.dumps(core_body), method='POST')
        reply = self.wait()
        self.assertEqual(reply.code, 400)

        body = {
            'description': description,
            'files': {'system.xml.gz.b64': fb1, 'integrator.xml.gz.b64': fb2},
            'steps_per_frame': 50000,
            'engine': 'openmm',
            'engine_versions': ['6.0'],
            'stage': 'public'
        }
        uri = 'https://127.0.0.1:'+str(self.cc_hport)+'/targets'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url), headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        target_id_public = json.loads(reply.body.decode())['target_id']

        for target_id in [target_id_public, target_id_private]:
            uri = 'https://'+url+':'+str(self.cc_hport)+'/streams'
            rand_bin = base64.b64encode(os.urandom(1024)).decode()
            body = {'target_id': target_id,
                    'files': {"state.xml.gz.b64": rand_bin}
                    }

            client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                         validate_cert=common.is_domain(url),
                         headers=headers)
            reply = self.wait()
            self.assertEqual(reply.code, 200)

        uri = 'https://'+url+':'+str(self.cc_hport)+'/core/assign'
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     body=json.dumps(core_body), method='POST')
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        token = json.loads(reply.body.decode())['token']

        headers = {'Authorization': token}

        uri = 'https://'+url+':'+str(self.ws_hport)+'/core/start'
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     method='GET', headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        core_target = json.loads(reply.body.decode())['target_id']
        self.assertEqual(core_target, target_id_public)

    def test_post_target_and_streams(self):
        headers = {'Authorization': self.auth_token}
        client = self.client
        url = self.url

        fb1, fb2, fb3, fb4 = (base64.b64encode(os.urandom(1024)).decode()
                              for i in range(4))
        description = "Diwakar and John's top secret project"
        body = {
            'description': description,
            'files': {'system.xml.gz.b64': fb1, 'integrator.xml.gz.b64': fb2},
            'steps_per_frame': 50000,
            'engine': 'openmm',
            'engine_versions': ['6.0'],
            'stage': 'public',
            }
        uri = 'https://127.0.0.1:'+str(self.cc_hport)+'/targets'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url), headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        target_id = json.loads(reply.body.decode())['target_id']
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets'
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        target_ids = set(json.loads(reply.body.decode())['targets'])
        self.assertEqual(target_ids, {target_id})

        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets/info/'+target_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        # test POSTing 20 streams
        post_streams = set()

        stream_binaries = {}

        for i in range(20):
            uri = 'https://'+url+':'+str(self.cc_hport)+'/streams'
            rand_bin = base64.b64encode(os.urandom(1024)).decode()
            body = {'target_id': target_id,
                    'files': {"state.xml.gz.b64": rand_bin}
                    }

            client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                         validate_cert=common.is_domain(url),
                         headers=headers)
            reply = self.wait()
            self.assertEqual(reply.code, 200)
            stream_id = json.loads(reply.body.decode())['stream_id']
            post_streams.add(stream_id)
            stream_binaries[stream_id] = rand_bin

        # test GET the streams
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets/streams/'\
              +target_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        body = json.loads(reply.body.decode())

        streams = set()
        for stream_name in body:
            streams.add(stream_name)
        self.assertEqual(streams, post_streams)

        # stop and start random stream
        stream_id = random.sample(streams, 1)[0]
        uri = 'https://'+url+':'+str(self.cc_hport)+'/streams/stop/'\
              +stream_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers, method='PUT', body='{}')
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        uri = 'https://'+url+':'+str(self.cc_hport)+'/streams/start/'\
              +stream_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers, method='PUT', body='{}')
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        # delete a random stream
        stream_id = random.sample(streams, 1)[0]
        uri = 'https://'+url+':'+str(self.cc_hport)+'/streams/delete/'\
              +stream_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers, method='PUT', body='{}')
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        # test GET the streams again
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets/streams/'\
              +target_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        body = json.loads(reply.body.decode())
        streams = set()
        for stream_name in body:
            streams.add(stream_name)
        post_streams.remove(stream_id)
        self.assertEqual(streams, post_streams)

        # test assigning
        body = {
            'engine': 'openmm',
            'engine_version': '6.0'
        }
        uri = 'https://'+url+':'+str(self.cc_hport)+'/core/assign'
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     body=json.dumps(body), method='POST')
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())
        token = content['token']
        uri = content['uri']

        # fetch from the WS
        ws_headers = {'Authorization': token}
        client.fetch(uri, self.stop, headers=ws_headers,
                     validate_cert=common.is_domain(url))
        rep = self.wait()
        self.assertEqual(rep.code, 200)

        content = json.loads(rep.body.decode())
        get_target_id = content['target_id']
        self.assertEqual(get_target_id, target_id)
        stream_id = content['stream_id']
        self.assertTrue(stream_id in post_streams)
        sys_file = content['target_files']['system.xml.gz.b64'].encode()
        intg_file = content['target_files']['integrator.xml.gz.b64'].encode()
        state_file = content['stream_files']['state.xml.gz.b64'].encode()
        self.assertEqual(open(os.path.join(self.ws.targets_folder, target_id,
                         'system.xml.gz.b64'), 'rb').read(), sys_file)
        self.assertEqual(open(os.path.join(self.ws.targets_folder, target_id,
                         'integrator.xml.gz.b64'), 'rb').read(), intg_file)
        self.assertEqual(open(os.path.join(self.ws.streams_folder, stream_id,
                         'state.xml.gz.b64'), 'rb').read(), state_file)
        self.assertEqual(open(os.path.join(self.ws.streams_folder, stream_id,
                         'state.xml.gz.b64'), 'rb').read(),
                         stream_binaries[stream_id].encode())

    @classmethod
    def tearDownClass(cls):
        super(Test, cls).tearDownClass()
        cls.cc.db.flushdb()
        cls.ws.db.flushdb()
        cls.cc.shutdown(kill=False)
        cls.ws.shutdown(kill=False)
        shutil.rmtree(cls.cc.data_folder)
        shutil.rmtree(cls.ws.data_folder)


class TestMultiCC(tornado.testing.AsyncTestCase):
    @classmethod
    def setupClass(cls):
        super(TestMultiCC, cls).setUpClass()
        cls.ccs = {}
        cls.ccs['flash'] = {}
        cls.ccs['jaedong'] = {}
        cls.ccs['bisu'] = {}

        rport_start = 2398
        hport_start = 9001

        mongo_options = {
            'host': 'localhost',
        }

        for k, v in cls.ccs.items():
            v['rport'] = rport_start
            v['hport'] = hport_start
            targets_folder = 'targets_folder_'+k
            redis_options = {'port': rport_start, 'logfile': os.devnull}
            v['cc'] = cc.CommandCenter(k, cc_pass=None,
                                       redis_options=redis_options,
                                       mongo_options=mongo_options,
                                       targets_folder=targets_folder)
            hport_start += 1
            rport_start += 1

        ws_rport = 5872
        cls.ws_hport = 8342

        ws_name = 'solace'
        redis_options = {'port': ws_rport, 'logfile': os.devnull}
        external_options = {'external_http_port': cls.ws_hport}

        cls.ws = ws.WorkServer(ws_name, redis_options=redis_options,
                               external_options=external_options,
                               mongo_options=mongo_options,
                               targets_folder='ws_targets_folder',
                               streams_folder='ws_streams_folder')

    def setUp(self):
        super(TestMultiCC, self).setUp()
        for k, v in self.ccs.items():
            v['cc'].mdb.users.managers.drop()
            v['cc'].mdb.community.donors.drop()
            v['cc'].add_ws(self.ws.name, '127.0.0.1', self.ws_hport)
            v['httpserver'] = tornado.httpserver.HTTPServer(
                v['cc'],
                io_loop=self.io_loop,
                ssl_options={'certfile': 'certs/public.crt',
                             'keyfile': 'certs/private.pem'})
            v['httpserver'].listen(v['hport'])

        self.ws_httpserver = tornado.httpserver.HTTPServer(
            self.ws,
            io_loop=self.io_loop,
            ssl_options={'certfile': 'certs/public.crt',
                         'keyfile': 'certs/private.pem'})
        self.ws_httpserver.listen(self.ws_hport)

        # register a manager account
        client = tornado.httpclient.AsyncHTTPClient(io_loop=self.io_loop)
        url = '127.0.0.1'
        email = 'proteneer@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password,
            'role': 'manager'
        }

        uri = 'https://'+url+':'+str(self.ccs['flash']['hport'])+'/managers'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url))
        rep = self.wait()
        self.assertEqual(rep.code, 200)
        auth = json.loads(rep.body.decode())['token']

        self.auth, self.url, self.client = auth, url, client

    def test_post_streams(self):
        headers = {'Authorization': self.auth}
        client = self.client
        url = self.url
        total_targets = set()
        total_streams = set()
        for cc_name, cc_prop in self.ccs.items():
            fb1, fb2, fb3, fb4 = (base64.b64encode(os.urandom(1024)).decode()
                                  for i in range(4))
            description = "Diwakar and John's top secret project"
            body = {
                'description': description,
                'files': {'system.xml.gz.b64': fb1,
                          'integrator.xml.gz.b64': fb2},
                'steps_per_frame': 50000,
                'engine': 'openmm',
                'engine_versions': ['6.0'],
                }
            port = cc_prop['hport']
            uri = 'https://'+url+':'+str(port)+'/targets'
            client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                         validate_cert=common.is_domain(url), headers=headers)
            reply = self.wait()
            self.assertEqual(reply.code, 200)
            target_id = json.loads(reply.body.decode())['target_id']
            total_targets.add(target_id)

            for i in range(5):
                uri = 'https://'+url+':'+str(port)+'/streams'
                rand_bin = base64.b64encode(os.urandom(1024)).decode()
                body = {'target_id': target_id,
                        'files': {"state.xml.gz.b64": rand_bin}
                        }

                client.fetch(uri, self.stop, method='POST',
                             body=json.dumps(body),
                             validate_cert=common.is_domain(url),
                             headers=headers)
                reply = self.wait()
                self.assertEqual(reply.code, 200)
                total_streams.add(json.loads(reply.body.decode())['stream_id'])

        workserver_streams = set()
        for target in total_targets:
            uri = 'https://'+url+':'+str(self.ws_hport)+\
                '/targets/streams/'+target
            client.fetch(uri, self.stop, validate_cert=common.is_domain(url))
            reply = self.wait()
            self.assertEqual(reply.code, 200)
            streams_found = json.loads(reply.body.decode())
            for stream_id, prop in streams_found.items():
                workserver_streams.add(stream_id)

        self.assertEqual(workserver_streams, total_streams)

    def tearDown(self):
        super(TestMultiCC, self).tearDown()
        for k, v in self.ccs.items():
            v['httpserver'].stop()
            v['cc'].db.flushdb()
            v['cc'].mdb.users.managers.drop()
        self.ws_httpserver.stop()

    @classmethod
    def tearDownClass(cls):
        super(TestMultiCC, cls).tearDownClass()
        cls.ws.db.flushdb()
        cls.ws.shutdown(kill=False)
        shutil.rmtree(cls.ws.data_folder)
        for k, v in cls.ccs.items():
            v['cc'].db.flushdb()
            v['cc'].shutdown(kill=False)
            shutil.rmtree(v['cc'].data_folder)


class TestMultiWS(tornado.testing.AsyncTestCase):
    @classmethod
    def setUpClass(cls):
        super(TestMultiWS, cls).setUpClass()
        cls.workservers = {}
        cls.workservers['flash'] = {}
        cls.workservers['jaedong'] = {}
        cls.workservers['bisu'] = {}

        rport_start = 2398
        hport_start = 9001

        for k, v in cls.workservers.items():
            v['hport'] = hport_start
            targets_folder = 'targets_folder_'+k
            streams_folder = 'streams_folder_'+k

            redis_options = {'port': rport_start, 'logfile': os.devnull}
            external_options = {'external_http_port': hport_start}

            v['ws'] = ws.WorkServer(k, redis_options=redis_options,
                                    external_options=external_options,
                                    targets_folder=targets_folder,
                                    streams_folder=streams_folder)
            rport_start += 1
            hport_start += 1

        cc_rport = 5872
        cls.cc_hport = 8342

        redis_options = {'port': cc_rport, 'logfile': os.devnull}

        mongo_options = {
            'host': 'localhost',
            'port': 27017
        }

        cls.cc = cc.CommandCenter(cc_name='goliath',
                                  cc_pass=None,
                                  redis_options=redis_options,
                                  mongo_options=mongo_options,
                                  targets_folder='cc_targets')

    def setUp(self):
        super(TestMultiWS, self).setUp()
        self.cc.mdb.users.managers.drop()
        self.cc.mdb.community.donors.drop()
        for k, v in self.workservers.items():
            self.cc.add_ws(k, '127.0.0.1', v['hport'])
            v['httpserver'] = tornado.httpserver.HTTPServer(
                v['ws'],
                io_loop=self.io_loop,
                ssl_options={'certfile': 'certs/public.crt',
                             'keyfile': 'certs/private.pem'})
            v['httpserver'].listen(v['hport'])

        self.cc_httpserver = tornado.httpserver.HTTPServer(
            self.cc,
            io_loop=self.io_loop,
            ssl_options={'certfile': 'certs/public.crt',
                         'keyfile': 'certs/private.pem'})
        self.cc_httpserver.listen(self.cc_hport)

        # register a manager account
        client = tornado.httpclient.AsyncHTTPClient(io_loop=self.io_loop)
        url = '127.0.0.1'
        email = 'proteneer@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password,
            'role': 'manager'
        }

        uri = 'https://'+url+':'+str(self.cc_hport)+'/managers'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url))

        rep = self.wait()
        self.assertEqual(rep.code, 200)
        auth = json.loads(rep.body.decode())['token']

        self.auth, self.url, self.client = auth, url, client

    def tearDown(self):
        for k, v in self.workservers.items():
            v['httpserver'].stop()
            v['ws'].db.flushdb()
        self.cc_httpserver.stop()
        self.cc.mdb.users.managers.drop()

    def test_workserver_status(self):
        client = self.client
        client.fetch('https://127.0.0.1:'+str(self.cc_hport)+'/ws/status',
                     self.stop, validate_cert=False)
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())
        rep_servers = content.keys()
        self.assertEqual(rep_servers, self.workservers.keys())

    def test_specify_target(self):
        headers = {'Authorization': self.auth}
        client = self.client
        url = self.url

        available_targets = []

        for i in range(25):
            # post a target
            fb1, fb2, fb3, fb4 = (base64.b64encode(os.urandom(1024)).decode()
                                  for i in range(4))
            description = "Diwakar and John's top secret project"
            body = {
                'description': description,
                'files': {'system.xml.gz.b64': fb1,
                          'integrator.xml.gz.b64': fb2},
                'steps_per_frame': 50000,
                'engine': 'openmm',
                'engine_versions': ['6.0'],
                # we should be able to get a target regardless of stage
                }
            uri = 'https://127.0.0.1:'+str(self.cc_hport)+'/targets'
            client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                         validate_cert=common.is_domain(url), headers=headers)
            reply = self.wait()
            self.assertEqual(reply.code, 200)
            target_id = json.loads(reply.body.decode())['target_id']
            available_targets.append(target_id)
            # post a stream
            uri = 'https://'+url+':'+str(self.cc_hport)+'/streams'
            rand_bin = base64.b64encode(os.urandom(1024)).decode()
            body = {'target_id': target_id,
                    'files': {"state.xml.gz.b64": rand_bin}
                    }

            client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                         validate_cert=common.is_domain(url),
                         headers=headers)
            reply = self.wait()
            self.assertEqual(reply.code, 200)

        # check to make sure that the stream we activate corresponds to the
        # target we specify
        random.shuffle(available_targets)
        for specific_target in available_targets:
            body = {
                'engine': 'openmm',
                'engine_version': '6.0',
                'target_id': specific_target
            }

            uri = 'https://'+url+':'+str(self.cc_hport)+'/core/assign'
            client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                         body=json.dumps(body), method='POST')
            reply = self.wait()
            self.assertEqual(reply.code, 200)
            content = json.loads(reply.body.decode())
            core_token = content['token']
            uri = content['uri']

            ws_headers = {'Authorization': core_token}
            client.fetch(uri, self.stop, headers=ws_headers,
                         validate_cert=common.is_domain(url))
            reply = self.wait()
            self.assertEqual(reply.code, 200)

            content = json.loads(reply.body.decode())
            self.assertEqual(content['target_id'], specific_target)

    def test_post_target_restricted(self):
        auth, url, client = self.auth, self.url, self.client
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
            'allowed_ws': ['flash', 'jaedong'],
            'stage': 'public'
            }
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets'

        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url), headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        target_id = json.loads(reply.body.decode())['target_id']
        # test POST 20 streams
        post_streams = set()
        for i in range(20):
            uri = 'https://'+url+':'+str(self.cc_hport)+'/streams'
            rand_bin = base64.b64encode(os.urandom(1024)).decode()
            body = {'target_id': target_id,
                    'files': {"state.xml.gz.b64": rand_bin}
                    }
            client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                         validate_cert=common.is_domain(url),
                         headers=headers)
            reply = self.wait()
            self.assertEqual(reply.code, 200)
            post_streams.add(json.loads(reply.body.decode())['stream_id'])

        # test GET the streams
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets/streams/'\
              +target_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        body = json.loads(reply.body.decode())
        streams = set()
        striated_servers = set()

        for stream_name in body:
            ws_name = stream_name.split(':')[1]
            striated_servers.add(ws_name)
            streams.add(stream_name)
        self.assertEqual(streams, post_streams)
        self.assertEqual(striated_servers, {'flash', 'jaedong'})

        # test assigning
        for i in range(5):
            print('.', end='')
            sys.stdout.flush()
            body = {
                'engine': 'openmm',
                'engine_version': '6.0'
            }
            uri = 'https://'+url+':'+str(self.cc_hport)+'/core/assign'
            client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                         body=json.dumps(body), method='POST')
            reply = self.wait()
            self.assertEqual(reply.code, 200)
            content = json.loads(reply.body.decode())
            token = content['token']
            uri = content['uri']

            # fetch from the WS
            ws_headers = {'Authorization': token}
            client.fetch(uri, self.stop, headers=ws_headers,
                         validate_cert=common.is_domain(url))
            rep = self.wait()
            self.assertEqual(rep.code, 200)
            time.sleep(1)

        # test deleting the target
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets/delete/'\
              +target_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers, method='PUT', body='')
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        # test GET the targets
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets'
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)

        reply_target = json.loads(reply.body.decode())['targets']
        self.assertEqual(reply_target, [])

        self.assertEqual(set(self.cc.db.keys('*')),
                         set(['ws:jaedong', 'ws:flash', 'wss', 'ws:bisu']))

        for ws in self.workservers:
            self.assertEqual(self.workservers[ws]['ws'].db.keys('*'), [])

    @classmethod
    def tearDownClass(cls):
        super(TestMultiWS, cls).tearDownClass()
        cls.cc.db.flushdb()
        cls.cc.shutdown(kill=False)

        shutil.rmtree(cls.cc.data_folder)
        for k, v in cls.workservers.items():
            v['ws'].db.flushdb()
            v['ws'].shutdown(kill=False)
            shutil.rmtree(v['ws'].data_folder)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)
