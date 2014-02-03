import tornado
import tornado.web
import tornado.httpclient
import tornado.httpserver
import tornado.testing
import tornado.gen

import unittest
import os
import shutil
import ws
import cc
import sys
import random
import base64
import json
import time
import common


class Test(tornado.testing.AsyncTestCase):

    @classmethod
    def setUpClass(cls):
        super(Test, cls).setUpClass()
        cls.ws_rport = 2398
        cls.ws_hport = 9028
        cls.cc_rport = 5872
        cls.cc_hport = 8342
        cls.ws = ws.WorkServer('mengsk', redis_port=cls.ws_rport,
                               targets_folder='ws_targets',
                               streams_folder='ws_streams',
                               debug=True)
        cls.cc = cc.CommandCenter('goliath', redis_port=cls.cc_rport,
                                  targets_folder='cc_targets', debug=True)

    def setUp(self):
        super(Test, self).setUp()
        self.cc.add_ws('mengsk', '127.0.0.1', self.ws_hport, self.ws_rport)
        self.cc_httpserver = tornado.httpserver.HTTPServer(
            self.cc,
            io_loop=self.io_loop,
            ssl_options={'certfile': 'certs/cc.crt',
                         'keyfile': 'certs/cc.key'})
        self.ws_httpserver = tornado.httpserver.HTTPServer(
            self.ws,
            io_loop=self.io_loop,
            ssl_options={'certfile': 'certs/ws.crt',
                         'keyfile': 'certs/ws.key'})
        self.cc_httpserver.listen(self.cc_hport)
        self.ws_httpserver.listen(self.ws_hport)

    def tearDown(self):
        self.cc_httpserver.stop()
        self.ws_httpserver.stop()
        self.cc.mdb.managers.drop()

    def test_post_target_and_streams(self):
        # register an account
        client = tornado.httpclient.AsyncHTTPClient(io_loop=self.io_loop)
        url = '127.0.0.1'
        email = 'proteneer@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password
        }
        uri = 'https://'+url+':'+str(self.cc_hport)+'/managers'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url))
        rep = self.wait()
        self.assertEqual(rep.code, 200)
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
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets'
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
        for k, v in body.items():
            streams.add(k)
        self.assertEqual(streams, post_streams)

        # delete a random stream
        stream_id = random.sample(streams, 1)[0]
        uri = 'https://'+url+':'+str(self.cc_hport)+'/streams/delete/'\
              +stream_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers, method='PUT', body='{}')
        reply = self.wait()

        # test GET the streams again
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets/streams/'\
              +target_id
        client.fetch(uri, self.stop, validate_cert=common.is_domain(url),
                     headers=headers)
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        body = json.loads(reply.body.decode())
        streams = set()
        for k, v in body.items():
            streams.add(k)
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
        folders = [cls.ws.targets_folder, cls.ws.streams_folder,
                   cls.cc.targets_folder]
        for folder in folders:
            if os.path.exists(folder):
                shutil.rmtree(folder)


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
            v['rport'] = rport_start
            v['hport'] = hport_start
            targets_folder = 'targets_folder_'+k
            streams_folder = 'streams_folder_'+k
            v['targets_folder'] = targets_folder
            v['streams_folder'] = streams_folder
            v['ws'] = ws.WorkServer(k, redis_port=rport_start,
                                    targets_folder=targets_folder,
                                    streams_folder=streams_folder)
            rport_start += 1
            hport_start += 1

        cls.cc_rport = 5872
        cls.cc_hport = 8342

        cls.cc = cc.CommandCenter('goliath', redis_port=cls.cc_rport,
                                  targets_folder='cc_targets')

    def setUp(self):
        super(TestMultiWS, self).setUp()
        for k, v in self.workservers.items():
            self.cc.add_ws(k, '127.0.0.1', v['hport'], v['rport'])
            v['httpserver'] = tornado.httpserver.HTTPServer(
                v['ws'],
                io_loop=self.io_loop,
                ssl_options={'certfile': 'certs/ws.crt',
                             'keyfile': 'certs/ws.key'})
            v['httpserver'].listen(v['hport'])

        self.cc_httpserver = tornado.httpserver.HTTPServer(
            self.cc,
            io_loop=self.io_loop,
            ssl_options={'certfile': 'certs/cc.crt',
                         'keyfile': 'certs/cc.key'})
        self.cc_httpserver.listen(self.cc_hport)

    def tearDown(self):
        for k, v in self.workservers.items():
            v['httpserver'].stop()
        self.cc_httpserver.stop()
        self.cc.mdb.managers.drop()
        pass

    def test_post_target_restricted(self):
        # register an account
        client = tornado.httpclient.AsyncHTTPClient(io_loop=self.io_loop)
        url = '127.0.0.1'
        email = 'proteneer@gmail.com'
        password = 'test_pw_me'
        body = {
            'email': email,
            'password': password
        }
        uri = 'https://'+url+':'+str(self.cc_hport)+'/managers'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=common.is_domain(url))
        rep = self.wait()
        self.assertEqual(rep.code, 200)
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
            'allowed_ws': ['flash', 'jaedong'],
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
        for k, v in body.items():
            streams.add(k)
            striated_servers.add(v[2])
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

    @classmethod
    def tearDownClass(cls):
        super(TestMultiWS, cls).tearDownClass()
        cls.cc.db.flushdb()
        cls.cc.shutdown(kill=False)

        folders = [cls.cc.targets_folder]

        for k, v in cls.workservers.items():
            v['ws'].db.flushdb()
            v['ws'].shutdown(kill=False)
            folders.append(v['targets_folder'])
            folders.append(v['streams_folder'])

        for folder in folders:
            if os.path.exists(folder):
                shutil.rmtree(folder)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)
