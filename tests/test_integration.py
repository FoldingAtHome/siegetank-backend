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
import base64
import json
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
        core_token = str(uuid.uuid4())
        db_body = {'_id': core_token,
                   'engine': 'openmm',
                   'engine_version': '6.0'}
        cores = self.cc.mdb.cores.keys
        cores.insert(db_body)
        self.core_token = core_token
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

    def _post_target(self, host, stage='public', weight=1):
        headers = {'Authorization': self.auth_token}
        options = {'steps_per_frame': 50000}
        body = {
            'description': 'test project',
            'engine': 'openmm',
            'engine_versions': ['6.0'],
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

    def _delete_stream(self, stream_id):
        headers = {'Authorization': self.auth_token}
        scv_id = stream_id.split(':')[1]
        host = self._get_scv_host(scv_id)
        reply = self.fetch(host, '/streams/delete/'+stream_id, method='PUT',
                           headers=headers, body='')
        self.assertEqual(reply.code, 200)

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
        headers = {'Authorization': self.core_token}
        reply = self.fetch(host, '/core/assign', method='POST',
                           body=json.dumps(body), headers=headers)
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

    def _get_streams(self, host, target_id):
        # get striated scvs
        host = self.cc_host
        headers = {'Authorization': self.auth_token}
        shards = self._get_target_info(host, target_id)['shards']
        streams = []
        for scv in shards:
            host = self._get_scv_host(scv)
            reply = self.fetch(host, '/targets/streams/'+target_id,
                               headers=headers)
            self.assertEqual(reply.code, 200)
            content = json.loads(reply.body.decode())
            streams += content['streams']
        return streams

    def _get_scvs(self):
        host = self.cc_host
        reply = self.fetch(host, '/scvs/status')
        self.assertEqual(reply.code, 200)
        content = json.loads(reply.body.decode())
        return content

    def _get_scv_host(self, scv_name):
        return self._get_scvs()[scv_name]['host']

    def test_scv_status(self):
        server_scvs = self._get_scvs()
        for scv in self.scvs:
            scv_name = scv['app'].name
            scv_host = scv['host']
            self.assertEqual(server_scvs[scv_name]['host'], scv_host)

    def test_post_stream(self):
        target_id = self._post_target(self.cc_host)['target_id']
        self._post_stream(self.cc_host, target_id)
        info = self._get_target_info(self.cc_host, target_id)
        self.assertTrue(info['shards'][0] in
                        [k['app'].name for k in self.scvs])

    def test_assign_target(self):
        content = self._post_target(self.cc_host)
        target_id = content['target_id']
        options = content['options']
        self._post_stream(self.cc_host, target_id)
        content = self._assign(self.cc_host, target_id)
        self.assertEqual(content['options'], options)
        content = self._core_start(content['url'], content['token'])
        self.assertEqual(content['target_id'], target_id)

    def test_assign_private(self):
        content = self._post_target(self.cc_host, stage='private')
        target_id = content['target_id']
        options = content['options']
        self._post_stream(self.cc_host, target_id)
        self._assign(self.cc_host, expected_code=400)
        content = self._assign(self.cc_host, target_id)
        self.assertEqual(content['options'], options)

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
        scv_streams = self._get_streams(self.cc_host, target_id)
        self.assertEqual(set(scv_streams), stream_ids)

    def test_target_delete(self):
        target_id = self._post_target(self.cc_host)['target_id']
        stream_id = self._post_stream(self.cc_host, target_id)['stream_id']
        headers = {'Authorization': self.auth_token}
        reply = self.fetch(self.cc_host, '/targets/delete/'+target_id,
                           method='PUT', headers=headers, body='')
        self.assertEqual(reply.code, 400)
        self._delete_stream(stream_id)
        reply = self.fetch(self.cc_host, '/targets/delete/'+target_id,
                           method='PUT', headers=headers, body='')
        self.assertEqual(reply.code, 200)
        reply = self.fetch(self.cc_host, '/targets', headers=headers)
        self.assertEqual(reply.code, 200)
        self.assertEqual(json.loads(reply.body.decode())['targets'], [])

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    unittest.TextTestRunner(verbosity=3).run(suite)
