import unittest
import os
import shutil

import tornado
import tornado.web
import tornado.httpclient
import tornado.testing
import tornado.gen

import ws
import cc
import sys

import base64
import json


class Test(tornado.testing.AsyncTestCase):

    @classmethod
    def setUpClass(cls):
        super(Test, cls).setUpClass()
        cls.ws_rport = 2398
        cls.cc_rport = 5872
        cls.ws_hport = 9028
        cls.cc_hport = 8342
        cls.ws = ws.WorkServer('mengsk', redis_port=cls.ws_rport)
        cls.cc = cc.CommandCenter('goliath', redis_port=cls.cc_rport,
                                  targets_folder='cc_targets')

    def setUp(self):
        super(Test, self).setUp()
        self.cc.add_ws('mengsk', '127.0.0.1', self.ws_hport, self.ws_rport)
        self.cc.listen(self.cc_hport, io_loop=self.io_loop, ssl_options={
            'certfile': 'certs/ws.crt', 'keyfile': 'certs/ws.key'})
        self.ws.listen(self.ws_hport, io_loop=self.io_loop, ssl_options={
            'certfile': 'certs/cc.crt', 'keyfile': 'certs/cc.key'})

    def test_post_target_and_streams(self):
        client = tornado.httpclient.AsyncHTTPClient(io_loop=self.io_loop)
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
        url = '127.0.0.1'
        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets'
        client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                     validate_cert=cc._is_domain(url))
        reply = self.wait()

        self.assertEqual(reply.code, 200)
        target_id = json.loads(reply.body.decode())['target_id']

        # test POSTing 20 streams
        for i in range(20):
            uri = 'https://'+url+':'+str(self.cc_hport)+'/streams'
            rand_bin = base64.b64encode(os.urandom(1024)).decode()
            body = {'target_id': target_id,
                    'files': {"state.xml.gz.b64": rand_bin}
                    }

            client.fetch(uri, self.stop, method='POST', body=json.dumps(body),
                         validate_cert=cc._is_domain(url))
            reply = self.wait()
            self.assertEqual(reply.code, 200)

        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets'

        client.fetch(uri, self.stop, validate_cert=cc._is_domain(url))
        reply = self.wait()
        self.assertEqual(reply.code, 200)
        target_ids = set(json.loads(reply.body.decode())['targets'])
        self.assertEqual(target_ids, {target_id})

        uri = 'https://'+url+':'+str(self.cc_hport)+'/targets/info/'+target_id
        client.fetch(uri, self.stop, validate_cert=cc._is_domain(url))
        reply = self.wait()
        print(reply.body)

    @classmethod
    def tearDownClass(cls):
        super(Test, cls).tearDownClass()
        cls.cc.db.flushdb()
        cls.ws.db.flushdb()
        cls.cc.shutdown(kill=False)
        cls.ws.shutdown(kill=False)
        folders = ['streams', 'targets', cls.cc.targets_folder]
        for folder in folders:
            if os.path.exists(folder):
                shutil.rmtree(folder)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])
    #suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    #suite.addTest(WSInitTestCase())
    unittest.TextTestRunner(verbosity=3).run(suite)
