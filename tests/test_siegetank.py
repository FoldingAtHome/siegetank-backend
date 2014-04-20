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
import subprocess
import os
import signal
import time
import base64
import json
import shutil
import glob
import random
import pymongo

import siegetank.base
import requests


class TestSiegeTank(unittest.TestCase):
    def setUp(self):
        cc_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', 'cc')
        scv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', 'scv')

        self.pid2 = subprocess.Popen(scv_path, #stdout=open(os.devnull),
            #stderr=open(os.devnull),
            shell=True, preexec_fn=lambda: os.setpgid(0, 0))
        time.sleep(2)
        self.pid1 = subprocess.Popen(cc_path, #stdout=open(os.devnull),
            #stderr=open(os.devnull),
            shell=True, preexec_fn=lambda: os.setpgid(0, 0))
        cc_uri = '127.0.0.1:8980'
        time.sleep(2)

        # try adding a user
        requests.post('https://127.0.0.1:8980/managers',
                      data=json.dumps({'email': 'test_user@gmail.com',
                                       'password': 'test_pass',
                                       'role': 'manager',
                                       'weight': 1}),
                      verify=False)
        token = siegetank.generate_token(cc_uri, 'test_user@gmail.com',
                                         'test_pass')
        siegetank.login(cc_uri, token)

    def tearDown(self):
        try:
            os.killpg(self.pid1.pid, signal.SIGTERM)
        except Exception as e:
            print(e)
            pass
        try:
            os.killpg(self.pid2.pid, signal.SIGTERM)
        except Exception as e:
            print(e)
            pass
        time.sleep(1)
        for data_folder in glob.glob('*_data'):
            shutil.rmtree(data_folder)
        pass

        mdb = pymongo.MongoClient()
        for db_name in mdb.database_names():
            mdb.drop_database(db_name)

    def test_add_target(self):
        options = {'description': 'siegetank_demo', 'steps_per_frame': 10000}
        engines = ['openmm_60_opencl', 'openmm_60_cuda']

        weight = 5
        creation_time = time.time()
        target = siegetank.base.add_target(options=options,
                                           engines=engines,
                                           stage='public',
                                           weight=weight,
                                           )

        self.assertEqual(target.options, options)
        self.assertEqual(target.engines, engines)
        self.assertEqual(target.weight, weight)
        self.assertAlmostEqual(target.creation_date, creation_time, places=0)
        target_ids = set()
        for k in siegetank.get_targets():
            target_ids.add(k.id)
        self.assertEqual(target_ids, {target.id})
        encoded_state = 'some_binary1'
        encoded_system = 'some_binary2'
        encoded_intg = 'some_binary3'
        files = {'system.xml.gz.b64': encoded_system,
                 'integrator.xml.gz.b64': encoded_intg,
                 'state.xml.gz.b64': encoded_state}
        for i in range(20):
            target.add_stream(files=files)
        stream = random.sample(target.streams, 1)[0]
        self.assertEqual(stream.status, 'OK')
        self.assertEqual(stream.frames, 0)
        self.assertEqual(stream.download('state.xml.gz.b64'),
                         encoded_state.encode())
        self.assertEqual(stream.active, False)
        new_binary = base64.b64encode(b'hehehe').decode()
        stream.stop()
        stream.replace('state.xml.gz.b64', new_binary)
        self.assertEqual(stream.download('state.xml.gz.b64'),
                         new_binary.encode())
        correct_ids = set()
        for s in target.streams:
            correct_ids.add(s.id)
        correct_ids.remove(stream.id)
        stream.delete()
        test_ids = set()
        for s in target.streams:
            test_ids.add(s.id)
        self.assertEqual(correct_ids, test_ids)
        for stream in target.streams:
            stream.delete()
        new_engines = ['a', 'b']
        new_options = {
            'foo': 'bar'
        }
        new_stage = 'private'
        options.update(new_options)
        target.update(options=new_options, engines=new_engines,
                      stage=new_stage)
        self.assertEqual(target.options, options)
        self.assertEqual(target.engines, new_engines)
        self.assertEqual(target.stage, new_stage)
        self.assertEqual(target.weight, weight)

        target.delete()
        self.assertEqual(siegetank.get_targets(), set())
