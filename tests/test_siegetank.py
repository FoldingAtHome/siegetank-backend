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
        self.cc_uri = '127.0.0.1:8980'
        time.sleep(2)

        # try adding a user
        requests.post('https://127.0.0.1:8980/managers',
                      data=json.dumps({'email': 'test_user@gmail.com',
                                       'password': 'test_pass',
                                       'role': 'manager',
                                       'weight': 1}),
                      verify=False)
        token = siegetank.generate_token(self.cc_uri, 'test_user@gmail.com',
                                         'test_pass')
        siegetank.login(self.cc_uri, token)

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
        state_url = 'http://www.stanford.edu/~yutongz/state.xml.gz'
        system_url = 'http://www.stanford.edu/~yutongz/system.xml.gz'
        integrator_url = 'http://www.stanford.edu/~yutongz/integrator.xml.gz'

        state_gz = requests.get(state_url).content
        system_gz = requests.get(system_url).content
        integrator_gz = requests.get(integrator_url).content

        encoded_state = base64.b64encode(state_gz).decode()
        encoded_system = base64.b64encode(system_gz).decode()
        encoded_intg = base64.b64encode(integrator_gz).decode()

        options = {'description': 'siegetank_demo', 'steps_per_frame': 10000}
        engines = ['openmm_60_opencl', 'openmm_60_cuda']
        files = {'system.xml.gz.b64': encoded_system,
                 'integrator.xml.gz.b64': encoded_intg
                 }
        weight = 5
        creation_time = time.time()
        target = siegetank.base.add_target(cc_uri=self.cc_uri,
                                           options=options,
                                           engines=engines,
                                           stage='public',
                                           files=files,
                                           weight=weight,
                                           )

        self.assertEqual(target.options, options)
        self.assertEqual(target.engines, engines)
        self.assertEqual(target.weight, weight)
        self.assertAlmostEqual(target.creation_date, creation_time, places=0)
        target_ids = set()
        for k in siegetank.get_targets(self.cc_uri):
            target_ids.add(k.id)
        self.assertEqual(target_ids, {target.id})
        for i in range(20):
            target.add_stream(files={'state.xml.gz.b64': encoded_state})
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
        target.delete()
        self.assertEqual(siegetank.get_targets(self.cc_uri), set())
