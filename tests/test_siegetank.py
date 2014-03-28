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

import siegetank.base
import requests


class TestSiegeTank(unittest.TestCase):
    def setUp(self):
        cc_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', 'cc')
        ws_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', 'ws')
        self.pid1 = subprocess.Popen(cc_path, stdout=open(os.devnull),
            stderr=open(os.devnull),
            shell=True, preexec_fn=lambda: os.setpgid(0, 0))

        time.sleep(1)
        self.pid2 = subprocess.Popen(ws_path, stdout=open(os.devnull),
            stderr=open(os.devnull),
            shell=True, preexec_fn=lambda: os.setpgid(0, 0))

        self.cc_uri = '127.0.0.1:8980'

        # try adding a user
        requests.post('https://127.0.0.1:8980/managers',
                      data=json.dumps({'email': 'test_user@gmail.com',
                                       'password': 'test_pass',
                                       'role': 'manager'}),
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

        options = {'steps_per_frame': 10000}
        engine = 'openmm'
        engine_versions = ['5.5', '9.9']
        description = 'some test case'
        files = {'system.xml.gz.b64': encoded_system,
                 'integrator.xml.gz.b64': encoded_intg
                 }

        creation_time = time.time()
        target = siegetank.base.add_target(cc_uri=self.cc_uri,
                                           options=options,
                                           engine=engine,
                                           engine_versions=engine_versions,
                                           description=description,
                                           stage='public',
                                           files=files
                                           )

        self.assertEqual(target.options, options)
        self.assertEqual(target.engine, engine)
        self.assertEqual(set(target.engine_versions), set(engine_versions))
        self.assertEqual(target.description, target.description)
        self.assertEqual(target.allowed_ws, [])
        self.assertAlmostEqual(target.creation_date, creation_time, places=0)
        target_ids = set()
        for k in siegetank.get_targets(self.cc_uri):
            target_ids.add(k.id)
        self.assertEqual(target_ids, {target.id})
        self.assertEqual(target.download('system.xml.gz.b64'),
                         encoded_system.encode())
        self.assertEqual(target.download('integrator.xml.gz.b64'),
                         encoded_intg.encode())

        for i in range(50):
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

        # finish to make sure we dont have this extra set

        target.delete()
        self.assertEqual(siegetank.get_targets(self.cc_uri), set())
