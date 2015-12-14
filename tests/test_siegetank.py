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
# import base64
import shutil
import glob
import random
import pymongo
import psutil
import hashlib

import siegetank.base
import tests.utils


class TestSiegeTank(unittest.TestCase):

    def setUp(self):
        super(TestSiegeTank, self).setUp()
        mdb = pymongo.MongoClient()
        for db_name in mdb.database_names():
            mdb.drop_database(db_name)

        cc_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               '..', 'cc_bin')
        scv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                '..', 'scv_bin')

        self.pid2 = subprocess.Popen(scv_path,  # stdout=open(os.devnull),
                                     # stderr=open(os.devnull),
                                     shell=True,
                                     preexec_fn=lambda: os.setpgid(0, 0))
        time.sleep(1)
        self.pid1 = subprocess.Popen(cc_path,  # stdout=open(os.devnull),
                                     # stderr=open(os.devnull),
                                     shell=True,
                                     preexec_fn=lambda: os.setpgid(0, 0))
        # cc_uri = '127.0.0.1:8980'
        time.sleep(3)

        result = tests.utils.add_user(manager=True)
        siegetank.login(result['token'], '127.0.0.1:8980')

    def tearDown(self):
        super(TestSiegeTank, self).tearDown()

        p = psutil.Process(int(self.pid1.pid))
        child_pid = p.get_children(recursive=True)
        for pid in child_pid:
            if pid.name() == p.name():
                os.kill(pid.pid, signal.SIGTERM)

        time.sleep(1)
        for data_folder in glob.glob('*_data'):
            try:
                shutil.rmtree(data_folder)
            except:
                print('Unable to remove', data_folder)
                pass
        mdb = pymongo.MongoClient()
        for db_name in mdb.database_names():
            mdb.drop_database(db_name)

    def test_sync_stream(self):

        def add_partition(stream_id, partitions):
            stream_dir = os.path.join('firebat_data', 'streams', stream_id)
            for i in partitions:
                p_dir = os.path.join(stream_dir, str(i), '0')
                os.makedirs(p_dir)
                open(os.path.join(p_dir, 'bin1'), 'wb').write(os.urandom(5987))
                open(os.path.join(p_dir, 'bin2'), 'wb').write(os.urandom(9820))

                os.makedirs(os.path.join(p_dir, 'checkpoint_files'))
                open(os.path.join(p_dir, 'checkpoint_files',
                                  'cin1'), 'wb').write(os.urandom(2048))
                open(os.path.join(p_dir, 'checkpoint_files',
                                  'cin2'), 'wb').write(os.urandom(2048))

        def remove_local_partition(stream_id, partition, filename=None):
            stream_dir = os.path.join('sync_data', stream_id)
            if filename:
                os.remove(os.path.join(stream_dir, str(partition), filename))
            else:
                shutil.rmtree(os.path.join(stream_dir, str(partition)))

        def remove_local_seed_file(stream_id, filename):
            stream_dir = os.path.join('sync_data', stream_id)
            os.remove(os.path.join(stream_dir, 'files', filename))

        def are_frame_dirs_equal(sync_dir, stream_dir):
            """ Does not check for seed_files or checkpoint_files. """
            for partition in os.listdir(stream_dir):
                if partition == 'files':
                    continue
                for frame_file in os.listdir(
                    os.path.join(
                        stream_dir,
                        partition,
                        '0')):
                    if frame_file == 'checkpoint_files':
                        continue
                    bin1 = open(
                        os.path.join(
                            stream_dir,
                            partition,
                            '0',
                            frame_file),
                        'rb')
                    try:
                        bin2 = open(
                            os.path.join(
                                sync_dir,
                                partition,
                                frame_file),
                            'rb')
                    except:
                        return False
                    if bin1.read() != bin2.read():
                        return False
            return True

        weight = 5
        options = {'description': 'siegetank_demo', 'steps_per_frame': 10000}
        engines = ['openmm_60_opencl', 'openmm_60_cuda']
        target = siegetank.base.add_target(options=options,
                                           engines=engines,
                                           stage='public',
                                           weight=weight,
                                           )
        encoded_state = 'some_binary1'
        encoded_system = 'some_binary2'
        encoded_intg = 'some_binary3'
        files = {'system.xml.gz.b64': encoded_system,
                 'integrator.xml.gz.b64': encoded_intg,
                 'state.xml.gz.b64': encoded_state}
        siegetank.base.refresh_scvs()
        random_scv = random.choice(list(siegetank.base.scvs.keys()))
        for i in range(3):
            target.add_stream(files, random_scv)
        target.add_stream(files, random_scv)
        stream = random.sample(target.streams, 1)[0]
        sync_dir = os.path.join('sync_data', stream.id)
        stream_dir = os.path.join('firebat_data', 'streams', stream.id)
        stream.sync(sync_dir)
        self.assertTrue(are_frame_dirs_equal(sync_dir, stream_dir))
        add_partition(stream.id, [4, 9, 34, 493])
        self.assertEqual(stream.partitions, [4, 9, 34, 493])
        # test stream sync
        stream.sync(sync_dir)
        self.assertTrue(are_frame_dirs_equal(sync_dir, stream_dir))
        add_partition(stream.id, [589, 2098, 29038])
        self.assertEqual(stream.partitions, [4, 9, 34, 493, 589, 2098, 29038])
        stream.sync(sync_dir)
        self.assertTrue(are_frame_dirs_equal(sync_dir, stream_dir))
        remove_local_partition(stream.id, 589)
        self.assertFalse(are_frame_dirs_equal(sync_dir, stream_dir))
        stream.sync(sync_dir)
        self.assertTrue(are_frame_dirs_equal(sync_dir, stream_dir))
        remove_local_partition(stream.id, 34, 'bin1')
        self.assertFalse(are_frame_dirs_equal(sync_dir, stream_dir))
        stream.sync(sync_dir)
        self.assertTrue(are_frame_dirs_equal(sync_dir, stream_dir))

        shutil.rmtree(sync_dir)

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
        self.assertEqual(target.owner, 'proteneer')
        self.assertEqual(target.options, options)
        self.assertEqual(target.engines, engines)
        self.assertEqual(target.weight, weight)
        self.assertAlmostEqual(target.creation_date, creation_time, places=0)
        target_ids = set()
        for k in siegetank.list_targets():
            target_ids.add(k.id)
        self.assertEqual(target_ids, {target.id})
        encoded_state = 'some_binary1'
        encoded_system = 'some_binary2'
        encoded_intg = 'some_binary3'
        files = {'system.xml.gz.b64': encoded_system,
                 'integrator.xml.gz.b64': encoded_intg,
                 'state.xml.gz.b64': encoded_state}
        siegetank.base.refresh_scvs()

        random_scv = random.choice(list(siegetank.base.scvs.keys()))
        for i in range(20):
            target.add_stream(files, random_scv)

        tags = {'pdb.gz.b64': hashlib.md5(os.urandom(1024)).hexdigest()}
        for i in range(20):
            stream = target.add_stream(files, random_scv, tags)
            self.assertEqual(stream.download('tags/pdb.gz.b64'),
                             tags['pdb.gz.b64'].encode())

        stream = random.sample(target.streams, 1)[0]

        self.assertEqual(stream.status, 'enabled')
        self.assertEqual(stream.frames, 0)
        self.assertEqual(stream.download('files/state.xml.gz.b64'),
                         encoded_state.encode())
        self.assertEqual(stream.active, False)
        # new_binary = base64.b64encode(b'hehehe')
        stream.stop()
        correct_ids = set()
        for s in target.streams:
            correct_ids.add(s.id)
        correct_ids.remove(stream.id)
        stream.delete()
        time.sleep(3)
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

        # it may take sometime to update
        time.sleep(1)

        target.delete()
        self.assertEqual(siegetank.list_targets(), [])
