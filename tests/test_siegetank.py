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
import shutil
import glob
import random
import filecmp
import pymongo

import siegetank.base
import tests.utils

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

        result = tests.utils.add_user(manager=True)
        siegetank.login(cc_uri, result['token'])

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
        mdb = pymongo.MongoClient()
        for data_folder in glob.glob('*_data'):
            shutil.rmtree(data_folder)
        for db_name in mdb.database_names():
            mdb.drop_database(db_name)

    def test_sync_stream(self):

        def add_partition(stream_id, partitions):
            stream_dir = os.path.join('firebat_data', 'streams', stream_id)
            for i in partitions:
                p_dir = os.path.join(stream_dir, str(i))
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


        def are_dir_trees_equal(dir1, dir2):
            """
            Compare two directories recursively. Files in each directory are
            assumed to be equal if their names and contents are equal.

            @param dir1: First directory path
            @param dir2: Second directory path

            @return: True if the directory trees are the same and
                there were no errors while accessing the directories or files, 
                False otherwise.
           """

            dirs_cmp = filecmp.dircmp(dir1, dir2)
            if len(dirs_cmp.left_only) > 0 or len(dirs_cmp.right_only) > 0 or \
                len(dirs_cmp.funny_files) > 0:
                return False
            (_, mismatch, errors) = filecmp.cmpfiles(
                dir1, dir2, dirs_cmp.common_files, shallow=False)
            if len(mismatch) > 0 or len(errors) > 0:
                return False
            for common_dir in dirs_cmp.common_dirs:
                new_dir1 = os.path.join(dir1, common_dir)
                new_dir2 = os.path.join(dir2, common_dir)
                if not are_dir_trees_equal(new_dir1, new_dir2):
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

        print('DEBUG:', siegetank.base.scvs)

        random_scv = random.choice(list(siegetank.base.scvs.keys()))
        for i in range(3):
            target.add_stream(files, random_scv)
        target.add_stream(files, random_scv)
        stream = random.sample(target.streams, 1)[0]
        sync_dir = os.path.join('sync_data',stream.id)
        stream_dir = os.path.join('firebat_data', 'streams', stream.id)
        stream.sync(sync_dir, sync_seeds=True)
        self.assertTrue(are_dir_trees_equal(sync_dir, stream_dir))
        add_partition(stream.id, [4, 9, 34, 493])
        # test stream sync
        stream.sync(sync_dir, sync_seeds=True)
        self.assertTrue(are_dir_trees_equal(sync_dir, stream_dir))
        add_partition(stream.id, [589, 2098, 29038])
        stream.sync(sync_dir, sync_seeds=True)
        self.assertTrue(are_dir_trees_equal(sync_dir, stream_dir))
        remove_local_partition(stream.id, 589)
        self.assertFalse(are_dir_trees_equal(sync_dir, stream_dir))
        stream.sync(sync_dir, sync_seeds=True)
        self.assertTrue(are_dir_trees_equal(sync_dir, stream_dir))
        remove_local_partition(stream.id, 34, 'bin1')
        self.assertFalse(are_dir_trees_equal(sync_dir, stream_dir))
        stream.sync(sync_dir, sync_seeds=True)
        self.assertTrue(are_dir_trees_equal(sync_dir, stream_dir))
        remove_local_partition(stream.id, 4, 'checkpoint_files/cin1')
        self.assertFalse(are_dir_trees_equal(sync_dir, stream_dir))
        stream.sync(sync_dir, sync_seeds=True)
        self.assertTrue(are_dir_trees_equal(sync_dir, stream_dir))
        remove_local_seed_file(stream.id, 'system.xml.gz.b64')
        self.assertFalse(are_dir_trees_equal(sync_dir, stream_dir))
        stream.sync(sync_dir, sync_seeds=False)
        self.assertFalse(are_dir_trees_equal(sync_dir, stream_dir))
        stream.sync(sync_dir, sync_seeds=True)
        self.assertTrue(are_dir_trees_equal(sync_dir, stream_dir))

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
        siegetank.base.refresh_scvs()

        print('DEBUG', siegetank.base.scvs)

        random_scv = random.choice(list(siegetank.base.scvs.keys()))
        for i in range(20):
            target.add_stream(files, random_scv)
        stream = random.sample(target.streams, 1)[0]

        self.assertEqual(stream.status, 'OK')
        self.assertEqual(stream.frames, 0)
        self.assertEqual(stream.download('files/state.xml.gz.b64'),
                         encoded_state.encode())
        self.assertEqual(stream.active, False)
        new_binary = base64.b64encode(b'hehehe')
        stream.stop()
        stream.upload('files/state.xml.gz.b64', new_binary)
        self.assertEqual(stream.download('files/state.xml.gz.b64'),
                         new_binary)
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
