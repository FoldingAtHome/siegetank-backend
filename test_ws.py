import ws
import cc
import hashlib
import redis
import tornado.ioloop
from tornado.testing import AsyncHTTPTestCase
import unittest
import subprocess
import json
import time
import uuid
import base64
import os
import random
import struct
import requests
import shutil
import cStringIO
import tarfile


def _tar_strings(strings, names):
    ''' Returns a cStringIO'd tar file of strings with names in names '''
    assert len(strings) == len(names)

    tar_outfile = cStringIO.StringIO()
    with tarfile.open(mode='w', fileobj=tar_outfile) as tarball:
        for string, name in zip(strings,names):
            frame_binary = string
            # binary string
            frame_string = cStringIO.StringIO()
            frame_string.write(frame_binary)
            frame_string.seek(0)
            info = tarfile.TarInfo(name=name)
            info.size=len(frame_binary)
            # tarfile as a string
            tarball.addfile(tarinfo=info, fileobj=frame_string)
    return tar_outfile

class WSHandlerTestCase(AsyncHTTPTestCase):

    @classmethod
    def setUpClass(self):
        redis_port = str(6827)
        self.redis_client = ws.init_redis(redis_port)
        # Use a single DB session
        self.redis_client.flushdb()
        self.increment = 3
        self._folders = ['files','streams']
        for folder in self._folders:
            if not os.path.exists(folder):
                os.makedirs(folder)
        super(AsyncHTTPTestCase, self).setUpClass()

    @classmethod
    def tearDownClass(self):
        ''' Destroy the server '''
        self.redis_client.flushdb()
        self.redis_client.shutdown()
        tornado.ioloop.IOLoop.instance().stop()
        for folder in self._folders:
            if os.path.exists(folder):
                shutil.rmtree(folder)
        super(AsyncHTTPTestCase, self).tearDownClass()

    def get_app(self):
        return tornado.web.Application([
                        (r'/frame', ws.FrameHandler),
                        (r'/stream', ws.StreamHandler),
                        (r'/heartbeat', ws.HeartbeatHandler, 
                                        dict(increment=self.increment))
                        ])

    def test_add_stream(self):
        # Add a stream
        system_bin      = str(uuid.uuid4())
        state_bin       = str(uuid.uuid4())
        integrator_bin  = str(uuid.uuid4())
        files = {
            'state_bin' : state_bin,
            'system_bin' : system_bin,
            'integrator_bin' : integrator_bin
        }
        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)
        self.assertEqual(resp.code, 200)
        stream_id = resp.body
        return stream_id, system_bin, state_bin, integrator_bin

    def test_assign_stream(self):
        stream_id, system_bin, state_bin, integrator_bin = \
            self.test_add_stream()
        token_id = str(uuid.uuid4())
        self.redis_client.sadd('active_streams',stream_id)
        self.redis_client.hset('active_stream:'+stream_id, 
                               'shared_token', token_id)
        self.redis_client.hset('active_stream:'+stream_id, 
                               'donor', 'proteneer')
        self.redis_client.hset('active_stream:'+stream_id, 
                               'start_time', self.redis_client.time()[0])
        self.redis_client.hset('active_stream:'+stream_id, 
                               'steps', 0)
        self.redis_client.set('shared_token:'+token_id+':stream', stream_id)
        # set a really long timer to make sure this doesn't die half way
        ws_time = cc.sum_time(self.redis_client.time())
        self.redis_client.zadd('heartbeats',stream_id,ws_time+600)
        return stream_id, token_id, system_bin, state_bin, integrator_bin

    def test_get_frame(self):
        res = self.test_assign_stream()
        stream_id      = res[0]
        token_id       = res[1]
        system_bin     = res[2]
        state_bin      = res[3]
        integrator_bin = res[4]

        headers  = {'shared_token' : token_id}
        # Test GET a job
        response = self.fetch('/frame', headers=headers, method='GET')  
        with tarfile.open(mode='r', fileobj=
                          cStringIO.StringIO(response.body)) as tarball:
            for member in tarball.getmembers():
                if member.name == 'system.xml.gz':
                    self.assertEqual(tarball.extractfile(member).read(),
                                     system_bin)
                if member.name == 'state.xml.gz':
                    self.assertEqual(tarball.extractfile(member).read(),
                                     state_bin)
                if member.name == 'integrator.xml.gz':
                    self.assertEqual(tarball.extractfile(member).read(),
                                     integrator_bin)

    def test_post_frame(self):
        res            = self.test_assign_stream()
        stream_id      = res[0]
        token_id       = res[1]
        headers        = {'shared_token' : token_id}

        # Test POST a single frame
        frame_binary1 = os.urandom(1024)
        tar_out = _tar_strings([frame_binary1],['frame.xtc'])
        resp = self.fetch('/frame', headers=headers, 
                    method='POST', body=tar_out.getvalue())
        self.assertEqual(resp.code, 200)
        self.assertEqual(self.redis_client.hget('stream:'+stream_id,
                                                'frames'),str(0))
        self.assertEqual(self.redis_client.hget('active_stream:'+stream_id,
                                                'buffer_frames'),str(1))
        with open(os.path.join('streams',stream_id,'buffer.xtc'), 'rb') as f:
            self.assertEqual(f.read(),frame_binary1)
        # Test sending a single frame with a state file
        frame_binary2 = os.urandom(1024)
        state_binary = os.urandom(2048)
        tar_out = _tar_strings([frame_binary2, state_binary],
                               ['frame.xtc','state.xml.gz'])
        resp = self.fetch('/frame', headers=headers, 
                    method='POST', body=tar_out.getvalue())
        self.assertEqual(resp.code, 200)
        self.assertEqual(self.redis_client.hget('stream:'+stream_id,
                                                'frames'),str(2))
        self.assertEqual(self.redis_client.hget('active_stream:'+stream_id,
                                                'buffer_frames'),str(0))
        stream_dir = os.path.join('streams',stream_id)
        if os.path.exists(os.path.join(stream_dir,'buffer.xtc')):
            with open(os.path.join('streams',stream_id,'buffer.xtc'), 
                                   'rb') as f:
                if len(f.read()):
                    raise Exception('Bad buffer, not empty')
        with open(os.path.join(stream_dir,'frames.xtc'), 'rb') as f:
            self.assertEqual(f.read(),frame_binary1+frame_binary2)
        if not os.path.exists(os.path.join(stream_dir,'state.xml.gz')):
            raise Exception('Checkpoint state file missing!')
        
    def test_post_bad_frame(self):
        # Test POSTing an error 
        res = self.test_assign_stream()
        stream_id = res[0]
        token_id  = res[1]
        headers   = {'shared_token' : token_id,
                     'error_code'   : 'BadState'}
        resp = self.fetch('/frame', headers=headers, method='POST', body='')
        self.assertEqual(resp.code, 400)
        # Make sure we can no longer POST to this stream
        headers = {'shared_token' : token_id}
        frame_binary1 = os.urandom(1024)
        tar_out = _tar_strings([frame_binary1],['frame.xtc'])
        resp = self.fetch('/frame', headers=headers, 
                          method='POST', body=tar_out.getvalue())
        rc = self.redis_client
        self.assertFalse(rc.exists('shared_token:'+token_id+':stream'))
        self.assertFalse(rc.sismember('active_streams',stream_id))
        self.assertFalse(rc.exists('active_stream:'+stream_id))
        self.assertEqual(rc.hget('stream:'+stream_id,'error_count'),str(1))

    def test_disable_stream(self):
        res = self.test_assign_stream()
        stream_id = res[0]
        token_id  = res[1]
        headers   = {'shared_token' : token_id}

        # Test we can't POST if stream is disabled
        self.redis_client.hset('stream:'+stream_id,'status','DISABLED')
        frame_binary1 = os.urandom(1024)
        tar_out = _tar_strings([frame_binary1],['frame.xtc'])
        resp = self.fetch('/frame', headers=headers, 
                    method='POST', body=tar_out.getvalue())
        self.assertEqual(resp.code, 400)
        self.redis_client.hset('stream:'+stream_id,'status','OK')
        frame_binary1 = os.urandom(1024)
        tar_out = _tar_strings([frame_binary1],['frame.xtc'])
        resp = self.fetch('/frame', headers=headers, 
                    method='POST', body=tar_out.getvalue())
        self.assertEqual(resp.code, 200)

    def test_heartbeat(self):
        token_id = str(uuid.uuid4())
        stream_id = str(uuid.uuid4())
        self.redis_client.set('shared_token:'+token_id+':stream', stream_id)

        # test sending request to uri: /heartbeat extends the expiration time
        for iteration in range(10):
            response = self.fetch('/heartbeat', method='POST',
                                body=json.dumps({'shared_token' : token_id}))
            hb = self.redis_client.zscore('heartbeats',stream_id)
            ws_start_time = cc.sum_time(self.redis_client.time())
            self.assertAlmostEqual(ws_start_time+self.increment, hb, places=2)
        # test expirations
        response = self.fetch('/heartbeat', method='POST',
                    body=json.dumps({'shared_token' : token_id}))
        self.redis_client.sadd('active_streams',stream_id)
        self.redis_client.hset('active_stream:'+stream_id, 
                               'shared_token', token_id)
        self.assertTrue(
            self.redis_client.sismember('active_streams',stream_id) and
            self.redis_client.exists('active_stream:'+stream_id) and 
            self.redis_client.exists('shared_token:'+token_id+':stream'))
        time.sleep(self.increment+1)
        ws.check_heartbeats() 
        self.assertFalse(
            self.redis_client.sismember('active_streams',stream_id) and
            self.redis_client.exists('active_stream:'+stream_id) and 
            self.redis_client.exists('shared_token:'+token_id+':stream'))

    def test_post_stream(self):
        system_bin     = 'system.xml.gz'
        state_bin      = 'state.xml.gz'
        integrator_bin = 'integrator.xml.gz'
        system_hash = hashlib.md5(system_bin).hexdigest()
        integrator_hash = hashlib.md5(integrator_bin).hexdigest()

        # Test send binaries of system.xml and integrator
        files = {
            'state_bin' : state_bin,
            'system_bin' : system_bin,
            'integrator_bin' : integrator_bin
        }
        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)
        self.assertEqual(resp.code, 200)
        stream_id1 = resp.body
        self.assertTrue(
            self.redis_client.sismember('file_hashes',system_hash) and 
            self.redis_client.sismember('file_hashes',integrator_hash) and
            os.path.exists(os.path.join('files',system_hash)) and
            os.path.exists(os.path.join('files',integrator_hash)) and 
            os.path.exists(os.path.join('streams',
                                         stream_id1,'state.xml.gz')))

        # Test send hashes of existing files
        files = { 
            'state_bin' : state_bin,
            'system_hash' : system_hash,
            'integrator_hash' : integrator_hash
        }
        prep = requests.Request('POST','http://url',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)
        stream_id2 = resp.body
        self.assertEqual(resp.code, 200)
        server_streams = self.redis_client.smembers('streams')
        self.assertTrue(stream_id1 in server_streams)
        self.assertTrue(stream_id2 in server_streams)

        # Test send one hash one bin
        files = { 
            'state_bin' : state_bin,
            'system_hash' : system_hash,
            'integrator_bin' : integrator_bin
        }
        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)
        stream_id3 = resp.body
        self.assertEqual(resp.code, 200)
        server_streams = self.redis_client.smembers('streams')
        self.assertTrue(stream_id1 in server_streams)
        self.assertTrue(stream_id2 in server_streams)
        self.assertTrue(stream_id3 in server_streams)

        # Verify integrity of file
        self.assertTrue(
            self.redis_client.sismember('file_hashes',system_hash) and 
            self.redis_client.sismember('file_hashes',integrator_hash))

        system_bin_read = open(os.path.join('files',system_hash)).read()
        integ_bin_read = open(os.path.join('files',integrator_hash)).read()
        self.assertEqual(system_bin_read, system_bin)
        self.assertEqual(integ_bin_read, integrator_bin)

        stream_ids = [stream_id1, stream_id2, stream_id3]
        # verify redis entries
        for stream in stream_ids:
            frame_count = self.redis_client.hget('stream:'+stream, 'frames')
            status = self.redis_client.hget('stream:'+stream, 'status')
            test_system_hash = self.redis_client.hget('stream:'+stream,
                                                 'system_hash')
            test_integrator_hash = self.redis_client.hget('stream:'+stream,
                                                 'integrator_hash')
            self.assertEqual(status,'OK')
            self.assertEqual(frame_count,'0')
            self.assertEqual(system_hash, test_system_hash)
            self.assertEqual(integrator_hash, test_integrator_hash)

        for stream in stream_ids:
            state_bin_read = open(os.path.join('streams',stream,
                                  'state.xml.gz')).read()
            self.assertEqual(state_bin_read, state_bin)
        
        for stream in stream_ids:
            shutil.rmtree(os.path.join('streams',stream))
        os.remove(os.path.join('files',system_hash))
        os.remove(os.path.join('files',integrator_hash))

    def test_post_bad_stream(self):
        system_bin     = 'system.xml.gz'
        state_bin      = 'state.xml.gz'
        integrator_bin = 'integrator.xml.gz'
        system_hash = hashlib.md5(system_bin).hexdigest()
        integrator_hash = hashlib.md5(integrator_bin).hexdigest()
        # Missing integrator
        files = { 
            'state_bin' : state_bin,
            'system_hash' : system_hash,
        }
        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)

        self.assertEqual(resp.code, 400)

        # Missing state
        files = { 
            'system_hash' : system_hash,
            'integrator_bin' : integrator_bin
        }
        prep = requests.Request('POST','http://myurl',files=files).prepare()
        resp = self.fetch('/stream', method='POST', headers=prep.headers,
                          body=prep.body)
        self.assertEqual(resp.code, 400)

    def assertEqualHash(self, string1, string2):
        self.assertEqual(hashlib.md5(string1).hexdigest(),
                         hashlib.md5(string2).hexdigest())

    def test_post_frames_get_stream(self):
        res            = self.test_assign_stream()
        stream_id      = res[0]
        token_id       = res[1]
        headers        = {'shared_token' : token_id}
        # GET a checkpoint.xml
        resp = self.fetch('/frame', headers=headers, method='GET')
        self.assertEqual(resp.code, 200)
        # POST 124 frames and 2 checkpoints, 
        # NOTE: ONE-INDEXED, frame[0] not used for anything
        frame_binaries = [os.urandom(1024) for i in range(125)]
        chkpt_binary   = [os.urandom(2048) for i in range(2)]
        # send frames 1-49, note that 0th frame is not used
        for frame_binary in frame_binaries[1:50]:
            tar_out = _tar_strings([frame_binary], ['frame.xtc'])
            resp = self.fetch('/frame', headers=headers, 
                    method='POST', body=tar_out.getvalue())
            self.assertEqual(resp.code, 200)
        # send a frame with a checkpoint
        tar_out = _tar_strings([frame_binaries[50],chkpt_binary[0]], 
                               ['frame.xtc', 'state.xml.gz'])
        resp = self.fetch('/frame', headers=headers, 
                    method='POST', body=tar_out.getvalue())
        self.assertEqual(resp.code, 200)
        # send frames 51-99
        for frame_binary in frame_binaries[51:100]:
            tar_out = _tar_strings([frame_binary], ['frame.xtc'])
            resp = self.fetch('/frame', headers=headers, 
                    method='POST', body=tar_out.getvalue())
            self.assertEqual(resp.code, 200)
        # send a frame with a checkpoint
        tar_out = _tar_strings([frame_binaries[100],chkpt_binary[1]], 
                               ['frame.xtc', 'state.xml.gz'])
        resp = self.fetch('/frame', headers=headers, 
                    method='POST', body=tar_out.getvalue())
        self.assertEqual(resp.code, 200)
        # send the remaining frames
        for frame_binary in frame_binaries[101:]:
            tar_out = _tar_strings([frame_binary], ['frame.xtc'])
            resp = self.fetch('/frame', headers=headers, 
                    method='POST', body=tar_out.getvalue())
            self.assertEqual(resp.code, 200)
        true_frames = ''.join(frame_binaries[1:101])
        buffer_frames = ''.join(frame_binaries[101:])
        self.assertEqual(self.redis_client.hget('stream:'+stream_id, 
                        'frames'), str(100))
        self.assertEqual(self.redis_client.hget('active_stream:'+stream_id,
                        'buffer_frames'), str(24))
        # make sure the frames.xtc and buffer.xtc
        with open(os.path.join('streams',stream_id,'frames.xtc')) as frames:
            self.assertEqualHash(true_frames, frames.read())
        with open(os.path.join('streams',stream_id,'buffer.xtc')) as buffers:
            self.assertEqualHash(buffer_frames, buffers.read())

        # finally we download the stream
        dtoken = str(uuid.uuid4())
        self.redis_client.set('download_token:'+dtoken+':stream', stream_id)
        headers = {
            'download_token' : dtoken
        }
        resp = self.fetch('/stream', headers=headers, method='GET')
        self.assertEqualHash(true_frames, resp.body)

    def test_delete_stream(self):
        # create and assign a stream
        res = self.test_assign_stream()
        stream_id = res[0]
        token_id  = res[1]
        headers = { 'stream_id' : stream_id }
        resp = self.fetch('/stream', headers=headers, method='DELETE')
        self.assertEqual(resp.code, 200)
        rc = self.redis_client
        # check memory
        self.assertFalse(rc.sismember('active_streams',stream_id))
        self.assertFalse(rc.exists('active_stream:'+stream_id))
        self.assertFalse(rc.sismember('streams',stream_id))
        self.assertFalse(rc.exists('stream:'+stream_id))
        self.assertFalse(rc.exists('download_token:'+stream_id+':stream'))
        self.assertFalse(rc.exists('shared_token:'+stream_id+':stream'))
        # check disk
        stream_path = os.path.join('streams',stream_id)

        self.assertFalse(os.path.exists(stream_path))

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(WSHandlerTestCase)
    unittest.TextTestRunner(verbosity=3).run(suite)