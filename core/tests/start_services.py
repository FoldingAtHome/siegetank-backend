# Start services necessary to test the core.
import os
import subprocess
import time
import requests
import json
import base64
import gzip


def add_test_user():
    reply = requests.post('https://127.0.0.1:8980/managers',
                          data=json.dumps({'email': 'test_user@gmail.com',
                                           'password': 'test_pass'}),
                          verify=False)
    assert reply.status_code == 200


def auth():
    reply = requests.post('https://127.0.0.1:8980/auth',
                          data=json.dumps({'email': 'test_user@gmail.com',
                                           'password': 'test_pass'}),
                          verify=False)
    return json.loads(reply.content.decode())['token']


def post_target(auth_token):
    system_xml = gzip.decompress(requests.get('http://www.proteneer.com/xmls/system.xml.gz').content)
    integrator_xml = gzip.decompress(requests.get('http://www.proteneer.com/xmls/integrator.xml.gz').content)

    encoded_system = base64.b64encode(gzip.compress(system_xml)).decode()
    encoded_intg = base64.b64encode(gzip.compress(integrator_xml)).decode()

    reply = requests.post('https://127.0.0.1:8980/targets',
                          headers={'Authorization': auth_token},
                          verify=False,
                          data=json.dumps({'description': 'foo',
                                           'engine': 'openmm',
                                           'engine_versions': ['6.0'],
                                           'steps_per_frame': 10000,
                                           'files': {'system.xml.gz.b64': encoded_system,
                                                     'integrator.xml.gz.b64': encoded_intg
                                                     } 
                                           })
                          )
    return json.loads(reply.content.decode())['target_id']


def post_streams(target_id, state_xml, auth_token):
    encoded_state = base64.b64encode(gzip.compress(state_xml)).decode()
    reply = requests.post('https://127.0.0.1:8980/streams',
                          headers={'Authorization': auth_token},
                          verify=False,
                          data=json.dumps({'target_id': target_id,
                                           'files': {'state.xml.gz.b64': encoded_state}
                                           })
                          )
    assert reply.status_code == 200
    return json.loads(reply.content.decode())['stream_id']

if __name__ == '__main__':
    cc_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           '..', '..', 'cc')
    ws_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           '..', '..', 'ws')

    pid1 = subprocess.Popen(cc_path, stdout=subprocess.PIPE, shell=True)
    time.sleep(1)
    pid2 = subprocess.Popen(ws_path, stdout=subprocess.PIPE, shell=True)

    with open('pids.log', 'w') as log:
        log.write(str(pid1.pid))
        log.write(' ')
        log.write(str(pid2.pid))

    try:
        add_test_user()
    except Exception:
        pass
    token = auth()
    target_id = post_target(token)
    state_xml = gzip.decompress(requests.get('http://www.proteneer.com/xmls/state.xml.gz').content)
    for i in range(10):
        post_streams(target_id, state_xml, token)
