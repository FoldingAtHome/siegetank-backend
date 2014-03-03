import requests
import json
from siegetank.util import is_domain, encode_files


class Stream:
    def __init__(self, id, ws):
        self._id = id
        self._ws = ws

    def reload(self):
        pass

    @property
    def frames(self):
        pass

    @property
    def status(self):
        return self._status

    @property
    def ws(self):
        return self._ws


class Target:
    def __init__(self, cc_uri, target_id):
        self.uri = cc_uri
        self._id = target_id
        self._description = None
        self._steps_per_frame = None
        self._creation_date = None
        self._allowed_ws = None
        self._engine = None
        self._engine_versions = None
        self._files = None
        self._streams = None

    def add_stream(self, files):
        assert isinstance(files, dict)
        body = {
            "target_id": self.id,
            "files": encode_files(files),
        }
        url = 'https://'+self.uri+'/streams'
        reply = requests.post(url, data=json.dumps(body),
                              verify=is_domain(self.uri))
        if(reply.status_code != 200):
            print(reply.text)
            raise Exception('Bad status code')
        else:
            return json.loads(reply.text)['stream_id']
        self.reload_streams()

    def delete_stream(self, stream_id):
        url = 'https://'+self.uri+'/streams/delete/'+stream_id
        reply = requests.put(url, verify=is_domain(self.uri))
        if(reply.status_code != 200):
            print(reply.text, reply.status_code)
            raise Exception('Failed to delete stream')
        self.reload_streams()

    def reload_streams(self):
        url = 'https://'+self.uri+'/targets/streams/'+self.id
        reply = requests.get(url, verify=is_domain(self.uri))
        if(reply.status_code != 200):
            raise Exception('Failed to load target streams')
        stream_info = json.loads(reply.text)
        self._streams = stream_info

    def reload_info(self):
        url = 'https://'+self.uri+'/targets/info/'+self.id
        reply = requests.get(url, verify=is_domain(self.uri))
        if(reply.status_code != 200):
            raise Exception('Failed to load target info')
        info = json.loads(reply.text)
        self._description = info['description']
        self._steps_per_frame = info['steps_per_frame']
        self._creation_date = info['creation_date']
        self._allowed_ws = info['allowed_ws']
        self._engine = info['engine']
        self._engine_versions = info['engine_versions']
        self._files = info['files']

    @property
    def id(self):
        return self._id

    @property
    def files(self):
        if not self._files:
            self.reload_info()
        return self._files

    @property
    def streams(self):
        if not self._streams:
            self.reload_streams()
        return self._streams

    @property
    def description(self):
        if not self._description:
            self.reload_info()
        return self._description

    @property
    def steps_per_frame(self):
        if not self._steps_per_frame:
            self.reload_info()
        return self._steps_per_frame

    @property
    def creation_date(self):
        if not self._creation_date:
            self.reload_info()
        return self._creation_date

    @property
    def allowed_ws(self):
        if not self._allowed_ws:
            self.reload_info()
        return self._allowed_ws

    @property
    def engine(self):
        if not self._engine:
            self.reload_info()
        return self._engine

    @property
    def engine_versions(self):
        if not self._engine_versions:
            self.reload_info()
        return self._engine_versions


def workserver_status(cc_uri):
    url = 'https://'+cc_uri+'/ws/status'
    reply = requests.get(url, verify=is_domain(cc_uri))
    if reply.status_code == 200:
        return reply.json()


#TODO: DESCRIPTION MUST BE BASE64'D
def add_target(cc_uri, steps_per_frame, engine, engine_versions,
               description='', stage='private', files=None, allowed_ws=None):
    """
    """

    body = {}

    if files:
        body['files'] = encode_files(files)
    body['steps_per_frame'] = steps_per_frame
    body['engine'] = engine
    assert type(engine_versions) == list
    body['engine_versions'] = engine_versions
    body['description'] = description
    body['stage'] = stage
    if allowed_ws:
        body['allowed_ws'] = allowed_ws
    url = 'https://'+cc_uri+'/targets'
    reply = requests.post(url, data=json.dumps(body), verify=is_domain(cc_uri))
    if reply.status_code != 200:
        print(reply.status_code, reply.text)
        raise Exception('Cannot add target')
    target_id = reply.json()['target_id']
    target = Target(cc_uri, target_id)
    return target

load_target = Target


def get_targets(cc_uri):
    """
    Return a list of targets on the CC. If you are logged in, then this method
    returns your list of targets.
    """
    url = 'https://'+cc_uri+'/targets'
    reply = requests.get(url, verify=is_domain(cc_uri))
    if reply.status_code != 200:
        raise Exception('Cannot list targets')
    targets = reply.json()['targets']
    return targets
