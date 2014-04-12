import requests
import json
import base64
from siegetank.util import is_domain, encode_files

auth_token = None
login_cc = None
scvs = dict()


def login(cc, token):
    """ Login to a particular command center using the generated token. """
    url = 'https://'+cc+'/managers/verify'
    headers = {'Authorization': token}
    reply = requests.get(url, verify=is_domain(cc), headers=headers)
    if reply.status_code != 200:
        raise ValueError("Bad token")
    global auth_token
    auth_token = token
    global login_cc
    login_cc = cc
    global scvs
    scvs = refresh_scvs(cc)


def generate_token(cc, email, password):
    """ Generate a login token and login automatically. """
    data = {
        "email": email,
        "password": password
    }
    uri = 'https://'+cc+'/managers/auth'
    reply = requests.post(uri, data=json.dumps(data), verify=is_domain(cc))
    if reply.status_code != 200:
        raise ValueError("Bad login credentials")
    token = json.loads(reply.text)['token']
    login(cc, token)
    return token


def refresh_scvs(cc):
    """ Update and return the status of the workservers owned by ``cc`` """
    global scvs
    url = 'https://'+cc+'/scvs/status'
    reply = requests.get(url, verify=is_domain(cc))
    if reply.status_code == 200:
        content = reply.json()
        for scv_name, scv_prop in content.items():
            # sets host and status fields
            scvs[scv_name] = scv_prop
    return scvs


class Base:
    def __init__(self, uri):
        self.uri = uri

    def _get(self, path, host=None):
        headers = {'Authorization': auth_token}
        if host is None:
            host = self.uri
        url = 'https://'+host+path
        return requests.get(url, headers=headers, verify=is_domain(self.uri))

    def _put(self, path, body=None):
        headers = {'Authorization': auth_token}
        url = 'https://'+self.uri+path
        if body is None:
            body = '{}'
        return requests.put(url, headers=headers, data=body,
                            verify=is_domain(self.uri))

    def _post(self, path, body=None):
        headers = {'Authorization': auth_token}
        url = 'https://'+self.uri+path
        if body is None:
            body = '{}'
        return requests.post(url, headers=headers, data=body,
                             verify=is_domain(self.uri))


class Stream(Base):
    """ A Stream is a single trajectory residing on a remote server. """
    def __init__(self, stream_id):
        """ Retrieve an existing stream object """

        self._id = stream_id
        self._frames = None
        self._status = None
        self._error_count = None
        self._active = None
        scv_name = stream_id.split(':')[1]
        global scvs
        uri = scvs[scv_name]['host']
        super(Stream, self).__init__(uri)

    def __repr__(self):
        frames = str(self.frames)
        return '<stream '+self.id+' s:'+self.status+' f:'+frames+'>'

    def start(self):
        """ Start this stream. """
        reply = self._put('/streams/start/'+self.id)
        if reply.status_code != 200:
            print(reply.text)
            raise Exception('Bad status code')
        self.reload_info()

    def stop(self):
        """ Stop this stream. """
        reply = self._put('/streams/stop/'+self.id)
        if reply.status_code != 200:
            print(reply.text)
            raise Exception('Bad status code')
        self.reload_info()

    def delete(self):
        """ Delete this stream from the SCV. You must take care to not
        use this stream object anymore afterwards.

        """
        reply = self._put('/streams/delete/'+self.id)
        if reply.status_code != 200:
            print(reply.text)
            raise Exception('Bad status code')
        self._id = None

    def download(self, filename):
        """ Download a file from the stream.

        :param filename: ``filename`` can be produced by the core,
            or it can be a file you initialized passed in.

        """
        reply = self._get('/streams/download/'+self.id+'/'+filename)
        return reply.content

    def replace(self, filename, filedata):
        """ Replace a file on the stream.

        :param filename: name of the file, eg. state.xml.gz.b64
        :param filedata: base64 encoded data

        """
        # make sure filedata is encoded in b64 format
        base64.b64decode(filedata)
        body = json.dumps({
            "files": {filename: filedata}
        })
        reply = self._put('/streams/replace/'+self.id, body=body)
        if reply.status_code != 200:
            print(reply.text)
            raise Exception('Bad status code')

    def reload_info(self):
        reply = self._get('/streams/info/'+self.id)
        content = json.loads(reply.text)
        self._frames = content['frames']
        self._status = content['status']
        self._error_count = content['error_count']
        self._active = content['active']

    @property
    def id(self):
        """ Return the stream's id """
        return self._id

    @property
    def active(self):
        """ Returns True if the stream is activated by a core """
        if not self._active:
            self.reload_info()
        return self._active

    @property
    def frames(self):
        """ Return the number of frames completed so far """
        if not self._frames:
            self.reload_info()
        return self._frames

    @property
    def status(self):
        """ Return the status of the stream """
        if not self._status:
            self.reload_info()
        return self._status

    @property
    def error_count(self):
        """ Return the number of errors this stream has encountered """
        if not self._error_count:
            self.reload_info()
        return self._error_count


class Target(Base):
    """ A Target is a collection of Streams residing on a remote server. """
    def __init__(self, target_id, cc_uri=None):
        """ Retrieve an existing target object """
        global login_cc
        if cc_uri is None:
            cc_uri = login_cc
        if cc_uri is None:
            raise Exception("You are not logged in to a cc!")
        self._id = target_id
        self._description = None
        self._options = None
        self._creation_date = None
        self._shards = None
        self._engines = None
        self._streams = None
        self._weight = None
        super(Target, self).__init__(cc_uri)

    def __repr__(self):
        return '<target '+self.id+'>'

    def attach_shard(self):
        raise Exception('Not implemented')

    def detach_shard(self):
        raise Exception('Not implemented')

    def delete(self):
        """ Delete this target from the backend """
        reply = self._put('/targets/delete/'+self.id)
        if reply.status_code != 200:
            print(reply.text)
            raise Exception('Bad status code')
        self._id = None

    def add_stream(self, files):
        """ Add a stream to the target.

        :param files: a dictionary of filenames to binaries matching the core's
            requirements

        """
        assert isinstance(files, dict)
        body = {
            "target_id": self.id,
            "files": encode_files(files),
        }
        reply = self._post('/streams', json.dumps(body))
        if reply.status_code != 200:
            print(reply.text)
            raise Exception('Bad status code')
        else:
            return json.loads(reply.text)['stream_id']

    def reload_streams(self):
        """ Reload the target's set of streams """
        self._streams = set()
        global scvs
        for scv in self.shards:
            host = scvs[scv]['host']
            reply = self._get('/targets/streams/'+self.id, host=host)
            if reply.status_code != 200:
                raise Exception('Failed to load streams from SCV: '+scvs)
            print(reply.json())
            for stream_id in reply.json()['streams']:
                self._streams.add(Stream(stream_id))

    def reload_info(self):
        """ Reload the target's information """
        reply = self._get('/targets/info/'+self.id)
        if reply.status_code != 200:
            raise Exception('Failed to load target info')
        info = json.loads(reply.text)
        self._description = info['description']
        self._options = info['options']
        self._creation_date = info['creation_date']
        self._shards = info['shards']
        self._engines = info['engines']
        self._weight = info['weight']

    @property
    def id(self):
        """ Get the target id """
        return self._id

    @property
    def streams(self):
        """ Get the set of streams in this target """
        self.reload_streams()
        return self._streams

    @property
    def description(self):
        """ Get the description of the target. """
        if not self._description:
            self.reload_info()
        return self._description

    @property
    def options(self):
        """ Get the options for this target. """
        if not self._options:
            self.reload_info()
        return self._options

    @property
    def creation_date(self):
        """ Get the date the target was created. """
        if not self._creation_date:
            self.reload_info()
        return self._creation_date

    @property
    def shards(self):
        """ Return a list of SCVs the streams are sharded across. """
        if not self._shards:
            self.reload_info()
        return self._shards

    @property
    def engines(self):
        """ Get the list of engines being used. """
        if not self._engines:
            self.reload_info()
        return self._engines

    @property
    def engine_versions(self):
        """ Get the list of engine versions allowed. """
        if not self._engine_versions:
            self.reload_info()
        return self._engine_versions

    @property
    def weight(self):
        return self._weight


def add_target(options, engines, cc_uri=None, weight=1,
               description='', stage='private', files=None):
    """ Add a target to be managed by the CC at ``cc_uri``.

    ``options`` is a dictionary of ,
    discard_water, xtc_precision, etc.

    :param options: dict, core specific options like ``steps_per_frame``.
    :param engine: list, eg. ["openmm_60_opencl", "openmm_50_cuda"]
    :param cc_uri: str, which cc to use, do not add http prefixes
    :param description: str, JSON safe plain text description
    :param stage: str, stage of the target, allowed values are 'disabled',
        'private', 'public'
    :param weight: int, the weight of the target relative to your other targets
    """
    if cc_uri is None:
        cc_uri = login_cc
    if cc_uri is None:
        raise Exception("You are not logged in to a cc!")

    body = {}
    if files:
        body['files'] = encode_files(files)
    body['options'] = options
    body['engines'] = engines
    assert type(engines) == list
    body['description'] = description
    body['stage'] = stage
    body['weight'] = weight
    url = 'https://'+cc_uri+'/targets'
    global auth_token
    headers = {'Authorization': auth_token}
    reply = requests.post(url, data=json.dumps(body), verify=is_domain(cc_uri),
                          headers=headers)
    if reply.status_code != 200:
        print(reply.status_code, reply.text)
        raise Exception('Cannot add target')
    target_id = reply.json()['target_id']
    target = Target(target_id, cc_uri)
    return target

load_target = Target
load_stream = Stream


def get_targets(cc_uri=None):
    """ Return your set of targets.

    """
    if cc_uri is None:
        cc_uri = login_cc
    if cc_uri is None:
        raise Exception("You are not logged in to a cc!")
    url = 'https://'+cc_uri+'/targets'
    reply = requests.get(url, verify=is_domain(cc_uri))
    if reply.status_code != 200:
        raise Exception('Cannot list targets')
    target_ids = reply.json()['targets']
    targets = set()
    for target_id in target_ids:
        targets.add(Target(target_id, cc_uri))
    return targets
