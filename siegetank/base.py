import requests
import json
import base64
from siegetank.util import is_domain, encode_files

auth_token = None
login_cc = None
workservers = dict()


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
    global workservers
    workservers = refresh_workservers(cc)


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


def refresh_workservers(cc):
    """ Update and return the status of the workservers owned by `cc` """
    global workservers
    url = 'https://'+cc+'/ws/status'
    reply = requests.get(url, verify=is_domain(cc))
    if reply.status_code == 200:
        content = reply.json()
        for ws_name, ws_properties in content.items():
            # sets url and status fields
            workservers[ws_name] = ws_properties
    return workservers


class Base:
    def __init__(self, uri):
        self.uri = uri

    def _get(self, path):
        headers = {'Authorization': auth_token}
        url = 'https://'+self.uri+path
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
    def __init__(self, stream_id):
        """ Retrieve an existing stream object """

        self._id = stream_id
        self._frames = None
        self._status = None
        self._error_count = None
        self._active = None
        ws_name = stream_id.split(':')[1]
        global workservers
        ws_uri = workservers[ws_name]['url']
        super(Stream, self).__init__(ws_uri)

    def __repr__(self):
        frames = str(self._frames)
        return '<stream '+self.id+' s:'+self._status+' f:'+frames+'>'

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
        """ Delete this stream from the workserver. You must take care to not
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
            or it can be a stream_file

        """
        reply = self._get('/streams/download/'+self.id+'/'+filename)
        return reply.content

    def replace(self, filename, filedata):
        """ Replace a file on the stream.

        :param filename: name of the file, eg. state.xml.gz.b64
        :param filedata: base64 encoded data

        """

        base64.b64decode(filedata)

        body = json.dumps({
            "stream_files": {filename: filedata}
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
        self._allowed_ws = None
        self._engine = None
        self._engine_versions = None
        self._files = None
        self._streams = None
        super(Target, self).__init__(cc_uri)

    def __repr__(self):
        return '<target '+self.id+'>'

    def delete(self):
        """ Delete this target from the backend """
        reply = self._put('/targets/delete/'+self.id)
        if reply.status_code != 200:
            print(reply.text)
            raise Exception('Bad status code')
        self._id = None

    def download(self, filename):
        """ Download a target file from the command center.

        :param filename: a particular target_file to retrieve

        """
        reply = self._get('/targets/download/'+self._id+'/'+filename)
        if reply.status_code != 200:
            raise Exception('Bad status_code')
        return reply.content

    def add_stream(self, files):
        """ Add a stream to the target. The filenames passed in here must be
        consistent with the filenames used by other streams. Else the behavior
        is undefined.

        :param files: a dictionary of filenames to binaries

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
            #TODO: return Stream object? or return at all?
            return json.loads(reply.text)['stream_id']

    def reload_streams(self):
        """ Reload the target's set of streams """
        reply = self._get('/targets/streams/'+self.id)
        if reply.status_code != 200:
            raise Exception('Failed to load target streams')
        stream_info = json.loads(reply.text)
        self._streams = set()
        for stream_name, prop in stream_info.items():
            stream_object = Stream(stream_name)
            stream_object._frames = prop['frames']
            stream_object._status = prop['status']
            self._streams.add(stream_object)

    def reload_info(self):
        """ Reload the target's information """
        reply = self._get('/targets/info/'+self.id)
        if reply.status_code != 200:
            raise Exception('Failed to load target info')
        info = json.loads(reply.text)
        self._description = info['description']
        self._options = info['options']
        self._creation_date = info['creation_date']
        self._allowed_ws = info['allowed_ws']
        self._engine = info['engine']
        self._engine_versions = info['engine_versions']
        self._files = info['files']

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
        """ Get the description of the target """
        if not self._description:
            self.reload_info()
        return self._description

    @property
    def options(self):
        """ Get the options for this target """
        if not self._options:
            self.reload_info()
        return self._options

    @property
    def creation_date(self):
        """ Get the date the target was created """
        if not self._creation_date:
            self.reload_info()
        return self._creation_date

    @property
    def allowed_ws(self):
        """ Get the list of workservers the target is allowed to striate over
        """
        if not self._allowed_ws:
            self.reload_info()
        return self._allowed_ws

    @property
    def engine(self):
        """ Get the engine type """
        if not self._engine:
            self.reload_info()
        return self._engine

    @property
    def engine_versions(self):
        """ Get the list of engine versions allowed. """
        if not self._engine_versions:
            self.reload_info()
        return self._engine_versions


def add_target(options, engine, engine_versions, cc_uri=None,
               description='', stage='private', files=None, allowed_ws=None):
    """ Add a target to be managed by the workserver at ``cc_uri``. Currently
    supported ``engine`` is 'openmm'.

    ``options`` is a dictionary of ,
    discard_water, xtc_precision, etc.

    :param options: dict, core specific options like ``steps_per_frame``.
    :param engine: str, eg. ``openmm``
    :param engine_versions: str, eg ``6.0``
    :param cc_uri: str, which cc to add the target to, eg. flash.stanford.edu
    :param description: str, JSON safe plain text description
    :param stage: str, stage of the target, allowed values are 'disabled',
        'private', 'beta', 'public'
    :param allowed_ws: str, which workserver we striated on. If None, then the
        target will be striated over across all available workservers.

    """
    if cc_uri is None:
        cc_uri = login_cc
    if cc_uri is None:
        raise Exception("You are not logged in to a cc!")

    body = {}
    if files:
        body['files'] = encode_files(files)
    body['options'] = options
    body['engine'] = engine
    assert type(engine_versions) == list
    body['engine_versions'] = engine_versions
    body['description'] = description
    body['stage'] = stage
    if allowed_ws:
        body['allowed_ws'] = allowed_ws
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
    """ Return a set of targets on the CC owned by you. If ``cc_uri`` is not
    provided, then this retrieves the targets on the cc specified during login.

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
