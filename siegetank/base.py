import requests
import json
from siegetank.util import is_domain, encode_files


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
        url = 'https://'+self.uri+'/streams'
        reply = requests.post(url, data=json.dumps(body),
                              verify=is_domain(self.uri))
        if(reply.status_code != 200):
            print(reply.text)
            raise Exception('Bad status code')
        else:
            return json.loads(reply.text)['stream_id']

    def delete_stream(self, stream_id):
        """ Delete ``stream_id`` from the target """
        url = 'https://'+self.uri+'/streams/delete/'+stream_id
        reply = requests.put(url, verify=is_domain(self.uri))
        if(reply.status_code != 200):
            print(reply.text, reply.status_code)
            raise Exception('Failed to delete stream')

    def reload_streams(self):
        """ Reload the list of streams available on the command center. """
        url = 'https://'+self.uri+'/targets/streams/'+self.id
        reply = requests.get(url, verify=is_domain(self.uri))
        if(reply.status_code != 200):
            raise Exception('Failed to load target streams')
        stream_info = json.loads(reply.text)
        self._streams = stream_info

    def reload_info(self):
        """ Reload the target's information from the commmand center. """
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
        """ Get the target id """
        return self._id

    @property
    def files(self):
        """ Get the list of files used by the target

        .. note:: Does not return the stream files

        """
        if not self._files:
            self.reload_info()
        return self._files

    @property
    def streams(self):
        """ Load the list of streams owned by this target """
        if not self._streams:
            self.reload_streams()
        return self._streams

    @property
    def description(self):
        """ Get the description of the target """
        if not self._description:
            self.reload_info()
        return self._description

    @property
    def steps_per_frame(self):
        """ Get the number of steps per frame """
        if not self._steps_per_frame:
            self.reload_info()
        return self._steps_per_frame

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


def workserver_status(cc_uri):
    """ Return the status of the workservers """
    url = 'https://'+cc_uri+'/ws/status'
    reply = requests.get(url, verify=is_domain(cc_uri))
    if reply.status_code == 200:
        return reply.json()


#TODO: DESCRIPTION MUST BE BASE64'D
def add_target(cc_uri, steps_per_frame, engine, engine_versions,
               description='', stage='private', files=None, allowed_ws=None):
    """
    Add a target to be managed by the workserver at ``cc_uri``. Currently
    supported ``engine`` is 'openmm'.
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

    .. note:: You do not need to prefix ``cc_uri`` with http
    """
    url = 'https://'+cc_uri+'/targets'
    reply = requests.get(url, verify=is_domain(cc_uri))
    if reply.status_code != 200:
        raise Exception('Cannot list targets')
    targets = reply.json()['targets']
    return targets
