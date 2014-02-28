import requests
import json


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
    # create a new target on the backend
    def __init__(self, id, cc_uri):
        self.uri = 'https://'+cc_uri+':443'
        self._id = id
        self._description = None
        self._steps_per_frame = None
        self._creation_date = None
        self._allowed_ws = None
        self._engine = None
        self._engine_versions = None

    # override with your own 
    def add_stream(files):
        print('hehe')

    def reload(self):
        reply = requests.get(self.uri+'/targets/info/'+self.id)
        if(reply.status_code != 200):
            raise Exception('Failed to load data about stream')
        info = json.loads(reply.text)
        self._description = info['description']
        self._steps_per_frame = info['steps_per_frame']
        self._creation_date = info['creation_date']
        self._allowed_ws = info['allowed_ws']
        self._engine = info['engine']
        self._engine_versions = info['engine_versions']

    @property
    def id(self):
        return self._id

    @property
    def streams(self):
        if not self._streams:
            self.reload()
        return self._streams

    @property
    def description(self):
        if not self._description:
            self.reload()
        return self._description

    @property
    def steps_per_frame(self):
        if not self._steps_per_frame:
            self.reload()
        return self._steps_per_frame

    @property
    def creation_date(self):
        if not self._creation_date:
            self.reload()
        return self._creation_date

    @property
    def allowed_ws(self):
        if not self._allowed_ws:
            self.reload()
        return self._allowed_ws

    @property
    def engine(self):
        if not self._engine:
            self.reload()
        return self._engine

    @property
    def engine_versions(self):
        if not self._engine_versions:
            self.reload()
        return self._engine_versions

    @classmethod
    def load_from_id(id):
        cls
        pass