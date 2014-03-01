import requests
import json
import siegetank.login


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
    @classmethod
    def load(cls, target_id, cc_uri):
        cls.uri = 'https://'+cc_uri+':443'
        target = cls(target_id, cc_uri)
        target._id = target_id
        target._description = None
        target._steps_per_frame = None
        target._creation_date = None
        target._allowed_ws = None
        target._engine = None
        target._engine_versions = None
        target._target_files = None

    def add_stream(files):
        body = json.loads(files)
        requests.put(files)

    def reload_files(self):
        reply = requests.get(self.uri+'hehe')

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

    @property:
    def target_files(self):
        if not self._target_files:
            requests.fetch(self.uri)

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