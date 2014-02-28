import requests
import json
import simtk.openmm


class Stream():
    @property
    def frames(self):
        pass


class Target():
    # create a new target on the backend
    def __init__(self, id, cc_uri):
        self.cc_uri = cc_uri
        self._id = id
        self._description = None
        self._steps_per_frame = None
        self._creation_date = None
        self._allowed_ws = None
        self._engine = None
        self._engine_versions = None

    def add_stream(files):
        print('hehe')

    def reload(self):
        reply = requests.get(uri+'/targets/info/'+self.id)
        if(reply.status_code != 200):
            raise Exception("Failed to fill target internals")

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
        pass

def WorkServer():


import siegetank

siegetank.login(auth_token)

# these methods return a target
target = siegetank.openmm.add_target(system, integrator)
target = siegetank.openmm.load_target(target_id)
# add a stream
target.add_stream(state)
# load streams
foo = target.streams



siegetank.openmm.shoot(state)
