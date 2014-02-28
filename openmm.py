import requests
import simtk.openmm


class Stream():
    @property
    def frames(self):
        pass


class Target():
    # create a new target on the backend
    def __init__(self, ):
        self.id = id

    def add_stream(files):
        print('hehe')

    @property
    def streams(self):
        return self.streams

    @property
    def description(self):
        pass

    @property
    def steps_per_frame(self):
        pass

    @property
    def creation_date(self):
        pass

    @property
    def allowed_ws(self):
        pass

    @property
    def engine(self):
        pass

    @property
    def engine_versions(self):
        pass

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
