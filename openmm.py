import siegetank.base as base


class OpenMMTarget(base.Target):
    def add_stream(state):
        pass


def add_target(system, integrator, version=None):
    pass

# return a OpenMM Target
target = siegetank.openmm.add_target(system, integrator)
# add a stream, canaries to makes sure that the state is compatible with the
# pre-existing target files?
target.add_stream(state)
# load streams
foo = target.streams



siegetank.openmm.shoot(state)
