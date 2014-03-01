import siegetank.base as base
import simtk.openmm as openmm
import simtk.unit as unit


class OpenMMTarget(base.Target):
    def __init__(self, id, cc):
        """ Default constructor tries to load id from cc"""
        super(OpenMMTarget, self).__init__(id, cc)
        self._system = None
        self._integrator = None

    def deserialize_target_files(self):
        if not self._target_files 
        pass

    @property
    def system(self):
        if not self._system:
            self.download_files()
        return self.system

    @property
    def integrator(self):
        if not self._integrator:
            self.download_files()
        return self._integrator

    def shoot(self, state, system=None, integrator=None, validate=True):
        """ Add a stream to the target.

            If the system and/or the integrator already exists on the target,
            then an exception is thrown.

        """

        if not self.system:
            assert system is None
            # assert system is of type openmm system here
        else:
            system = openmm.xmlserializer.deserialize(
                self.target_files['system.xml'])

        if not self.target_files['integrator.xml']:
            assert integrator is None
            # assert system is of type openmm integrator here
        else:
            integrator = openmm.xmlserializer.deserialize(
                self.target_files['integrator.xml'])

        if validate:
            # load state(s)
            # load system
            # load integrator
            # do 1 step.
            pass


def add_target():
    siegetank.add.base