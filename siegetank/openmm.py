import siegetank.base as base
import simtk.openmm as openmm
import simtk.unit as unit
import base64
import requests


class OpenMMTarget(base.Target):
    def __init__(self, id, cc):
        """ Default constructor tries to load id from cc"""
        super(OpenMMTarget, self).__init__(id, cc)
        # these are loaded lazily (namely when we need to shoot new states)
        # also cached when downloaded.
        self._system = None
        self._integrator = None

    @property
    def system(self):
        if not self._system:
            filename = 'system.xml.gz.b64'
            b64f = requests.fetch(self.uri+'/target/'+self.id+'/'+filename)
            self._system = openmm.deserialize(base64.b64decode(b64f))
        return self._system

    @property
    def integrator(self):
        if not self._integrator:
            filename = 'integrator.xml.gz.b64'
            b64f = requests.fetch(self.uri+'/target/'+self.id+'/'+filename)
            self._integrator = openmm.deserialize(base64.b64decode(b64f))
        return self._integrator

    def shoot(self, state, system=None, integrator=None, validate=True):
        """ Add a stream to the target.

            If the system and/or the integrator already exists on the target,
            then an exception is thrown.

        """

        if not system:
            system = self.system
        if not integrator:
            integrator = self.integrator

        # do type checks here

        # check states

        if validate:
            # load state(s)
            # load system
            # load integrator
            # do 1 step.
            pass


def add_target(cc_uri,
               steps_per_frame,
               description='',
               stage='private',
               system=None,
               integrator=None,
               allowed_ws=None,
               engine_versions=None):

    if isinstance(steps_per_frame, unit.quantity.Quantity):
        steps = int(steps_per_frame/integrator.getStepSize())
    elif isinstance(steps_per_frame, int):
        steps = steps_per_frame

    engine = 'openmm'
    if not engine_versions:
        engine_versions = openmm.__version__

    target_files = {}

    if integrator:
        intg_string = openmm.serialize(integrator)
        target_files['integrator.xml'] = intg_string
    if system:
        sys_string = openmm.serialize(system)
        target_files['system.xml'] = sys_string

    return add_target(steps, engine, engine_versions, descriptions=description,
                      stage=stage, files=target_files, allowed_ws=allowed_ws)
