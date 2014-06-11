OpenMM Engines
==============

OpenMM engines are powered by `OpenMM <http://www.openmm.org>`_, and is currently the primary build for cores. Each engine name indicates the particular release of OpenMM that is being used.

Desktop
-------

**Available Engines**: [openmm_601_opencl, openmm_601_cpu]

These engines are linked against OpenMM Release 6.0.1, and are deployed on Linux (and soon Windows). To use this engine, you must specify the following options for adding new targets, and files for adding new streams:

.. sourcecode:: python

    target_options = {
        'steps_per_frame': 50000, # number of steps per frame
        'description': 'Some plaintext, JSON-serializable, description.'
    }

    my_target = siegetank.add_target(options=target_options, ...)

    stream_files = {
        'system.xml.gz.b64': 'text', # base64 encoded gzipped XML files
        'integrator.gz.b64': 'text', # base64 encoded gzipped XML files
        'state.xml.gz.b64': 'text' # base64 encoded gzipped XML files
    }

    my_target.add_stream(files=stream_files, ...)
