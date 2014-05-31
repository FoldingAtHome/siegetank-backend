Tutorial
===============

Begin by logging in with your token.

.. sourcecode:: python

    import siegetank
    siegetank.login('your_unique_token')


Creating Targets
----------------

A target is a collection of streams with a common set of options.

To add a target, you must provide it with a dictionary of options (which differ for each type of engine), and a list of engines you'd like to use. Optionally, you can configure the weight of target, which is relative to your other targets and determines how often this target is assigned. In addition, the default stage of the target is ``private``, which restricts access until you're ready to make it ``public``.

.. sourcecode:: python
    
    opts = {'description': 'Benchmark Protein',
            'steps_per_frame': 50000}
    target = siegetank.add_target(options=opts, engines=['openmm'])
    > <target 1740a921-923c-4518-b1f5-a6e964418614>

A target object has a bunch of options you can query at any time. You will use this target later on to add streams to it.

.. sourcecode:: python

    target.options
    > {'description': 'Benchmark Protein', 'steps_per_frame': 50000}
    target.creation_date
    > 1401560581.7190273,
    target.engines
    > ['openmm']
    target.stage
    > 'private'
    target.weight
    > 1

To get a list of your targets later on:

.. sourcecode:: python

    siegetank.list_targets()
    > [<target 1740a921-923c-4518-b1f5-a6e964418614>]

To load your target later on from its id:

.. sourcecode::python

    siegetank.load_target('1740a921-923c-4518-b1f5-a6e964418614')
    > <target 1740a921-923c-4518-b1f5-a6e964418614>

Querying SCVs
-------------

The SCV is the main workhorse backend server, they are where the binary data for streams reside. SCVs are owned by different groups, so please make sure you have permission from them before adding your streams to them.

To get a dictionary of scvs and their status:

.. sourcecode:: python

    siegetank.scvs
    > {
        'proline': {
            'host': 'proline.stanford.edu:443',
            'online': True
            },
        'vspg11': {
            'host': 'vspg11.stanford.edu:443',
            'online': True
            }
       }

Adding Streams
--------------

A stream is defined by a set of files and a particular SCV it resides on. The
set of files to use depends on the particular engine of interest. The files must be encoded properly. As an example, OpenMM files must be gzipped (note: zlib is not the same thing as gzip) and base64 encoded.

.. sourcecode:: python

    import requests  # utility to fetch a set of pre-generated xml files
    import base64

    state_url = 'http://web.stanford.edu/~yutongz/state.xml.gz'
    system_url = 'http://web.stanford.edu/~yutongz/system.xml.gz'
    integrator_url = 'http://web.stanford.edu/~yutongz/integrator.xml.gz'
    encoded_system = base64.b64encode(system_gz).decode()
    encoded_intg = base64.b64encode(integrator_gz).decode()
    encoded_state = base64.b64encode(state_gz).decode()

.. note:: the slightly awkward base64.b64encode() followed by a decode() is a subtle python3 issue because b64encode() returns a ``bytes`` which must be converted to the unicode ``str``.

.. sourcecode:: python

    data = {
        'system.xml.gz.b64': encoded_system,
        'state.xml.gz.b64': encoded_state,
        'integrator.xml.gz.b64': encoded_intg
    }

    stream = target.add_stream(files=data, scv='vspg11')
    > <stream 6918e316-5c6f-425d-8c1e-902f4b0ba144:vspg11 s:OK f:0>

The s: indicates if the stream is OK or not, and f:0 indicates the number of frames. You can view a list of properties of the stream.

.. sourcecode:: python

    stream.active
    > False
    stream.frames
    > 0
    stream.status
    > 'OK'
    stream.error_count
    > 0

To load a stream for use later on:

.. sourcecode:: python

    > siegetank.load_stream('6918e316-5c6f-425d-8c1e-902f4b0ba144:vspg11')

To delete the stream:

.. sourcecode:: python

    > stream.delete()

Additional API documentation is available above.
