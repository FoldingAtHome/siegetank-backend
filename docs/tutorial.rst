Tutorial
===============

.. note:: This tutorial assumes python 3.3 or later is used.

Begin by logging in with your token.

.. sourcecode:: python

    import siegetank
    siegetank.login('your_unique_token')


Creating Targets
----------------

To add a target, you must provide a dictionary of options (which differ for each `engine <engines.html>`_) along with a list of engines you'd like to use. Optionally, you can configure the weight of the target, which is relative to other targets owned by you. If target A has a weight of 2, and target B has a weight 1, then target A will be assigned twice as often as B. In addition, the default stage of the target is ``private``, which restricts access from public cores until you're ready to make it ``public``.

.. sourcecode:: python
    
    opts = {'description': 'Benchmark Protein', 'steps_per_frame': 50000}
    target = siegetank.add_target(options=opts, engines=['openmm_601_opencl', 'openmm_601_cpu'])

A target also has a bunch of properties that you can query:

.. sourcecode:: python

    target.options
    > {'description': 'Benchmark Protein', 'steps_per_frame': 50000}
    target.creation_date
    > 1401560581.7190273,
    target.engines
    > ['openmm_601_opencl', 'openmm_601_cpu']
    target.stage
    > 'private'
    target.weight
    > 1

To get a list of all your targets later on:

.. sourcecode:: python

    siegetank.list_targets()
    > [<target 1740a921-923c-4518-b1f5-a6e964418614>]

To load your target from its id:

.. sourcecode:: python

    target = siegetank.load_target('1740a921-923c-4518-b1f5-a6e964418614')

To delete the target (only allowed if you've removed all of its streams):

.. sourcecode:: python

    target.delete()

Querying SCVs
-------------

The SCV is the main workhorse backend server, which stores binary data for streams. SCVs are owned by different research groups, so please make sure you have permission from them before adding your streams to them. To get a dictionary of scvs and their status:

.. sourcecode:: python

    siegetank.scvs
    > {'proline': {
            'host': 'proline.stanford.edu:443',
            'online': True},
        'vspg11': {
            'host': 'vspg11.stanford.edu:443',
            'online': True}}

Adding Streams
--------------

A stream is defined by a dict of files and a particular SCV it resides on. The set of files to use depends on the particular engine of interest. The files must be encoded properly prior to submission. For example, OpenMM based cores expect XML files that are gzipped and base64 encoded, with the names ``system.xml.gz.b64``, ``state.xml.gz.b64``, and ``integrator.xml.gz.b64``. The following shows an example using pre-generated and gzipped files.

.. sourcecode:: python

    import requests  # utility to fetch a set of pre-generated xml files
    import base64

    state_url = 'http://web.stanford.edu/~yutongz/state.xml.gz'
    system_url = 'http://web.stanford.edu/~yutongz/system.xml.gz'
    integrator_url = 'http://web.stanford.edu/~yutongz/integrator.xml.gz'
    state_gz = requests.get(state_url).content
    system_gz = requests.get(system_url).content
    integrator_gz = requests.get(integrator_url).content

If you have your XML files on disk, you can use the built-in gzip module:

.. sourcecode:: python

    import gzip

    system_gz = gzip.compress(open('my_system.xml', 'rb').read())
    state_gz = gzip.compress(open('my_state.xml', 'rb').read())
    system_gz = gzip.compress(open('my_integrator.xml', 'rb').read())

Once you've gzipped your files, apply a base64 encoding so they are JSON compatible.

.. sourcecode:: python

    encoded_system = base64.b64encode(system_gz).decode()
    encoded_intg = base64.b64encode(integrator_gz).decode()
    encoded_state = base64.b64encode(state_gz).decode()

    data = {
        'system.xml.gz.b64': encoded_system,
        'state.xml.gz.b64': encoded_state,
        'integrator.xml.gz.b64': encoded_intg
    }

    stream = target.add_stream(files=data, scv='vspg11')
    > <stream 6918e316-5c6f-425d-8c1e-902f4b0ba144:vspg11 s:OK f:0>

.. note:: the slightly awkward base64.b64encode() followed by a decode() is a subtle python3 issue because b64encode() returns ``bytes`` which must be converted to the unicode ``str``.

The stream descriptor looks like <stream xxxxx: s: OK f:0>, where s: indicates if the stream is OK or not, and f:0 indicates the number of frames. To get more information about the recently added stream:

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

    stream = siegetank.load_stream('6918e316-5c6f-425d-8c1e-902f4b0ba144:vspg11')

To delete the stream:

.. sourcecode:: python

    stream.delete()

Testing Your Stream
-------------------

Before you change the stage of your target to public, you should do testing with `pre-built cores <http://www.stanford.edu/~yutongz/ocores/>`_. These cores do not need a client to function, and can be run as is. They are built for Ubuntu 12.04/14.04 and are linked against OpenMM 6.0.1. Always get the latest version if possible.

By default, a target's stage is private, only cores that explicitly specify your target's id can obtain its streams. To test your private target, use:

..  sourcecode:: bash
    
    > ./ocore_xxx --target <your target's id>

to check if your target is functioning correctly. A good, scalable way of testing your target is to launch cores on AWS EC2 g2.2xlarge spot instances to test GPUs, or c3.large spot instances to test CPUs.

Analysis
--------

To retrieve the data for a given stream for analysis, an rsync-like API is provided. ``sync()`` incrementally fetches only the missing parts of stream data (as opposed to downloading everything in one go). It is highly recommended to sync and backup your data periodically (eg. once a day). Note that the onus of backing up the data is on the managers (though we do try our best to configure SCVs to use RAID6 hard drives).

.. sourcecode:: python

    data_folder = stream.id+'_data'
    stream.sync(data_folder) 

This will populate the folder with the stream's data. Let's look at the resulting folder:

.. sourcecode:: python

    import os

    os.listdir(data_folder)
    > ['2', '209', '38', '592']
    # important: sort by actual integer values and not lexicographical ordering
    sorted_folders = sorted(os.listdir('test_dir'), key=lambda value: int(value))
    > ['2', '38', '209', '592']

When storing the folder names, the stream is partitioned into the (open, closed] intervals: (0, 2](2, 38](38, 209](209, 592]. Each interval has a sub-folder called checkpoint_files that corresponds to the last frame. For OpenMM based targets, the ``frames.xtc`` file inside each folder can be binary appended to get a single xtc file.

.. sourcecode:: python

    concatenated_data = bytes()
    for folder in sorted_folders:
        frame_path = os.path.join(data_folder, folder, 'frames.xtc')
        concatenated_data += open(frame_path, 'rb').read()

.. note:: mdtraj supports loading in multiple xtc files via a list.