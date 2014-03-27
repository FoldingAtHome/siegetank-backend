Backend Deployment
==================

The backend server code tries to be as self contained as possible, and dependencies are generally limited to very well known python packages. Only python 3.3.3 and higher is supported. You can check the version of python by

.. sourcecode:: bash

    $ python --version
    Python 3.3.3

Next, install pip, the required packages, and build redis. 

.. sourcecode:: bash

    easy_install pip
    pip install -r requirements.txt
    cd redis; make -j4; cd ..

Then run the unit tests:

.. sourcecode:: bash
    
    nosetests

Configuration
-------------

The ``cc.conf`` and ``ws.conf`` files provide typical setup configurations. Note that this is just a regular python file. Each command center has a secret password called ``cc_pass``. This password is used for workservers to automatically register themselves to the command centers. The information for the cc is added to the ``command_centers`` section in the ``ws.conf`` file. A good way to generate the password for the cc is:

.. sourcecode:: bash
    
    $> python
    >>> import os, hashlib
    >>> hashlib.sha256(os.urandom(2048)).hexdigest()
    '6f14e0d553d5ad81d03b8808ca3c73e0d1eb1d65ee131281471c23b689d63489'

Another section of interest is:

.. sourcecode:: python

    external_options = {
        'external_url': '127.0.0.1',
        'external_http_port': '8960'
    }

This specifies the externally visible url and port that should be used by the CC to communicate with the WS. ``external_url`` corresponds to the signed certificate's domain name, eg: *proteneer.stanford.edu*. The workserver listens on ``internal_http_port`` and iptables can used to redirect incoming requests from ``external_http_port`` to ``internal_http_port``.

SSL Certificates
----------------

The new backend requires SSL for both HTTPS requests as well as MongoDB communication. If you are deploying workservers on a \*.stanford.edu domain, use `this link <https://itservices.stanford.edu/service/ssl/>`_ to request free SSL certificates for your machine. Note that you must own the machine the subdomain points to.

You should have three files that correspond to the options:

* *ssl_certfile* - a signed, public certificate issued by a CA
* *ssl_key* - the private key used to generate the initial signing request
* *ssl_ca_certs* - a CA chain intermediate connecting the certs

These files should be placed under the ``certs`` folder in the root directory.

Database Configuration
----------------------

The new backend uses MongoDB to store data on donors, stream statistics, authentication, and managers. This information is shared across all Command Center and Work Servers. Generally you won't need to deploy your own MongoDB instances.

As noted above, all communication to the database must be encrypted using SSL. In particular, you cannot use a vanilla build obtained from package managers such as apt-get. SSL support must be compiled in from source for ``mongod``, ``mongos``, and ``mongo``. However, pymongo python drivers work fine out of the box via pip.

Additional instructions for building and starting MongoDB with SSL support are available `here <http://docs.mongodb.org/manual/tutorial/configure-ssl/>`_.
