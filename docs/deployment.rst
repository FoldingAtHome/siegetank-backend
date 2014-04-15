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

The ``cc.conf`` and ``ws.conf`` files provide typical setup configurations. Note that this is just a regular python file. One important field in the config file is ``external_host``, which specifies the externally visible url and port that HTTP requests should be made to. The URL specified should correspond to host specified in the signed certificate, eg: *proteneer.stanford.edu*. The servers listen on ``internal_http_port`` and iptables can used to redirect incoming requests from ``external_http_port`` to ``internal_http_port`` (since only root can listen on ports <1024).

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
