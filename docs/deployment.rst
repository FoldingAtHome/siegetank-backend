Backend Deployment
==================

.. note:: This guide assumes deployment on Ubuntu 12.04 or later.

The backend server code tries to be as self contained as possible, and dependencies are generally limited to very well known python packages. Only python 3.3 and higher is supported. Install pip, the required packages, and build redis. 

.. sourcecode:: bash

    $ sudo apt-get install libffi-dev
    $ sudo apt-get install python3 python3-pip
    $ pip3 install -r requirements.txt
    $ cd redis; make -j4; cd ..

You can check the version of python by:

.. sourcecode:: bash

    $ python3 --version
    Python 3.4

Then run the unit tests:

.. sourcecode:: bash
    
    $ nosetests

Limits
------

The backend is designed to scale to hundreds of thousands of concurrent connections. In linux, sockets are implemented as file descriptors, so the number of maximum open files allowed will severely impact the performance of your server. To check what this number is:

.. sourcecode:: bash
	
	$ ulimit -n
	1024

To change the limits on Ubuntu based systems, you can either manually edit the ``/etc/security/limits.conf`` file, or run:

.. sourcecode:: bash

	$ sudo sh -c "echo ' * hard nofile 64000 ' >> /etc/security/limits.conf";
	$ sudo sh -c "echo ' * soft nofile 64000 ' >> /etc/security/limits.conf";

You will need to restart your server afterwards.

Configuration
-------------
The ``cc.conf`` and ``scv.conf`` files provide typical setup configurations. Note that this is just a regular python file. One important field in the config file is ``external_host``, which specifies the externally visible url and port that HTTP requests should be made to. The URL specified should correspond to host specified in the signed certificate, eg: *proteneer.stanford.edu*. The servers listen on ``internal_http_port`` and iptables can used to redirect incoming requests from ``external_http_port`` to ``internal_http_port`` (since only root can listen on ports <1024).

SSL Certificates
----------------
The new backend requires SSL certificates for security purposes. If you are deploying SCVs on a \*.stanford.edu domain, use `this link <https://itservices.stanford.edu/service/ssl/>`_ to request free SSL certificates for your machine.

You should have three files that correspond to the following fields in .conf files:

* *ssl_certfile* - a signed, public certificate issued by a CA
* *ssl_key* - the private key used to generate the initial signing request
* *ssl_ca_certs* - a CA chain intermediate connecting the certs

These files should be placed under the ``certs`` folder in the root directory.

.. note:: CCs that are deployed behind a load balancer have the option of not serving SSL traffic, provided that the proxy itself is terminating SSL traffic. To disable SSL, ``ssl_certfile``, ``ssl_key``, and ``ssl_ca_certs`` must **all** either be undefined or set to an empty string.

Database Configuration
----------------------
The backend uses MongoDB to store persistent data on donors, stream statistics, authentication, and managers. This information is shared across all CCs and SCVs. Generally you won't need to deploy your own MongoDB instances.

As noted above, all communication to the database must be encrypted using SSL. In particular, you cannot use a vanilla MongoDB build from package managers such as apt-get. SSL support must be compiled in from source for ``mongod``, ``mongos``, and ``mongo``.

Additional instructions for building and starting MongoDB with SSL support are available `here <http://docs.mongodb.org/manual/tutorial/configure-ssl/>`_.
