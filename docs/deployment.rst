Backend Deployment
==================

The workserver tries to be as self contained as possible, and dependencies are
generally limited to very well known python packages. First, you must install
python 3.3 (or better). Afterwards, make sure running ``python`` shows:

.. sourcecode:: bash

    Python 3.3.3 (default, Jan  6 2014, 11:04:52) 
    [GCC 4.6.3] on linux
    Type "help", "copyright", "credits" or "license" for more information.
    >>> 

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

The ``cc.conf`` and ``ws.conf`` files provide typical setup configurations. Note that this is just a regular python file. Each Command Center has a secret password called ``cc_pass``. This password is used for WorkServers to register themselves to the Command Center. A good way to generate this key is:

.. sourcecode:: bash
    
    $> python
    Python 3.3.3 (v3.3.3:c3896275c0f6, Nov 16 2013, 23:39:35) 
    [GCC 4.2.1 (Apple Inc. build 5666) (dot 3)] on darwin
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import os, hashlib
    >>> hashlib.sha256(os.urandom(2048)).hexdigest()
    '6f14e0d553d5ad81d03b8808ca3c73e0d1eb1d65ee131281471c23b689d63489'
    
Note that there are the odds guessing it randomly is about 1 in 10^100. Each WS that intends to register itself with the CC must present the cc_pass.

SSL Certificates
----------------

The new backend requires SSL, as HTTPS is fast becoming the Stanford. If deploying workservers on a \*.stanford.edu account, use `this link <https://itservices.stanford.edu/service/ssl/>`_ to request free SSL certificates for your machine. Note that you must own the machine the subdomain points to.

You should have three files that correspond to the options:

* *ssl_certfile* - a signed, public certificate issued by a CA
* *ssl_key* - your private key used to generate the initial signing request
* *ssl_ca_certs* - a CA chain that connects the CAs

