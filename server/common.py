# Tornado-powered workserver backend.
#
# Authors: Yutong Zhao <proteneer@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import redis
import subprocess
import sys
import time
import tornado
import ipaddress
import os
import pymongo
import motor
import signal
import tornado.options
import functools
import json
import logging


def sum_time(time):
    return int(time[0])+float(time[1])/10**6


def is_domain(url):
    """ Returns True if url is a domain """
    # if a port is present in the url, then we extract the base part
    base = url.split(':')[0]
    if base == 'localhost':
        return False
    try:
        ipaddress.ip_address(base)
        return False
    except Exception:
        return True


def authenticate_manager(method):
    """ Decorator for handlers that require manager authentication. Based off
    of tornado's authenticated method. This method only checks to see if the
    given token corresponds to a manager. It does not check if said manager
    should have access to some resource.

    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            self.request.headers['Authorization']
        except:
            self.write(json.dumps({'error': 'missing Authorization header'}))
            return self.set_status(401)
        if self.get_current_user():
            return method(self, *args, **kwargs)
        else:
            return self.set_status(401)

    return wrapper


def init_redis(redis_options, cwd=None):
    """ Spawn a redis subprocess port and returns a redis client.

        redis_options - a dictionary of redis options

        Parameters:
        redis_port - port of the redis server
        redis_pass - authenticate token. All other cilents must use
                     this token before they can send messages

    """
    redis_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                              '..', 'redis', 'src', 'redis-server')
    redis_path = os.path.abspath(redis_path)
    args = [redis_path]
    for option_name, option_value in redis_options.items():
        args.append('--'+option_name)
        args.append(str(option_value))
    redis_process = subprocess.Popen(args, cwd=cwd)

    if redis_process.poll() is not None:
        print('Could not start redis server, aborting')
        sys.exit(0)

    if 'requirepass' in redis_options:
        redis_pass = redis_options['requirepass']
    else:
        redis_pass = None

    if 'port' in redis_options:
        redis_port = redis_options['port']
    else:
        raise KeyError("port must be specified in redis options")

    redis_client = redis.Redis(host='localhost', password=redis_pass,
                               port=int(redis_port), decode_responses=True)
    # poll until redis server is alive
    alive = False
    start_time = time.time()
    while time.time()-start_time < 15.0:
        try:
            alive = redis_client.ping()
            break
        except Exception:
            pass
    if not alive:
        raise ValueError('Could not start redis')

    return redis_client


class BaseServerMixin():
    def initialize_motor(self):
        print('INITIALIZING MOTOR')
        mongo_options = self._mongo_options
        if mongo_options:
            host = mongo_options['host']
            ssl_kwargs = {}
            if is_domain(host):
                options = tornado.options.options
                try:
                    ssl_kwargs['ssl_certfile'] = options.ssl_certfile
                    ssl_kwargs['ssl_keyfile'] = options.ssl_keyfile
                    ssl_kwargs['ssl_ca_certs'] = options.ssl_ca_certs
                except AttributeError:
                    print('WARNING: SSL not enabled for MongoDB - this is OK\
                           if this message shows up during unit tests')
            self.motor = motor.MotorClient(host, **ssl_kwargs)

    def base_init(self, name, redis_options, mongo_options):
        """ A BaseServer is a server that is connected to both a redis server
        and a mongo server """
        self.name = name
        self.data_folder = name+'_data'
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
        self.redis_options = redis_options
        self.mongo_options = mongo_options

        channel = logging.handlers.RotatingFileHandler(
            filename=os.path.join(self.data_folder, 'server.log'))
        logging.getLogger('tornado.access').addHandler(channel)
        logging.getLogger('tornado.application').addHandler(channel)
        logging.getLogger('tornado.general').addHandler(channel)

        if 'appendfilename' in redis_options:
            redis_options['appendonly'] = 'yes'
        self.db = init_redis(redis_options, cwd=self.data_folder)

        self._mongo_options = mongo_options

        if mongo_options:
            host = mongo_options['host']
            ssl_kwargs = {}
            if is_domain(host):
                options = tornado.options.options
                try:
                    ssl_kwargs['ssl_certfile'] = options.ssl_certfile
                    ssl_kwargs['ssl_keyfile'] = options.ssl_keyfile
                    ssl_kwargs['ssl_ca_certs'] = options.ssl_ca_certs
                except AttributeError:
                    print('WARNING: SSL not enabled for MongoDB - this is OK\
                           if this message shows up during unit tests')
            self.mdb = pymongo.MongoClient(host, **ssl_kwargs)
            self.mdb.community.donors.ensure_index("token")

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown_redis(self):
        self.db.shutdown()
        self.db.connection_pool.disconnect()

    def shutdown(self, signal_number=None, stack_frame=None, kill=True):
        self.shutdown_redis()
        if kill:
            tornado.ioloop.IOLoop.instance().stop()
            sys.exit(0)


def configure_options(config_file, extra_options=None):
    if extra_options:
        for option_name, option_type in extra_options.items():
            tornado.options.define(option_name, type=option_type)
    tornado.options.define('name', type=str)
    tornado.options.define('redis_options', type=dict)
    tornado.options.define('mongo_options', type=dict)
    tornado.options.define('internal_http_port', type=int)
    tornado.options.define('ssl_certfile', type=str)
    tornado.options.define('ssl_key', type=str)
    tornado.options.define('ssl_ca_certs', type=str)
    tornado.options.define('external_host', type=str)
    tornado.options.parse_config_file(config_file)
