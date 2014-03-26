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
import signal
import tornado.options
import functools
import json


def sum_time(time):
    return int(time[0])+float(time[1])/10**6


def is_domain(url):
    """ Returns True if url is a domain """
    # if a port is present in the url, then we extract the base part
    base = url.split(':')[0]
    try:
        ipaddress.ip_address(base)
        return False
    except Exception:
        return True


def authenticate_manager(method):
    """ Decorator for handlers that require manager authentication. Based off
    of tornado's authenticated method.

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


def init_redis(redis_options):
    """ Spawn a redis subprocess port and returns a redis client.

        redis_options - a dictionary of redis options

        Parameters:
        redis_port - port of the redis server
        redis_pass - authenticate token. All other cilents must use
                     this token before they can send messages

    """
    redis_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                              '..', 'redis', 'src', 'redis-server')
    args = [redis_path]
    for option_name, option_value in redis_options.items():
        args.append('--'+option_name)
        args.append(str(option_value))
    redis_process = subprocess.Popen(args)

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
    def base_init(self, name, redis_options, mongo_options):
        """ A BaseServer is a server that is connected to both a redis server
        and a mongo server """
        self.name = name
        self.redis_options = redis_options
        self.mongo_options = mongo_options

        if 'logfile' in redis_options:
            if redis_options['logfile'] != os.devnull:
                redis_options['logfile'] += name
        if 'appendfilename' in redis_options:
            redis_options['appendonly'] = 'yes'
            redis_options['appendfilename'] += name
        self.db = init_redis(redis_options)

        if mongo_options:
            host = mongo_options['host']
            port = mongo_options['port']
            self.mdb = pymongo.MongoClient(host=host, port=port)
            self.mdb.community.donors.ensure_index("token")

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown_redis(self):
        print('shutting down redis...')
        self.db.shutdown()
        self.db.connection_pool.disconnect()

    def shutdown(self, signal_number=None, stack_frame=None, kill=True):
        print('shutting down server...')
        self.shutdown_redis()
        if kill:
            print('stopping tornado ioloop...')
            tornado.ioloop.IOLoop.instance().stop()
            sys.exit(0)


def configure_options(extra_options, conf_path):
    for option_name, option_type in extra_options.items():
        tornado.options.define(option_name, type=option_type)
    tornado.options.define('name', type=str)
    tornado.options.define('redis_options', type=dict)
    tornado.options.define('mongo_options', type=dict)
    tornado.options.define('internal_http_port', type=int)
    tornado.options.define('ssl_certfile', type=str)
    tornado.options.define('ssl_key', type=str)
    tornado.options.define('ssl_ca_certs', type=str)
    tornado.options.define('config_file', default=conf_path, type=str)
    tornado.options.parse_command_line()
    tornado.options.parse_config_file(tornado.options.options.config_file)
