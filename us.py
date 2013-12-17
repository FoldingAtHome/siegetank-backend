import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver

import datetime
import hashlib
import json
import os
import uuid
import random
import requests
import redis
import ConfigParser
import signal
import sys

import functools

import common
import hashset

# User Server
# 
# Manages Folding@home users. All connections to the US must use HTTPS since
# we do not roll our own crypto. We do not allow external access to the 
# underlying redis db since we would otherwise need to use something like
# stunnel. Since we have to have a tornado server anyways, we might as well as
# wrap all functionality behind it to prevent MitM attacks. 

# [ US ]
#
# HASH  KEY     'user:'+id              | id of the user
#       FIELD   'password'              | password of the user
#       FIELD   'token'                 | authentication token
#       FIELD   'email'                | user email
# SET   KEY     'user:'+id+':targets'   | set of target ids belonging to user
# STRNG KEY     'target:'+id+':cc'      | which CC the target is on
# STRNG KEY     'token:'+id+':user'     | which user the token belongs to

# STORAGE REQUIREMENTS: O(Number of Targets).

class User(hashset.HashSet):
    prefix = 'user'
    fields = {'password'    : str,
              'token'       : str,
              'email'      : str,
              'targets'     : set,
             }
    lookups = {'token'}

# TODO: Change passwords to use bcrypt

def cc_access(f):
    @functools.wraps(f)
    def decorated(self,*args,**kwargs):
        if self.request.remote_ip != '127.0.0.1':
            self.set_status(401)
            print 'UNAUTHORIZED'
            return
        else:
            return f(self,*args, **kwargs)
    return decorated

class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

class VerifyHandler(BaseHandler):
    @cc_access
    def get(self):
        ''' Get the user the token belongs to '''
        if self.request.remote_ip != '127.0.0.1':
            self.set_status(401)
            return  
        try:
            token_id = self.request.headers['token']
            user_id = User.lookup('token',token_id,self.db)
            if user_id:
                self.set_status(200)
                self.write(user_id)
            else:
                self.set_status(401)
        except Exception as e:
            self.set_status(400)
            self.write('Missing token header')

class AuthHandler(BaseHandler):
    def post(self):
        ''' Generate a token used for id purposes, the token generated
        is NOT a function of the password, it is a completely random
        hash. Each time this is called, a new user token is generated.
        '''
        try:
            content = json.loads(self.request.body)
            username = content['username']
            password = content['password']
            digest = hashlib.sha256(os.urandom(256)).hexdigest()
            user = User.instance(username, self.db)
            user['token'] = digest
            self.set_status(200)
            return self.write(digest)
        except Exception as e:
            print e
            self.set_status(401)

class UserHandler(BaseHandler):
    def get(self):
        ''' Return a list of targets owned by this user '''
        try:
            token_id = self.request.headers['token']
            user_id = User.lookup('token',token_id, self.db)
            user = User.instance(user_id, self.db)
            if user:
                # return a list of targets and the ip of the cc its on
                targets = user['targets']
                if targets:
                    ccs = []
                    for target_id in targets:
                        cc = self.db.get('target:'+target_id+':cc')
                        ccs.append(cc)
                    self.set_status(200)
                    self.write(json.dumps(dict(zip(targets,ccs))))
            else:
                self.set_status(401)
        except Exception as e:
            print 'Exception: ', e
            self.set_status(400)
    
    @cc_access
    def post(self):
        ''' Add a new user to the database '''
        try:
            content = json.loads(self.request.body)
            # json posts everything as unicode
            username = str(content['username'])
            password = str(content['password'])
            email    = str(content['email'])
            try: 
                User.instance(username,self.db)
                self.set_status(400)
                self.write('user:'+username+' already exists in db') 
                return
            except KeyError:
                pass
            user = User.create(username,self.db)
            user['password'] = password
            user['email'] = email
            self.set_status(200)
        except Exception as e:
            print 'ERROR:', e
            self.set_status(400)

    @cc_access
    def delete():
        pass

class TargetHandler(BaseHandler):
    @cc_access
    def post(self):
        ''' Add a new target owned by this user and indicate the CC it is on.'''
        try:
            content = json.loads(self.request.body)
            target  = str(content['target'])
            token   = str(content['token'])
            cc_id   = str(content['cc'])
            user_id = User.lookup('token',token,self.db)
            user = User.instance(user_id,self.db)
            user.sadd('targets',target)
            self.db.set('target:'+target+':cc',cc_id)
            self.set_status(200)
        except Exception as e:
            self.set_status(400)
            print 'TEST',e

class UserServer(tornado.web.Application, common.RedisMixin):
    def __init__(self,us_name,redis_port):
        self.db = common.init_redis(redis_port)
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        super(UserServer, self).__init__([
            (r'/verify', VerifyHandler),
            (r'/target', TargetHandler),
            (r'/user', UserHandler),
            (r'/auth', AuthHandler)
            ])

    def shutdown(self, signal_number=None, stack_frame=None):
        self.shutdown_redis()       
        print 'shutting down tornado...'
        tornado.ioloop.IOLoop.instance().stop()
        sys.exit(0)

def start():
    config_file = 'us_conf'
    Config = ConfigParser.ConfigParser()
    Config.read(config_file)    
    us_name       = Config.get('US','name')
    us_redis_port = Config.getint('US','redis_port')
    http_port     = Config.getint('US','http_port')
    us_instance   = UserServer(us_name, us_redis_port)
    us_server = tornado.httpserver.HTTPServer(us_instance,
        ssl_options = {'certfile' : 'ws.crt','keyfile' : 'ws.key'})
    us_server.listen(http_port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    start()