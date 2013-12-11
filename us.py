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
#       FIELD   'e-mail'                | user email
# SET   KEY     'user:'+id+':targets'   | list of target ids
# STRNG KEY     'target:'+id+':cc'      | which CC the target is on
# STRNG KEY     'token:'+id+':user'     | which user the token belongs to

# STORAGE REQUIREMENTS: O(Number of Targets).

# TODO: Change passwords to use bcrypt

def cc_access(f):
    @functools.wraps(f)
    def decorated(self,*args,**kwargs):
        if self.request.remote_ip != '127.0.0.1':
            self.set_status(401)
            return
        else:
            return f(self,*args, **kwargs)
    return decorated

'''
def require_auth(f):
    @functools.wraps(f)
    def decorated(self,*args,**kwargs):
        token = self.request.headers['token']
        if self.get_user(token):
            return f(self,*args, **kwargs)
        else:
            self.set_status(401)
            return
    return decorated
'''

class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

    def get_user(self,token):
        return self.db.get('token:'+token+':user')

    def get_token(self,user):
        return self.db.hget('user:'+user,'token')

    def set_token(self,user,token):
        self.db.hset('user:'+user,'token',token)
        self.db.set('token:'+token+':user',user)

    def add_target(self,user,target,cc):
        self.db.sadd('user:'+user+':targets',target)
        self.db.set('target:'+target+':cc',cc)

class VerifyHandler(BaseHandler):
    @cc_access
    def get(self):
        ''' Get the user the token belongs to '''
        if self.request.remote_ip != '127.0.0.1':
            self.set_status(401)
            return  
        try:
            token_id = self.request.headers['token']
            user_id = self.get_user(token_id)
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
            old_token = self.get_token(username)
            if old_token:
                self.db.delete('token:'+old_token+':user')
            self.set_token(username,digest)

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
            user_id = self.get_user(token_id)
            if user_id:
                # return a list of tokens and the ip of the cc its on
                targets = self.db.smembers('user:'+user_id+':targets')
                if targets:
                    ccs = []
                    ips = []
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
            # make sure all relevant fields exist beflore writing to db
            username = content['username']
            password = content['password']
            email    = content['email']
            if self.db.exists('user:'+username):
                self.set_status(400)
                self.write('user:'+username+' already exists in db') 
                return
            self.db.hset('user:'+username,'password',password)
            self.db.hset('user:'+username,'email',email)
            self.set_status(200)
        except Exception as e:
            print 'ERROR:', e
            self.set_status(400)

    def delete():
        pass

class TargetHandler(BaseHandler):
    @cc_access
    def post(self):
        ''' Add a new target owned by this user and indicate the CC it is on.'''
        try:
            content = json.loads(self.request.body)
            target  = content['target']
            token   = content['token']
            cc_id   = content['cc']
            user_id = self.get_user(token)
            self.add_target(user_id,target,cc_id)
            self.set_status(200)
        except Exception as e:
            self.set_status(400)
            print e

class UserServer(tornado.web.Application, common.RedisMixin):
    def __init__(self,us_name,redis_port):
        self.db = self.init_redis(redis_port)
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