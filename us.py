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
import redis
import configparser
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
              'email'       : str,
              'targets'     : set,
             }
    lookups = {'token'}

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
        
        Request Body:

            {
                'username' : username,
                'password' : password,
            }

        Response:
            
            {
                'authorization' : token
            }

        '''
        try:
            content = json.loads(self.request.body.decode())
            username = content['username']
            password = content['password']
            user = User.instance(username, self.db)
            if password != user['password']:
                self.set_status(401)
                return
            digest = hashlib.sha256(os.urandom(256)).hexdigest()
            user['token'] = digest
            self.set_status(200)
            auth_dict = { 'authorization' : digest }
            return self.write(json.dumps(auth_dict))
        except Exception as e:
            print(e)
            self.set_status(401)

class UsersHandler(BaseHandler):
    @cc_access
    def post(self):
        ''' Add a new user to the database. Body content must be
            in JSON format. 

            Request Body:

            {
                'username' : username,
                'password' : password,
                'email'    : email
            }

            Response:

            200 - OK
            400 - Bad Request

        '''

        try:
            content = json.loads(self.request.body.decode())
            username = content['username']
            password = content['password']
            email    = content['email']
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
            print('ERROR:', e)
            self.set_status(400)

    @cc_access
    def delete():
        ''' Delete a user from the db. 
            When a user is deleted, are the streams also deleted?'''

        pass

# Add a target:
# Restricted to CC
# POST x.com/targets

# Delete a target:
# Restricted to CC
# DELETE x.com/targets/id

# Get a list of targets
# GET x.com/targets
class TargetsHandler(BaseHandler):
    @cc_access
    def post(self):
        ''' Add a target owned by the user.

            Request Body:

                {
                    'target' : target_id,
                    'cc'     : cc_id
                    'user'   : user_id
                }

            Response:

                200 - OK

        '''
        try:
            content = json.loads(self.request.body.decode())
            target  = content['target']
            cc_id   = content['cc']
            user_id = content['user']
            user = User.instance(user_id,self.db)
            user.sadd('targets',target)
            self.db.set('target:'+target+':cc',cc_id)
            self.set_status(200)
        except Exception as e:
            self.set_status(400)

    def get(self):
        ''' Return a list of targets and the cc it resides on

            Required Headers:

                Authorization

            Response:

                { target_id_1 : cc_id,
                  target_id_2 : cc_id } 

        '''

        try:
            auth_token = self.request.headers['Authorization']
            user_id = User.lookup('token', auth_token, self.db)
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
            print('Exception: ', e)
            self.set_status(400)

class DeleteTargetHandler(BaseHandler):
    @cc_access
    def delete(self):
        ''' Delete a target owned by this user. '''
        try:    
            content = self.request.headers
            target  = str(content['target'])
            token   = str(content['token'])
            user_id = User.lookup('token',token,self.db)
            user = User.instance(user_id, self.db)
            count = user.srem('targets',target)
            if count == 0:
                self.write('Target not removed')
                self.set_status(400)
                return 
            self.db.delete('target:'+target+':cc')
            self.set_status(200)
        except Exception as e:
            print(e)
            self.set_status(400)

class UserServer(tornado.web.Application, common.RedisMixin):
    def __init__(self,us_name,redis_port):
        self.db = common.init_redis(redis_port)
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        super(UserServer, self).__init__([
            (r'/verify', VerifyHandler),
            (r'/targets', TargetsHandler),
            (r'/users', UsersHandler),
            (r'/auth', AuthHandler)
            ])

    def shutdown(self, signal_number=None, stack_frame=None):
        self.shutdown_redis()       
        print('shutting down tornado...')
        tornado.ioloop.IOLoop.instance().stop()
        sys.exit(0)

def start():
    config_file = 'us_conf'
    Config = configparser.ConfigParser()
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