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
# SET 	KEY		'users'					| set of usernames
# HASH  KEY		'user:'+id				| id of the user
# 		FIELD	'password'				| password of the user
#		FIELD   'token'					| authentication token
#		FIELD 	'hours' 				| number of hours used	
# SET 	KEY 	'user:'+id+':targets' 	| list of target ids
# STRNG KEY		'target:'+id+':cc' 		| which CC the target is on
# STRNG KEY 	'token:'+id+':user' 	| which user the token belongs to

class BaseHandler(tornado.web.RequestHandler):
    @property
    def db(self):
        return self.application.db

class VerifyHandler(tornado.web.RequestHandler):
	def get(self):
		''' Get the user the token belongs to '''
		try:
			token_id = self.request.headers['token']:
			user_id = self.db.get('token:'+token_id+':user')
			if user_id:
				self.set_status(200)
				self.write(user_id)
			else:
				self.set_status(401)
		except Exception as e:
			self.set_status(400)
			self.write('Missing token header')

class TargetHandler(tornado.web.RequestHandler):
	def get(self):
		''' Return a list of targets owned by this user '''
		try:
			token_id = self.request.headers['token']:
			user_id = self.db.get('token:'+token_id+':user')
			if user_id:
				# return a list of tokens and the ip of the cc its on
				# 
				targets = self.db.smembers('user:'+user_id+':targets')
				if targets:
					ccs = []
					ips = []
					for target_id in targets:
						cc = self.db.get('target:'+target_id+':cc')
						ccs.append(cc)
					self.set_status(200)
					self.write(json.dump({dict(zip(targets,ccs))})
			else:
				self.set_status(401)
		except Exception as e:
			self.set_status(400)
			self.write('Missing token header')
