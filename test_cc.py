import ws
import cc
import hashlib
import redis
import tornado.ioloop
from tornado.testing import AsyncHTTPTestCase
import unittest
import subprocess
import json
import time
import uuid
import base64
import os
import random
import struct
import requests
import shutil
import cStringIO
import tarfile

class CCTestCase(AsyncHTTPTestCase):
	@classmethod
	def setUpClass(self):