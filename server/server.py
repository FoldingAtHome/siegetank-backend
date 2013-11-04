from flask import Flask, request, abort, jsonify
from functools import wraps
from sqlalchemy.orm import sessionmaker
import uuid
import SQLTypes
import base64

app = Flask(__name__)

@app.route('/st/auth', methods=['POST'])
def authenticate():
	if not request.json:
		abort(400)
	if not 'username' in request.json or not 'password' in request.json:
			abort(400)

	session = Session()

	username = request.json['username']
	password = request.json['password']

	found_user = False

	# each authentication generates a new token
	for instance in session.query(SQLTypes.User).filter(SQLTypes.User.username==username):
		if instance.password == password:
			user_hash = str(uuid.uuid4())
			found_user = True
			instance.token = user_hash

	if not found_user:
		abort(401)

	session.commit()
	session.close()

	return jsonify( {'token': str(user_hash) } ) 

def require_auth(func):
	print 'require_auth'
	@wraps(func)
	def inner():
		if 'token' in request.json:
			token = request.json['token']
			session = Session()
			for instance in session.query(SQLTypes.User).filter(SQLTypes.User.token==token):
				auth_user = instance.username
			try:
				auth_user
				print 'authenticated!'
			except NameError:
				abort(401)
		else:
			abort(401)
		return func()
	return inner

@app.route('/st/projects', methods=['POST'])
@require_auth
def post_project():

	required_options = 'descriptions'

	if 'description' in request.json:
		description = request.json['description']
	else:
		abort(400)

	if 'system' in request.json:
		system = request.json['system']
	else:
		abort(400)

	if 'integrator' in request.json:
		integrator = request.json['integrator']
	else:
		abort(400)

	allowed_formats = ['xtc']

	if 'frame_format' in request.json:
		frame_format = request.json['frame_format']
		if not frame_format in allowed_formats:
			abort(400)
	else:
		frame_format = 'xtc'

	if 'steps_per_frame' in request.json:
		steps_per_frame = request.json['steps_per_frame']
	else:
		steps_per_frame = 50000

	if 'precision' in request.json:
		precision = request.json['precision']
	else:
		precision = 3

	session = Session()

	for instance in session.query(SQLTypes.User).filter(SQLTypes.User.token==request.json['token']):
		auth_user = instance

	project_id = str(uuid.uuid4())

	auth_user.projects.append(SQLTypes.Project(project_id, description, base64.b64encode(system), base64.b64encode(integrator), steps_per_frame, frame_format, precision))

	session.commit()
	session.close()

	return jsonify( { 'project_id' : project_id} )

if __name__ == '__main__':
	Session = SQLTypes.initialize()
	app.run(debug = True)