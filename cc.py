from flask import Flask, request, abort, jsonify
from functools import wraps
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import uuid
import base64
from ws import ws_types

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
    for instance in session.query(ws_types.User).filter(ws_types.User.username==username):
        if instance.password == password:
            user_hash = str(uuid.uuid4())
            found_user = True
            instance.token = user_hash

    if not found_user:
        abort(401)

    session.commit()
    session.close()

    return jsonify( {'token': str(user_hash) } ) 

def _verify_token(session):
    if 'token' in request.json:
        token = request.json['token']
        for instance in session.query(ws_types.User).filter(ws_types.User.token==token):
            auth_user = instance
        try:
            auth_user
            print 'authenticated!'
        except NameError:
            abort(401)
        return instance
    else:
        abort(401)

@app.route('/st/projects', methods=['POST'])
def post_project():

    session = Session()
    auth_user = _verify_token(session)

    for item in request.json:
        # required
        if item == 'description':
            description = request.json[item]
        elif item == 'system':
            system = request.json[item]
        elif item == 'integrator':
            integrator = request.json[item]
        # optional
        elif item == 'frame_format':
            frame_format = request.json[item]
            if frame_format != 'xtc':
                abort(400)
        elif item == 'steps_per_frame':
            steps_per_frame = request.json[item]
        elif item == 'precision':
            precision = request.json[item]
        elif item == 'token':
            pass
        else:
            abort(400)

    # defaults
    try: 
        frame_format
    except NameError:
        frame_format = 'xtc'

    try: 
        steps_per_frame
    except NameError:
        steps_per_frame = 50000

    try:
        precision
    except NameError:
        precision =3

    project_id = str(uuid.uuid4())
    auth_user.projects.append(ws_types.Project(project_id, description, base64.b64encode(system), base64.b64encode(integrator), steps_per_frame, frame_format, precision))

    session.commit()
    session.close()

    return jsonify( { 'project_id' : project_id} )

@app.route('/st/projects/<project_id>', methods=['POST'])
def post_stream(project_id):

    session = Session()
    auth_user = _verify_token(session)

    auth_username = auth_user.username

    for instance in session.query(ws_types.Project).filter(ws_types.Project.uuid==project_id):
        project = instance

    if auth_username != project.owner:
        abort(401)

    states = request.json['states']

    stream_ids = []

    for state in states:
        stream_uuid = str(uuid.uuid4())
        project.streams.append(ws_types.Stream(stream_uuid, base64.b64encode(state)))
        stream_ids.append(stream_uuid)

    session.commit()
    session.close()

    return jsonify( { 'stream_ids' : stream_ids } )

if __name__ == "__main__":
    engine = create_engine('postgresql://postgres:random@proline/sandbox2', echo=True)
    ws_types.Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    app.run(debug = True, host='0.0.0.0')