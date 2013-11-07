from flask import Flask, request, abort, jsonify
from functools import wraps
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from common import ws_types
from random import sample

# Siegetank Workserver
# Load (system.xml, integrator.xml) data from underlying postgres db into redis cache
# Generate a priority queue on redis for each project
# 

app = Flask(__name__)

# get a set of system.xml, state.xml, integrator.xml
@app.route('/ws/stream', methods=['GET'])
def get_stream():

    session = Session()



    # todo - load get id from priority queue

    for project in session.query(ws_types.Project).filter(ws_types.Project.uuid == 'dbfcae5a-1b9b-4c08-9407-99341dc110f2'):
        print 'foo'
        json_proj = project.system
        json_intg = project.integrator
        for stream in session.query(ws_types.Stream).filter(ws_types.Stream.uuid == '198b8b57-6efb-419d-a5bf-36bc63d9602b'):
            print 'bar'
            json_chkpt = stream.state

    return jsonify( {'payload' : [json_proj, json_intg, json_chkpt] } ) 

    session.close()
# match by client id
#@app.route('/ws/add_frame', methods=['GET'])
#def get_strea

if __name__ == "__main__":
    engine = create_engine('postgresql://postgres:random@proline/sandbox2', echo=True)
    ws_types.Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    app.run(debug = True, host='0.0.0.0')