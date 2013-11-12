from flask import Flask, Response, request, abort, jsonify, make_response
import redis
import uuid
import hashlib

app = Flask(__name__)
redis_cli = redis.Redis()

# Redis always serves as the communication interface
# User, project, data are stored in the underlying database()

# project:uuid - maps to IP of WS holding the project
@app.route('/st/add_user', methods=['POST'])
def add_user():
    if not 'username' in request.json \
        or not 'password' in request.json \
        or not 'email' in request.json:
        abort(401)

    username = request.json['username']
    password = request.json['password']
    email = request.json['email']

    if len(username) > 10:
        return 'username longer than 10 characters', 401

    if username.isalnum() == False:
        return 'username not alpha numeric', 401

    # see if user already exists in db
    if redis_cli.sismember('users', username) == True:
        return 'user exists', 401
    else:
        redis_cli.sadd('users', username)
        redis_cli.hset('user:'+username, 'username', username)
        redis_cli.hset('user:'+username, 'password', password)
        redis_cli.hset('user:'+username, 'email', email)
        redis_cli.hset('user:'+username, 'token', hashlib.sha256(username+password).hexdigest())

    return 'Success', 200

@app.route('/st/auth', methods=['GET'])
def authenticate():
    if not request.json:
        abort(400)
    if not 'username' in request.json or not 'password' in request.json:
            abort(400)

    username = request.json['username']
    password = request.json['password']

    if not redis_cli.sismember('users', username):
        return 'Bad username', 401

    real_pw, token = redis_cli.hmget('user:'+username, ['password', 'token'])

    if password != real_pw:
        return 'Bad password', 401

    return jsonify( {'token': str(token) } ) 

if __name__ == "__main__":
    app.run(debug = True, host='0.0.0.0')