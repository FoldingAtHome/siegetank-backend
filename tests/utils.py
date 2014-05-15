import uuid
import pymongo


def add_user(user='proteneer', manager=False, admin=False):
    token = str(uuid.uuid4())
    password = 'riesling'
    email = 'gibberish@gmail.com'
    db_body = {'_id': user,
               'password': password,
               'email': email,
               'token': token}
    mdb = pymongo.MongoClient()
    mdb.users.all.insert(db_body)
    result = db_body
    result['user'] = user
    if manager:
        weight = 1
        db_body = {'_id': user, 'weight': weight}
        mdb.users.managers.insert(db_body)
        result['weight'] = weight
    if admin:
        db_body = {'_id': user}
        mdb.users.admins.insert(db_body)
    return result
