import uuid
import pymongo
import ipaddress


def is_domain(url):
    """ Returns True if url is a domain """
    # if a port is present in the url, then we extract the base part
    base = url.split(':')[0]
    if base == 'localhost':
        return False
    try:
        ipaddress.ip_address(base)
        return False
    except Exception:
        return True


def add_user(user='proteneer', manager=False, admin=False, mongo_options=None):
    token = str(uuid.uuid4())
    password = 'riesling'
    email = 'gibberish@gmail.com'
    db_body = {'_id': user,
               'password': password,
               'email': email,
               'token': token}

    if not mongo_options:
        mdb = pymongo.MongoClient()
    else:
        if is_domain(mongo_options['host']):
            mongo_options['ssl'] = True
        if 'replicaSet' in mongo_options:
            mdb = pymongo.MongoReplicaSetClient(**mongo_options)
            mdb.read_preference = pymongo.ReadPreference.SECONDARY_PREFERRED
        else:
            mdb = pymongo.MongoClient(**mongo_options)

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
