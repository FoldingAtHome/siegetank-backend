import requests
import json
import functools
from siegetank.util import is_domain

token = None


def login(auth_token):
    """ Set the credentials into a singleton """
    global token
    global cc
    token = auth_token
    # haha :)
    headers = {'Authorization': auth_token}
    requests.get = functools.partial(requests.get, headers=headers)
    requests.post = functools.partial(requests.post, headers=headers)
    requests.put = functools.partial(requests.put, headers=headers)


def generate_token(cc, email, password):
    data = {
        "email": email,
        "password": password
    }
    uri = 'https://'+cc+'/managers/auth'
    reply = requests.post(uri, data=json.dumps(data), verify=is_domain(cc))
    if reply.status_code != 200:
        raise ValueError("Bad login credentials")
    return json.loads(reply.text)['token']
