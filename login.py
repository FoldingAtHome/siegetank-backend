import requests
import json
import ipaddress

token = None


def is_domain(url):
    """ Returns True if url is a domain """
    # if a port is present in the url, then we extract the base part
    base = url.split(':')[0]
    try:
        ipaddress.ip_address(base)
        return False
    except Exception:
        return True


def login(auth_token=None):
    """ Set the credentials into a singleton """
    global token
    token = auth_token


def generate_token(cc, email, password):
    print(email, password)
    data = {
        "email": email,
        "password": password
    }
    uri = 'https://'+cc+'/managers/auth'
    #print(uri)
    print(json.dumps(data))
    reply = requests.post(uri, data=json.dumps(data), verify=is_domain(cc))
    if reply.status_code != 200:
        raise ValueError("Bad login credentials")
    return json.loads(reply.text)['token']
