import base64
import gzip
import os
import ipaddress


def is_domain(url):
    """ Returns True if url is a domain, domain in the form of:
        sub.domain.com:2345 """
    # if a port is present in the url, then we extract the base part
    base = url.split(':')[0]
    try:
        ipaddress.ip_address(base)
        return False
    except Exception:
        return True


def encode_files(files):
    encoded_files = {}
    for filename, value in files.items():
        f_root, f_ext = os.path.splitext(filename)
        if f_ext == '.b64':
            if isinstance(value, bytes):
                value = value.decode()
            encoded_files[filename] = value
        elif f_ext == '.gz':
            assert isinstance(value, bytes)
            encoded_files[filename] = base64.b64encode(value).decode()
        else:
            if isinstance(value, str):
                value = value.encode()
            b64f = base64.b64encode(gzip.compress(value)).decode()
            encoded_files[filename] = b64f
    return encoded_files
