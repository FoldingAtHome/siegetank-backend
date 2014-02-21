# coding: utf-8
#
# Copyright (c) Alexandr Emelin. BSD license.
# All rights reserved.
#

import tornado.gen
import tornado.web
import json
import tornado.httpclient
from urllib.parse import urlencode, parse_qsl


def secret_cookie():
    return "f8600ffc391a8f14b55eb5a4332803fc1a203529"


class MainHandler(tornado.web.StaticFileHandler):
    def get(self):
        print(self.cookies)
        if self.get_cookie("cookie_monster") == secret_cookie():
            return self.write(":)")
        else:
            self.redirect("/auth/github")
            return


class GithubAuthHandler(tornado.web.StaticFileHandler):

    x_site_token = 'application'
    client_id = "0668d6beb960856fcc1a"
    client_secret = "3c61f3b7aa9d9ec0082af1cad5aa08596c6c9c01"
    proteneer_access_token = "a241c13240f05ca864d000ad1e50daa64a135ecd"

    @tornado.gen.coroutine
    def get(self):
        """ Located at /auth/github.

        Phase 1:

            Get code

        Phase 2:

            Redirect back to this URI with the code as an argument

        Phase 3:

            Get the access token

        Phase 4:

            Check and see if user is a collaborator for the backend repo

        """

        code = self.get_argument('code', None)

        if code:
            # todo check for cross site forgery.
            print('Passed Phase 1')
            parameters = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'code': code,
                'redirect_uri': "http://127.0.0.1:9430/",
            }
            uri = "https://github.com/login/oauth/access_token"
            client = tornado.httpclient.AsyncHTTPClient()
            try:
                reply = yield client.fetch(uri, method='POST',
                                           body=urlencode(parameters))
                access_token = dict(parse_qsl(reply.body.decode()))\
                    ['access_token']
                headers = {'Authorization': 'token '+access_token,
                           'User-Agent': 'Tornado OAuth'}
                client = tornado.httpclient.AsyncHTTPClient()
                uri = "https://api.github.com/user"

                reply = yield client.fetch(uri, headers=headers)
                content = json.loads(reply.body.decode())
                username = content['login']
                uri = "https://api.github.com/repos/proteneer/backend"+\
                      "/collaborators/"+username
                headers['Authorization'] = 'token '+self.proteneer_access_token
                reply = yield client.fetch(uri, headers=headers)
                self.set_cookie("cookie_monster", secret_cookie())
                self.redirect('/static/index.html')

            except Exception as e:
                print(e)
                return self.write(
                    """
                    <!DOCTYPE html>
                    <html>
                    <body>
                    <h1>:(</h1>
                    </body>
                    </html>
                    """)

        else:
            parameters = {
                'client_id': self.client_id,
                'state': self.x_site_token,
                'redirect_uri': "http://127.0.0.1:9430/auth/github",
            }

            uri = "https://github.com/login/oauth/authorize?"
            self.redirect(uri+urlencode(parameters))


class AuthStaticFileHandler(tornado.web.StaticFileHandler):

    @tornado.web.authenticated
    def get(self, path):
        print('PATH')
        tornado.web.StaticFileHandler.get(self, path)

    def get_current_user(self):
        if self.get_cookie("cookie_monster") != secret_cookie():
            return None
        else:
            return True

if __name__ == "__main__":
    application = tornado.web.Application([
        (r"/auth/github", GithubAuthHandler),
        (r"/", MainHandler),
        (r'/static/(.*)', AuthStaticFileHandler, {'path': "_build/html"})])

    print("starting server on port 9430")
    application.listen(9430)
    tornado.ioloop.IOLoop.instance().start()
