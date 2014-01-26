// OS X SSL instructions:
// ./Configure darwin64-x86_64-cc -noshared
// g++ -I ~/poco/include Core.cpp ~/poco/lib/libPoco* /usr/local/ssl/lib/lib*

#include <iostream>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLException.h>
#include <Poco/Net/X509Certificate.h>
#include <Poco/Util/Application.h>
#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/x509.h>

#include <fstream>
#include <string>
#include <streambuf>
#include <sstream>

#include <Security/Security.h>

using namespace std;
using namespace Poco::Net;
using namespace Poco::Util;
using namespace Poco;


class TestApp : public Application {

public:
    TestApp() {};
    ~TestApp() {};

    int main() {
        cout << "creating session" << endl;
        HTTPSClientSession session("www.yahoo.com", 443);
        cout << "creating get" << endl;
        HTTPRequest request("GET", "/");
        cout << "sending request" << endl;
        session.sendRequest(request);
        cout << '1' << endl;
        HTTPResponse response;
        cout << '1' << endl;
        session.receiveResponse(response);
        cout << response.getStatus() << endl;
        return 0;
    }

};

void read_cert_into_ctx(istream &some_stream, SSL_CTX *ctx) {
    // Add a stream of PEM formatted certificate strings to the trusted store
    // of the ctx.
    string line;
    string buffer;
    while(getline(some_stream, line)) {
        buffer.append(line);
        buffer.append("\n");
        if(line == "-----END CERTIFICATE-----") {
            BIO *bio;
            X509 *certificate;
            bio = BIO_new(BIO_s_mem());
            BIO_puts(bio, buffer.c_str());
            certificate = PEM_read_bio_X509(bio, NULL, 0, NULL);
            if(certificate == NULL)
                throw std::runtime_error("could not add certificate to trusted\
                                          CAs");
            X509_STORE* store = SSL_CTX_get_cert_store(ctx);
            int result = X509_STORE_add_cert(store, certificate);
            BIO_free(bio);
            buffer = "";
        }
    }
}


int main() {

    Context::Ptr context = new Context(Context::CLIENT_USE, "", 
        Context::VERIFY_RELAXED, 9, false);

    SSL_CTX *ctx = context->sslContext();
    std::ifstream t("rootcert.pem");
    std::string str((std::istreambuf_iterator<char>(t)),
                     std::istreambuf_iterator<char>());
    stringstream ss;
    ss << str;

    read_cert_into_ctx(ss, ctx);

    try {
        cout << "creating session" << endl;
        HTTPSClientSession session("www.yahoo.com", 443, context);
        cout << "creating get" << endl;
        HTTPRequest request("GET", "/");
        cout << "sending request" << endl;
        session.sendRequest(request);
        cout << "obtaining response" << endl;
        HTTPResponse response;
        session.receiveResponse(response);
        cout << response.getStatus() << endl;
        return 0;

    } catch(Exception &e) {
        cout << e.what() << endl;
        cout << e.displayText() << endl;
    }

}