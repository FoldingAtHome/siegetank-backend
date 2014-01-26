// OS X SSL instructions:
// ./Configure darwin64-x86_64-cc -noshared
// g++ -I ~/poco/include Core.cpp ~/poco/lib/libPoco* /usr/local/ssl/lib/lib*

#include <iostream>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Crypto/OpenSSLInitializer.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLException.h>
#include <Poco/Net/X509Certificate.h>
#include <Poco/Util/Application.h>
#include <openssl/ssl.h>
//#include <Poco/Crypto/X509Certificate.h>
#include <openssl/bio.h>
#include <openssl/x509.h>
#include <openssl/pem.h>

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

void split_multi_pem(istream &some_stream, SSL_CTX *ctx) {
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
                cout << "ERROR" << endl;
            int result = SSL_CTX_add_client_CA(ctx, certificate);
            cout << result << endl;
            buffer = "";
        }
    }
}

void split_multi_pem3(istream &some_stream, SSL_CTX *ctx) {
    // WORKING SSL (GODDAMN)
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
                cout << "ERROR" << endl;

            X509_STORE* store = SSL_CTX_get_cert_store(ctx);
            int result = X509_STORE_add_cert(store, certificate);

            buffer = "";
        }
    }
}

void split_multi_pem2(istream &some_stream, Context::Ptr &context) {
    string line;
    string buffer;
    while(getline(some_stream, line)) {
        buffer.append(line);
        buffer.append("\n");
        if(line == "-----END CERTIFICATE-----") {
            
            stringstream bs;
            bs << buffer;

            X509Certificate cc(bs);
            context->addChainCertificate(cc);
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

    split_multi_pem3(ss, ctx);
    STACK* stack = SSL_CTX_get_client_CA_list(ctx);
    cout << "STACK:" << stack << endl;

/*
    std::ifstream t("rootcert.pem");
    std::string str((std::istreambuf_iterator<char>(t)),
                     std::istreambuf_iterator<char>());
    stringstream ss;
    ss << str;
    split_multi_pem2(ss, context);
*/
    try {

        cout << "creating session" << endl;
        HTTPSClientSession session("www.yahoo.com", 443, context);
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

    } catch(Exception &e) {
        cout << e.what() << endl;
        cout << e.displayText() << endl;
    }

}