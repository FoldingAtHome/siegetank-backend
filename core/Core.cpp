// OS X SSL instructions:
// ./Configure darwin64-x86_64-cc -noshared
// g++ -I ~/poco/include Core.cpp ~/poco/lib/libPoco* /usr/local/ssl/lib/lib*
// Linux:
// g++ -I/home/yutong/openmm_install/include -I/usr/local/ssl/include -I/home/yutong/poco_install/include -L/home/yutong/poco_install/lib -L/usr/local/ssl/lib/ Core.cpp -lpthread -lPocoNetSSL -lPocoCrypto -lssl -lcrypto -lPocoUtil -ldl -lPocoXML -lPocoNet -lPocoFoundation -L/home/yutong/openmm_install/lib -L/home/yutong/openmm_install/lib/plugins -lOpenMMOpenCL_static /usr/lib/nvidia-current/libOpenCL.so -lOpenMM_static

#include <iostream>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLException.h>
#include <Poco/Net/X509Certificate.h>
#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/x509.h>

#include <fstream>
#include <string>
#include <streambuf>
#include <sstream>

#include <OpenMM.h>

using namespace std;
using namespace Poco;

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
            certificate = PEM_read_bio_X509(bio, NULL, NULL, NULL);
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

extern "C" void registerSerializationProxies();
// extern "C" void registerOpenCLPlatform();
// extern "C" void registerCudaPlatform();
extern "C" void registerCpuPlatform();
extern "C" void registerCpuPmeKernelFactories();

int main() {
    registerSerializationProxies();
    //registerOpenCLPlatform();
    registerCpuPlatform();
    //registerCudaPlatform();
    registerCpuPmeKernelFactories();
    ifstream system_file("systems/DHFR_SYSTEM_EXPLICIT.xml");
    ifstream integrator_file("systems/DHFR_INTEGRATOR_EXPLICIT.xml");
    ifstream state_file("systems/DHFR_STATE_EXPLICIT.xml");
    OpenMM::System *sys = OpenMM::XmlSerializer::deserialize<OpenMM::System>\
        (system_file);
    OpenMM::Integrator *integrator = OpenMM::XmlSerializer::deserialize\
        <OpenMM::Integrator>(integrator_file);
    OpenMM::State *state = OpenMM::XmlSerializer::deserialize\
        <OpenMM::State>(state_file);

    cout << "Creating context..." << endl;
    OpenMM::Context* coreContext_ = new OpenMM::Context(*sys, *integrator, \
                               OpenMM::Platform::getPlatformByName("CPU"));
    cout << "Context created..." << endl;

    coreContext_->setState(*state);

    cout << "Stepping..." << endl;
    for(int i=0; i < 10000; i++) {
        if( i % 100 == 0) {
            cout << i << endl;
        }
        integrator->step(1);
    }

    try {
        Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::CLIENT_USE, "", 
            Poco::Net::Context::VERIFY_RELAXED, 9, false);
        SSL_CTX *ctx = context->sslContext();
        std::ifstream t("rootcert.pem");
        std::string str((std::istreambuf_iterator<char>(t)),
                         std::istreambuf_iterator<char>());
        stringstream ss;
        ss << str;
        read_cert_into_ctx(ss, ctx);

        cout << "creating session" << endl;
        Poco::Net::HTTPSClientSession session("axess.sahr.stanford.edu", 443, context);
        cout << "creating get" << endl;
        Poco::Net::HTTPRequest request("GET", "/");
        cout << "sending request" << endl;
        session.sendRequest(request);
        cout << "obtaining response" << endl;
        Poco::Net::HTTPResponse response;
        session.receiveResponse(response);
        cout << response.getStatus() << endl;
        return 0;

    } catch(Exception &e) {
        cout << e.what() << endl;
        cout << e.displayText() << endl;
        cout << e.message() << endl;
    }

}