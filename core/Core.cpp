// OS X SSL instructions:
// ./Configure darwin64-x86_64-cc -noshared
// g++ -I ~/poco/include Core.cpp ~/poco/lib/libPoco* /usr/local/ssl/lib/lib*
// Linux:
// g++ -I/home/yutong/openmm_install/include -I/usr/local/ssl/include -I/home/yutong/poco152_install/include -L/home/yutong/poco152_install/lib -L/usr/local/ssl/lib/ Core.cpp -lpthread -lPocoNetSSL -lPocoCrypto -lssl -lcrypto -lPocoUtil -lPocoJSON -ldl -lPocoXML -lPocoNet -lPocoFoundation -L/home/yutong/openmm_install/lib -L/home/yutong/openmm_install/lib/plugins -lOpenMMOpenCL_static /usr/lib/nvidia-current/libOpenCL.so -lOpenMMCUDA_static /usr/lib/nvidia-current/libcuda.so /usr/local/cuda/lib64/libcufft.so -lOpenMMCPU_static -lOpenMMPME_static -L/home/yutong/fftw_install/lib/ -lfftw3f -lfftw3f_threads -lOpenMM_static; ./a.out 

// Linux:
// g++ -I/Users/yutongzhao/openmm_install/include -I/usr/local/ssl/include -I/users/yutongzhao/poco152_install/include -L/users/yutongzhao/poco152_install/lib -L/usr/local/ssl/lib/ Core.cpp -lpthread -lPocoNetSSL -lPocoCrypto -lssl -lcrypto -lPocoUtil -lPocoJSON -ldl -lPocoXML -lPocoNet -lPocoFoundation -L/Users/yutongzhao/openmm_install/lib -lOpenMMCPU_static -L/Users/yutongzhao/openmm_install/lib/plugins -lOpenMM_static; ./a.out 

//  ./configure --static --prefix=/home/yutong/poco152_install --omit=Data/MySQL,Data/ODBC

#include <iostream>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLException.h>
#include <Poco/Net/X509Certificate.h>
#include <Poco/UnicodeConverter.h>
#include <Poco/Util/Application.h>
#include <Poco/StreamCopier.h>
#include <Poco/Dynamic/Var.h>
#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/InflatingStream.h>

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/x509.h>

#include <fstream>
#include <string>
#include <streambuf>
#include <sstream>
#include <stdexcept>

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
extern "C" void registerCpuPlatform();

string decode_gz_b64(string encoded_string) {
    istringstream encoded_stream(encoded_string, std::ios_base::binary);
    Poco::Base64Decoder b64decoder(encoded_stream);
    std::string decoded_b64_string((std::istreambuf_iterator<char>(b64decoder)),
                     std::istreambuf_iterator<char>());
    istringstream gzip_stream(decoded_b64_string, std::ios_base::binary);
    Poco::InflatingInputStream inflater(gzip_stream, 
        Poco::InflatingStreamBuf::STREAM_GZIP);
    std::string data((std::istreambuf_iterator<char>(inflater)),
                     std::istreambuf_iterator<char>());
    return data;
}

int main() {

    try {
        string foo("H4sIAEnM6VIC//NIzcnJVwjPL8pJAQBWsRdKCwAAAA==");

        string result = decode_gz_b64(foo);

        if(result != "Hello World") {
            cout << "BAD B64 DECODE" << endl;
        } else {
            cout << "GZ B64 DECODE OK" << endl;
        }
        
        Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::CLIENT_USE, "", 
            Poco::Net::Context::VERIFY_NONE, 9, false);
        SSL_CTX *ctx = context->sslContext();
        std::ifstream t("rootcert.pem");
        std::string str((std::istreambuf_iterator<char>(t)),
                         std::istreambuf_iterator<char>());
        stringstream ss;
        ss << str;
        read_cert_into_ctx(ss, ctx);

        Poco::JSON::Parser parser;

        cout << "creating cc session" << endl;
        Poco::Net::HTTPSClientSession cc_session("127.0.0.1", 8980, context);
        
        string ws_uri;
        string ws_token;
        {
        cout << "fetching an assignment" << endl;
        Poco::Net::HTTPRequest request("POST", "/core/assign");
        string body("{\"engine\": \"openmm\", \"engine_version\": \"6.0\"}");
        request.setContentLength(body.length());
        cc_session.sendRequest(request) << body;
        cout << "obtaining response" << endl;
        Poco::Net::HTTPResponse response;
        istream &content_stream = cc_session.receiveResponse(response);
        if(response.getStatus() != 200) {
            throw std::runtime_error("Could not get an assignment from CC");
        }
        cout << response.getStatus() << endl;
        string content;
        Poco::StreamCopier::copyToString(content_stream, content);
        Poco::Dynamic::Var result = parser.parse(content);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();
        Poco::Dynamic::Var uri = object->get("uri");
        ws_uri = uri.convert<std::string>();
        Poco::Dynamic::Var token = object->get("token");
        ws_token = token.convert<std::string>();
        parser.reset();
        }
        Poco::URI wuri(ws_uri);
        Poco::Net::HTTPSClientSession ws_session(
            wuri.getHost(), wuri.getPort(), context);

        string stream_id;
        string target_id;
        string system_b64;
        string integrator_b64;
        string state_b64;

        {
        Poco::Net::HTTPRequest request("GET", wuri.getPath());
        request.set("Authorization", ws_token);
        ws_session.sendRequest(request);
        Poco::Net::HTTPResponse response;
        istream &content_stream = ws_session.receiveResponse(response);
        if(response.getStatus() != 200) {
            throw std::runtime_error("Could not get start a stream from WS");
        }
        string content;
        Poco::StreamCopier::copyToString(content_stream, content);
        Poco::Dynamic::Var result = parser.parse(content);
        Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();        
        stream_id = object->get("stream_id").convert<std::string>();
        target_id = object->get("target_id").convert<std::string>();
        // extract target files and stream files
        Poco::Dynamic::Var target_files = object->get("target_files");
        Poco::JSON::Object::Ptr object_file = target_files.extract<Poco::JSON::Object::Ptr>();
        system_b64 = object_file->get("system.xml.gz.b64").convert<std::string>();
        integrator_b64 = object_file->get("integrator.xml.gz.b64").convert<std::string>();
        Poco::Dynamic::Var stream_files = object->get("stream_files");
        object_file = stream_files.extract<Poco::JSON::Object::Ptr>();
        state_b64 = object_file->get("state.xml.gz.b64").convert<std::string>();
        parser.reset();
        }

        string system_xml_string = decode_gz_b64(system_b64);
        string integrator_xml_string = decode_gz_b64(integrator_b64);
        string state_xml_string = decode_gz_b64(state_b64);

        istringstream system_xml_stream(system_xml_string);
        istringstream integrator_xml_stream(integrator_xml_string);
        istringstream state_xml_stream(state_xml_string);

        registerSerializationProxies();
        registerCpuPlatform();

        OpenMM::System *sys = OpenMM::XmlSerializer::deserialize<OpenMM::System>(system_xml_stream);
        OpenMM::Integrator *integrator = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(integrator_xml_stream);
        OpenMM::State *state = OpenMM::XmlSerializer::deserialize<OpenMM::State>(state_xml_stream);

        OpenMM::Context* coreContext = new OpenMM::Context(*sys, *integrator, \
                               OpenMM::Platform::getPlatformByName("CPU"));
        coreContext->setState(*state);

        for(int i=0; i < 10000; i++) {
            if( i % 100 == 0) {
                cout << i << endl;
            }
            integrator->step(1);
        }

        // first pass the files through the b64 decoder, then inflate it.


        /*
        cout << "creating request" << endl;
        Poco::Net::HTTPRequest request("POST", "/managers");
        cout << "sending request" << endl;
        string body("{\"email\":\"proteneer@gmail.com\", \"password\": \"foo\"}");
        request.setContentLength(body.length());
        session.sendRequest(request) << body;
        cout << "obtaining response" << endl;
        Poco::Net::HTTPResponse response;
        session.receiveResponse(response);
        cout << response.getStatus() << endl;

        cout << "creating request" << endl;
        Poco::Net::HTTPRequest request("POST", "/auth");
        cout << "sending request" << endl;
        string body("{ \"email\": \"proteneer@gmail.com\", \"password\": \"foo\" }");
        request.setContentLength(body.length());
        session.sendRequest(request) << body;
        cout << "obtaining response" << endl;
        Poco::Net::HTTPResponse response;
        cout << session.receiveResponse(response).rdbuf() << endl;
        cout << response.getStatus() << endl;
        */


        return 0;

    } catch(Exception &e) {
        cout << e.what() << endl;
        cout << e.displayText() << endl;
        cout << e.message() << endl;
    }

}