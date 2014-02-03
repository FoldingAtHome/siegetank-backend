#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
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
#include <Poco/DeflatingStream.h>

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/x509.h>

#include <fstream>
#include <string>
#include <streambuf>
#include <sstream>
#include <stdexcept>
#include <algorithm>

#include <OpenMM.h>

#include "XTCWriter.h"
#include "Core.h"

using namespace std;

static void read_cert_into_ctx(istream &some_stream, SSL_CTX *ctx) {
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

static string decode_b64(const string &encoded_string) {
    istringstream encoded_stream(encoded_string, std::ios_base::binary);
    Poco::Base64Decoder b64decoder(encoded_stream);
    std::string decoded_b64_string((std::istreambuf_iterator<char>(b64decoder)), std::istreambuf_iterator<char>());
    return decoded_b64_string;
}

static string decode_gz(const string &gzipped_string) {
    istringstream gzip_stream(gzipped_string, std::ios_base::binary);
    Poco::InflatingInputStream inflater(gzip_stream, 
        Poco::InflatingStreamBuf::STREAM_GZIP);
    std::string data((std::istreambuf_iterator<char>(inflater)),
                     std::istreambuf_iterator<char>());
    return data; 
}

static string decode_gz_b64(const string &encoded_string) {
    return decode_gz(decode_b64(encoded_string));
}

Core::Core(int frame_send_interval, int checkpoint_send_interval) :
    _frame_send_interval(frame_send_interval),
    _checkpoint_send_interval(checkpoint_send_interval) {

}

Core::~Core() {
    delete _session;
}

void Core::initialize_session(Poco::URI &cc_uri) {
    Poco::Net::Context::Ptr context = new Poco::Net::Context(
        Poco::Net::Context::CLIENT_USE, "", 
        Poco::Net::Context::VERIFY_NONE, 9, false);
    SSL_CTX *ctx = context->sslContext();
    std::ifstream t("rootcert.pem");
    std::string str((std::istreambuf_iterator<char>(t)),
                     std::istreambuf_iterator<char>());
    stringstream ss;
    ss << str;
    read_cert_into_ctx(ss, ctx);

    cout << "connecting to cc..." << flush;
    Poco::Net::HTTPSClientSession cc_session(cc_uri.getHost(),
                                             cc_uri.getPort(),
                                             context);
    cout << "ok" << endl;

    string ws_uri;
    string ws_token;
    
    Poco::JSON::Parser parser;

    {
    cout << "getting assigned a ws..." << flush;
    Poco::Net::HTTPRequest request("POST", cc_uri.getPath());
    string body("{\"engine\": \"openmm\", \"engine_version\": \"6.0\"}");
    request.setContentLength(body.length());
    cc_session.sendRequest(request) << body;
    Poco::Net::HTTPResponse response;
    istream &content_stream = cc_session.receiveResponse(response);
    if(response.getStatus() != 200) {
        throw std::runtime_error("Could not get an assignment from CC");
    }
    cout << "ok" << endl;

    string content;
    Poco::StreamCopier::copyToString(content_stream, content);
    Poco::Dynamic::Var result = parser.parse(content);
    Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();
    ws_uri = object->get("uri").convert<std::string>();
    _auth_token = object->get("token").convert<std::string>();
    _frame_write_interval = object->get("steps_per_frame").convert<int>();
    parser.reset();
    }
    
    _ws_uri = Poco::URI(ws_uri);
    _session = new Poco::Net::HTTPSClientSession(_ws_uri.getHost(), 
        _ws_uri.getPort(), context);
}

void Core::start_stream(std::string &stream_id, std::string &target_id,
                        map<string, string> &target_files,
                        map<string, string> &stream_files) const {   
    Poco::Net::HTTPRequest request("GET", _ws_uri.getPath());
    request.set("Authorization", _auth_token);
    _session->sendRequest(request);
    Poco::Net::HTTPResponse response;
    istream &content_stream = _session->receiveResponse(response);
    if(response.getStatus() != 200) {
        cout << response.getStatus() << endl;
        cout << _ws_uri.getHost() << ":" << _ws_uri.getPort() << _ws_uri.getPath() << endl;
        throw std::runtime_error("Could not start a stream from WS");
    }
    string content;
    Poco::StreamCopier::copyToString(content_stream, content);
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(content);
    Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();        
    stream_id = object->get("stream_id").convert<std::string>();
    target_id = object->get("target_id").convert<std::string>();

    // extract target files
    {
    Poco::Dynamic::Var temp_obj = object->get("target_files");
    Poco::JSON::Object::Ptr object_ptr = temp_obj.extract<Poco::JSON::Object::Ptr>();
    for(Poco::JSON::Object::ConstIterator it=object_ptr->begin();
            it != object_ptr->end(); it++) {
        string filename = it->first;
        string filedata = it->second.convert<std::string>();
        if(filename.find(".b64") != string::npos) {
            filename = filename.substr(0, filename.length()-4);
            filedata = decode_b64(filedata);
            if(filename.find(".gz") != string::npos) {
                filename = filename.substr(0, filename.length()-3);
                filedata = decode_gz(filedata);
            }
        }
        target_files[filename] = filedata;
    }
    }

    // extract stream files
    {
    Poco::Dynamic::Var temp_obj = object->get("stream_files");
    Poco::JSON::Object::Ptr object_ptr = temp_obj.extract<Poco::JSON::Object::Ptr>();
    for(Poco::JSON::Object::ConstIterator it=object_ptr->begin();
            it != object_ptr->end(); it++) {
        string filename = it->first;
        string filedata = it->second.convert<std::string>();
        if(filename.find(".b64") != string::npos) {
            filename = filename.substr(0, filename.length()-4);
            filedata = decode_b64(filedata);
            if(filename.find(".gz") != string::npos) {       
                filename = filename.substr(0, filename.length()-3);
                filedata = decode_gz(filedata);
            }
        }
        stream_files[filename] = filedata;
    }
    }
}

void Core::main() {

}