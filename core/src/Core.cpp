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
#include <locale>

#include <signal.h>
#include "Core.h"
#include <ctime>

using namespace std;

static sig_atomic_t _global_exit = false;

static void exit_signal_handler(int param) {
    _global_exit = true;
}

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

static string encode_b64(const string &binary) {
    ostringstream binary_ostream(std::ios_base::binary);
    Poco::Base64Encoder b64encoder(binary_ostream);
    b64encoder << binary;
    b64encoder.close();
    string b64_string(binary_ostream.str());
    // POCO B64 adds "\r\n" when the line buffer is full - we can either remove
    // it, or manually escape. We choose the former. 
    b64_string.erase(std::remove(b64_string.begin(), b64_string.end(), '\n'), b64_string.end());
    b64_string.erase(std::remove(b64_string.begin(), b64_string.end(), '\r'), b64_string.end());
    return b64_string;
}

static string encode_gz(const string &binary) {
    ostringstream binary_ostream(std::ios_base::binary);
    Poco::DeflatingOutputStream deflater(binary_ostream,
        Poco::DeflatingStreamBuf::STREAM_GZIP);
    deflater << binary;
    deflater.close();
    return binary_ostream.str();
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

Core::Core(int checkpoint_send_interval,
           string engine,
           string engine_version) :
    _frame_send_interval(0),
    _frame_write_interval(0),
    _logstream(_logstring),
    _checkpoint_send_interval(checkpoint_send_interval),
    _start_time(time(NULL)),
    _heartbeat_interval(15),
    _session(NULL),
    _engine(engine),
    _engine_version(engine_version) {

    _global_exit = false;
    signal(SIGINT, exit_signal_handler);
    signal(SIGTERM, exit_signal_handler);
    _next_checkpoint_time = _start_time + _checkpoint_send_interval;
    _next_heartbeat_time = _start_time + _heartbeat_interval;
}

Core::~Core() {
    delete _session;
}

bool Core::exit() const {
    return _global_exit;
}

// see if host is a domain name or an ip address by checking the last char
static bool is_domain(string host) {
    char c = *host.rbegin();
    if(isdigit(c))
        return false;
    else
        return true;
}

static string parse_error(const string& body) {
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(body);
    Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();
    string error(object->get("error").convert<std::string>());
    parser.reset();
    return error;
}

void Core::_initialize_session(const Poco::URI &cc_uri) {
    Poco::Net::Context::VerificationMode verify_mode;
    if(is_domain(cc_uri.getHost())) {
        cout << "USING SSL:" << " " << cc_uri.getHost() << endl;
        verify_mode = Poco::Net::Context::VERIFY_RELAXED;
    } else {
        verify_mode = Poco::Net::Context::VERIFY_NONE;
    }

    Poco::Net::Context::Ptr context = new Poco::Net::Context(
        Poco::Net::Context::CLIENT_USE, "", 
        verify_mode, 9, false);
    SSL_CTX *ctx = context->sslContext();
    std::string ssl_string;

    // hacky as hell :)
    #include <root_certs.h>

    stringstream ss;
    ss << ssl_string;
    read_cert_into_ctx(ss, ctx);

    cout << "connecting to cc..." << flush;
    Poco::Net::HTTPSClientSession cc_session(cc_uri.getHost(),
                                             cc_uri.getPort(),
                                             context);
    string ws_uri;
    string ws_token;
    
    Poco::JSON::Parser parser;
    try {
        cout << "being assigned a ws..." << flush;
        Poco::Net::HTTPRequest request("POST", cc_uri.getPath());
        string body;
        body += "{\"engine\": \""+_engine+"\",";
        body += "\"engine_version\": \""+_engine_version+"\",";

        if(donor_token.length() > 0) {
            body += "\"donor_token\": \""+donor_token+"\",";
        }

        if(target_id.length() > 0) {
            body += "\"target_id\": \""+target_id+"\",";
        }

        stringstream core_version;
        core_version << CORE_VERSION;
        body += "\"core_version\": \""+core_version.str()+"\"}";
        request.setContentLength(body.length());
        cc_session.sendRequest(request) << body;
        Poco::Net::HTTPResponse response;
        istream &content_stream = cc_session.receiveResponse(response);

        string content;
        Poco::StreamCopier::copyToString(content_stream, content);

        if(response.getStatus() != 200) {
            string reason = parse_error(content);
            stringstream error;
            error << "Could not get an assignment from CC, reason: ";
            error << reason << endl;
            throw std::runtime_error(error.str());
        }
        cout << "ok" << flush;

        {
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

    } catch(Poco::Net::SSLException &e) {
        cout << e.displayText() << endl;
        throw;
    }
}

void Core::startStream(const Poco::URI &cc_uri,
                        map<string, string> &target_files,
                        map<string, string> &stream_files) {
    if(_session == NULL)
        _initialize_session(cc_uri);
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
    _stream_id = object->get("stream_id").convert<std::string>();
    string temptarget_id = object->get("target_id").convert<std::string>();

    // if user specified a target_id then we attempt to fetch the specified id
    if(target_id.length() > 0 && temptarget_id != target_id) {
        throw std::runtime_error("FATAL: Specified target_id mismatch");
    }
    target_id = temptarget_id;

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

void Core::sendFrameFiles(const map<string, string> &files, 
    int frame_count, bool gzip) const {

    Poco::Net::HTTPRequest request("PUT", "/core/frame");
    stringstream frame_count_str;
    frame_count_str << frame_count;
    string message;
    message += "{";
    message += "\"frames\":"+frame_count_str.str()+",";
    message += "\"files\":{";
    for(map<string, string>::const_iterator it=files.begin();
        it != files.end(); it++) {
        string filename = it->first;
        string filedata = it->second;
        if(gzip) {
            filedata = encode_gz(filedata);
            filename += ".gz";
        }
        filedata = encode_b64(filedata);
        if(it != files.begin())
            message += ",";
        message += "\""+filename+".b64\"";
        message += ":";
        message += "\""+filedata+"\"";
    }
    message += "}}";
    request.set("Authorization", _auth_token);
    request.setContentLength(message.length());
    _session->sendRequest(request) << message;
    Poco::Net::HTTPResponse response;
    _session->receiveResponse(response);
    if(response.getStatus() != 200) {
        throw std::runtime_error("Core::sendFrameFiles bad status code");
    }
}

void Core::sendCheckpointFiles(const map<string, string> &files, 
    bool gzip) const {

    Poco::Net::HTTPRequest request("PUT", "/core/checkpoint");
    string message;
    message += "{\"files\":{";
    for(map<string, string>::const_iterator it=files.begin();
        it != files.end(); it++) {
        string filename = it->first;
        string filedata = it->second;
        if(gzip) {
            filedata = encode_gz(filedata);
            filename += ".gz";
        }
        filedata = encode_b64(filedata);
        if(it != files.begin())
            message += ",";
        message += "\""+filename+".b64\"";
        message += ":";
        message += "\""+filedata+"\"";
    }
    message += "}}";
    request.set("Authorization", _auth_token);
    request.setContentLength(message.length());
    _session->sendRequest(request) << message;
    Poco::Net::HTTPResponse response;
    _session->receiveResponse(response);
    if(response.getStatus() != 200) {
        throw std::runtime_error("Core::sendCheckpointFiles bad status code");
    }
}

void Core::stopStream(string err_msg) {
    Poco::Net::HTTPRequest request("PUT", "/core/stop");
    string message;
    message += "{";
    if(err_msg.length() > 0) {
        cout << "stopping stream with error: " << err_msg << endl;
        string b64_error(encode_b64(err_msg));
        message += "\"error\": \"" + b64_error + "\"";
    }
    message += "}";
    request.set("Authorization", _auth_token);
    request.setContentLength(message.length());
    _session->sendRequest(request) << message;
    Poco::Net::HTTPResponse response;
    _session->receiveResponse(response);
    if(response.getStatus() != 200) {
        throw std::runtime_error("Core::stopStream bad status code");
    }
    delete _session;
    _session = NULL;
}

void Core::sendHeartbeat() const {
    Poco::Net::HTTPRequest request("POST", "/core/heartbeat");
    string message("{}");
    request.set("Authorization", _auth_token);
    request.setContentLength(message.length());
    _session->sendRequest(request) << message;
    Poco::Net::HTTPResponse response;
    _session->receiveResponse(response);
    if(response.getStatus() != 200) {
        throw std::runtime_error("Core::sendHeartbeat bad status code");
    }
}

void Core::main() {

}

bool Core::shouldSendCheckpoint() {
    time_t current_time = time(NULL);
    if(current_time > _next_checkpoint_time) {
        _next_checkpoint_time = current_time + _checkpoint_send_interval;
        return true;
    } else {
        return false;
    }
}

bool Core::shouldHeartbeat() {
    time_t current_time = time(NULL);
    if(current_time > _next_heartbeat_time) {
        _next_heartbeat_time = current_time + _heartbeat_interval;
        return true;
    } else {
        return false;
    }
}
