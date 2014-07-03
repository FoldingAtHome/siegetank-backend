// Authors: Yutong Zhao <proteneer@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

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
#include <Poco/URI.h>
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
#include <cstdlib>
#include <cstdio>

#include <ctime>

#include "Core.h"
#include "md5.h"

using namespace std;

static void read_cert_into_ctx(istream &some_stream, SSL_CTX *ctx) {
    // Add a stream of PEM formatted certificate strings to the trusted store
    // of the ctx.
    string line;
    string buffer;
	int count = 0;
    while(getline(some_stream, line)) {
		buffer.append(line);
        buffer.append("\n");
        if(line.find("END CERTIFICATE") != string::npos ) {
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

static int getPort(const std::string &s, char delim=':') {
    std::vector<std::string> elems;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    if(elems.size() > 1) {
        int port;
        stringstream pss(elems[1]);
        pss >> port;
        return port;
    } else {
        return 443;
    }
}

static std::string getHost(const std::string &s, char delim=':') {
    std::vector<std::string> elems;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems[0];
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

static string compute_md5(const string &binary_string) {
    md5_state_s state;
    md5_init(&state);
    md5_append(&state, reinterpret_cast<const unsigned char *>(&binary_string[0]), binary_string.size());
    unsigned char digest[16] = "";
    md5_finish(&state, digest);
    char converted[16*2+1];
    converted[33] = '\0'; 
    for(int i=0; i < 16; i++) {
        sprintf(&converted[i*2], "%02x", digest[i]);
    }
    return converted;
}

static vector<string> delimit(const string& input, char token) {
    vector<string> strings;
    istringstream f(input);
    string s;
    while (getline(f, s, token)) {
        strings.push_back(s);
    }
    return strings;
}

static void parse_host_and_port(const string& host_and_port, string &host, int &port) {
    vector<string> host_info = delimit(host_and_port, ':');
    if(host_info.size() == 1) {
        throw runtime_error("Proxy port not specified");
    }
    host = host_info[0];
    stringstream ss(host_info[1]);
    ss >> port;
}

static void parse_proxy_string(const string &input_string, string &username, string &password, string &host, int &port) {
    vector<string> result(delimit(input_string, '@'));
    if(result.size() == 2) {
        vector<string> user_info = delimit(result[0], ':');
        username = user_info[0];
        password = user_info[1];
        parse_host_and_port(result[1], host, port);
    } else if(result.size() == 1) {
        username.clear();
        password.clear();
        parse_host_and_port(result[0], host, port);
    } else {
        throw runtime_error("Bad proxy string");
    }
}

Core::Core(std::string core_key, std::ostream& log) :
    core_key_(core_key),
    logStream(log),
    session_(NULL) {
}

Core::~Core() {
    delete session_;
}

// see if host is a domain name or an ip address by checking the last char
static bool is_domain(const string &host) {
    char c = *host.rbegin();
    if(isdigit(c))
        return false;
    else
        return true;
}

void Core::assign(const string &cc_uri,
                  const string &donor_token,
                  const string &target_id,
                  const string &proxy_string) {
    logStream << "preparing for assignment..." << endl;
    Poco::Net::Context::VerificationMode verify_mode;
    if(is_domain(getHost(cc_uri))) {
        verify_mode = Poco::Net::Context::VERIFY_RELAXED;
    } else {
        verify_mode = Poco::Net::Context::VERIFY_NONE;
    }
    Poco::Net::Context::Ptr context = new Poco::Net::Context(
        Poco::Net::Context::CLIENT_USE, "", 
        verify_mode, 9, true);
	
	SSL_CTX *ctx = context->sslContext();

    // hacky as hell way to load certs:)
	{
	    #include "certs/certs_bundle_0.pem"
		stringstream ss;
		ss << ssl_string_0;
		read_cert_into_ctx(ss, ctx);
	}

	{
		#include "certs/certs_bundle_1.pem"
		stringstream ss;
		ss << ssl_string_1;
		read_cert_into_ctx(ss, ctx);
	}

	{
		#include "certs/certs_bundle_2.pem"
		stringstream ss;
		ss << ssl_string_2;
		read_cert_into_ctx(ss, ctx);
	}

	{
		#include "certs/certs_bundle_3.pem"
		stringstream ss;
		ss << ssl_string_3;
		read_cert_into_ctx(ss, ctx);
	}

	{
		#include "certs/certs_bundle_4.pem"
		stringstream ss;
		ss << ssl_string_4;
		read_cert_into_ctx(ss, ctx);
	}

	{
		#include "certs/certs_bundle_5.pem"
		stringstream ss;
		ss << ssl_string_5;
		read_cert_into_ctx(ss, ctx);
	}

	{
		#include "certs/certs_bundle_6.pem"
		;
		stringstream ss;
		ss << ssl_string_6;
		read_cert_into_ctx(ss, ctx);
	}

    logStream << "connecting to cc " << getHost(cc_uri) << "... " << endl;

    Poco::Net::HTTPSClientSession cc_session(getHost(cc_uri),
                                             getPort(cc_uri),
                                             context);

    if(proxy_string.size() > 0) {
        cout << "setting up proxy credentials... " << endl;
        string proxy_user, proxy_pass, proxy_host;
        int proxy_port;
        parse_proxy_string(proxy_string, proxy_user, proxy_pass, proxy_host, proxy_port);
        cc_session.setProxy(proxy_host, proxy_port);
        logStream << "setting proxy_host, proxy_port " << proxy_host << " " << proxy_port << endl;
        if(proxy_user.size() > 0 && proxy_pass.size() > 0) {
            cc_session.setProxyCredentials(proxy_user, proxy_pass);
            logStream << "setting proxy_user, proxy_pass " << proxy_user << " " << proxy_pass << endl;
        }
    }

	try {
        logStream << "assigning core to a stream..." << flush;
        Poco::Net::HTTPRequest request("POST", "/core/assign");
    	picojson::object obj;
        if(donor_token.length() > 0)
            obj["donor_token"] = picojson::value(donor_token);
        if(target_id.length() > 0)
            obj["target_id"] = picojson::value(target_id);
        string body = picojson::value(obj).serialize();
        request.set("Authorization", core_key_);
        request.setContentLength(body.length());
        cc_session.sendRequest(request) << body;
        Poco::Net::HTTPResponse response;
        istream &content_stream = cc_session.receiveResponse(response);

        if(response.getStatus() == 200) {
            logStream << "ok" << endl;
        } else if(response.getStatus() == 401) {
            logStream << "core is outdated" << endl; 
    #ifdef FAH_CORE
            exit(0x110);
    #else
            exit(1);
    #endif
        } else if(response.getStatus() == 400) {
            logStream << response.getStatus() << endl;
            logStream << content_stream.rdbuf() << endl;
            throw std::runtime_error("Bad Assignment Request");
        } else {
            logStream << response.getStatus() << endl;
            // In case of a bad connection (eg. 500), it is probably not safe to cout the content stream here.
            //logStream << content_stream.rdbuf() << endl;
            throw std::runtime_error("FATAL Assignment");
        }
        picojson::value json_value;
        content_stream >> json_value;
        string err = picojson::get_last_error();
        if(!err.empty())
            throw(std::runtime_error("assign() picojson error"+err));
        if(!json_value.is<picojson::object>())
            throw(std::runtime_error("no JSON object could be read"+err));
        picojson::value::object &json_object = json_value.get<picojson::object>();
        string ws_url(json_object["url"].get<string>());
        Poco::URI poco_url(ws_url);
        core_token_ = json_object["token"].get<string>();
        logStream << "connecting to scv " << poco_url.getHost() << "... " << endl;
        session_ = new Poco::Net::HTTPSClientSession(poco_url.getHost(), poco_url.getPort(), context);
        if(proxy_string.size() > 0) {
            cout << "setting up proxy credentials... " << endl;
            string proxy_user, proxy_pass, proxy_host;
            int proxy_port;
            parse_proxy_string(proxy_string, proxy_user, proxy_pass, proxy_host, proxy_port);
            session_->setProxy(proxy_host, proxy_port);
            logStream << "setting proxy_host, proxy_port " << proxy_host << " " << proxy_port << endl;
            if(proxy_user.size() > 0 && proxy_pass.size() > 0) {
                session_->setProxyCredentials(proxy_user, proxy_pass);
                logStream << "setting proxy_user, proxy_pass " << proxy_user << " " << proxy_pass << endl;
            }
        }
	} catch(Poco::Net::SSLException &se) {
		cout << se.message() << endl;
		cout << se.displayText() << endl;
		throw;
	}
}

void Core::startStream(const string &cc_uri,
                       const string &donor_token,
                       const string &target_id,
                       const string &proxy_string) {
    if(session_ != NULL) {
        delete session_;
        session_ = NULL;
    }
    assign(cc_uri, donor_token, target_id, proxy_string);
    logStream << "preparing to start stream..." << endl;
    Poco::Net::HTTPRequest request("GET", "/core/start");
    request.set("Authorization", core_token_);
    session_->sendRequest(request);
    Poco::Net::HTTPResponse response;
    logStream << "receiving response..." << endl;
    istream &content_stream = session_->receiveResponse(response);
    if(response.getStatus() != 200)
        throw std::runtime_error("Could not start a stream from SCV");
    std::string data((std::istreambuf_iterator<char>(content_stream)),
                 (std::istreambuf_iterator<char>()));
    if(response.has("Content-MD5")) {
        // compute md5sum
        cout << "verifying hash..." << endl;
        string expected(response.get("Content-MD5"));
        if(compute_md5(data) != expected) {
            cout << compute_md5(data) << endl;
            cout << expected << endl;
            throw std::runtime_error("MD5 mismatch");
        }
    }
    picojson::value json_value;
    std::istringstream input(data);
    input >> json_value;
    string err = picojson::get_last_error();
    if(!err.empty())
        throw(std::runtime_error("assign() picojson error"+err));
    if(!json_value.is<picojson::object>())
        throw(std::runtime_error("no JSON object could be read"+err));
    picojson::value::object &json_object = json_value.get<picojson::object>();
    stream_id_ = json_object["stream_id"].get<string>();
    target_id_ = json_object["target_id"].get<string>();

    logStream << "stream id: " << stream_id_.substr(0, 8) << endl;
    logStream << "target id: " << target_id_.substr(0, 8) << endl;

    if(target_id.size() > 0 && target_id != target_id_) {
        throw std::runtime_error("FATAL: Specified target_id mismatch");
    }
    picojson::value::object &json_files = json_object["files"].get<picojson::object>();
    for(picojson::value::object::const_iterator it = json_files.begin();
         it != json_files.end(); ++it) {
        string filename = it->first;
        string filedata = it->second.get<string>();
        if(filename.find(".b64") != string::npos) {
            filename = filename.substr(0, filename.length()-4);
            filedata = decode_b64(filedata);
            if(filename.find(".gz") != string::npos) {       
                filename = filename.substr(0, filename.length()-3);
                filedata = decode_gz(filedata);
            }
        }
        files_[filename] = filedata;
    }
    options_ = json_object["options"].serialize();
    cout << "json decode complete" << endl;
}

void Core::sendFrame(const map<string, string> &files, 
    int frame_count, bool gzip) const {
    logStream << "sending frame" << std::endl;
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
    request.set("Content-MD5", compute_md5(message));
    request.set("Authorization", core_token_);
    request.setContentLength(message.length());
    session_->sendRequest(request) << message;
    Poco::Net::HTTPResponse response;
    session_->receiveResponse(response);
    if(response.getStatus() != 200) {
        throw std::runtime_error("Core::sendFrame bad status code");
    }
}

void Core::sendCheckpoint(const map<string, string> &files, double frames, bool gzip) const {
    logStream << "sending checkpoint" << std::endl;
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
    message += "},";
    message += "\"frames\":";
    stringstream frames_string;
    frames_string << frames;
    message += frames_string.str();
    message += "}";
    request.set("Content-MD5", compute_md5(message));
    request.set("Authorization", core_token_);
    request.setContentLength(message.length());
    session_->sendRequest(request) << message;
    Poco::Net::HTTPResponse response;
    session_->receiveResponse(response);
    if(response.getStatus() != 200) {
        throw std::runtime_error("Core::sendCheckpointFiles bad status code");
    }
}

void Core::stopStream(string err_msg) {
    Poco::Net::HTTPRequest request("PUT", "/core/stop");
    string message;
    message += "{";
    if(err_msg.length() > 0) {
        logStream << "stopping stream with error: " << err_msg << endl;
        string b64_error(encode_b64(err_msg));
        message += "\"error\": \"" + b64_error + "\"";
    }
    message += "}";
    request.set("Authorization", core_token_);
    request.setContentLength(message.length());
    session_->sendRequest(request) << message;
    Poco::Net::HTTPResponse response;
    session_->receiveResponse(response);
    if(response.getStatus() != 200) {
        throw std::runtime_error("Core::stopStream bad status code");
    }
}

void Core::sendHeartbeat() const {
    Poco::Net::HTTPRequest request("POST", "/core/heartbeat");
    string message("{}");
    request.set("Authorization", core_token_);
    request.setContentLength(message.length());
    session_->sendRequest(request) << message;
    Poco::Net::HTTPResponse response;
    session_->receiveResponse(response);
    if(response.getStatus() != 200) {
        throw std::runtime_error("Core::sendHeartbeat bad status code");
    }
}

void Core::main() {

}
