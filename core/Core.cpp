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

Core::Core() {

    // initialize SSL
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

    cout << "creating cc session" << endl;
    _cc_session = new Poco::Net::HTTPSClientSession("127.0.0.1", 8980, context);

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
    ws_uri = object->get("uri").convert<std::string>();
    ws_token = object->get("token").convert<std::string>();
    _steps_per_frame = object->get("steps_per_frame").convert<int>();
    parser.reset();

    }

    Poco::URI wuri(ws_uri);
    _ws_session = new Poco::Net::HTTPSClientSession(wuri.getHost(), wuri.getPort(), context);

}