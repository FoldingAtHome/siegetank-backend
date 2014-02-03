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

using namespace std;
using namespace Poco;

extern "C" void registerSerializationProxies();
extern "C" void registerCpuPlatform();
extern "C" void registerOpenCLPlatform();

void OpenMMCore::main() {

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
        int steps_per_frame;

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
        steps_per_frame = object->get("steps_per_frame").convert<int>();
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
        registerOpenCLPlatform();

        OpenMM::System *sys = OpenMM::XmlSerializer::deserialize<OpenMM::System>(system_xml_stream);
        OpenMM::Integrator *integrator = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(integrator_xml_stream);
        OpenMM::State *state = OpenMM::XmlSerializer::deserialize<OpenMM::State>(state_xml_stream);

        OpenMM::Context* coreContext = new OpenMM::Context(*sys, *integrator, \
                               OpenMM::Platform::getPlatformByName("OpenCL"));
        coreContext->setState(*state);

        cout << "steps_per_frame: " << steps_per_frame << endl;

        // Need to base 64 encode the binary frame before sending
        int steps = 0;
        float time = state->getTime();
        // Nx3 vector

        vector<OpenMM::Vec3> state_positions = state->getPositions();
        vector<vector<float> > xtc_positions(state_positions.size(), vector<float>(3));
        for(int i=0; i < state_positions.size(); i++) {
            for(int j=0; j < 3; j++) {
                xtc_positions[i][j] = state_positions[i][j];
            }
        }
        
        OpenMM::Vec3 a,b,c;
        
        state->getPeriodicBoxVectors(a,b,c);
        vector<vector<float> > xtc_box(3, vector<float>(3));

        xtc_box[0][0] = a[0];
        xtc_box[0][1] = a[1];
        xtc_box[0][2] = a[2];

        xtc_box[1][0] = b[0];
        xtc_box[1][1] = b[1];
        xtc_box[1][2] = b[2];

        xtc_box[2][0] = c[0];
        xtc_box[2][1] = c[1];
        xtc_box[2][2] = c[2];

        // Append state as a binary
        ostringstream frame_ostream(std::ios_base::binary);
        XTCWriter xtc_writer(frame_ostream);
        xtc_writer.append(steps, time, xtc_box, xtc_positions);
        string frame_binary = frame_ostream.str();

        // Base64 Encode
        ostringstream frame_b64_ostream(std::ios_base::binary);
        Poco::Base64Encoder b64encoder(frame_b64_ostream);
        b64encoder << frame_binary;
        b64encoder.close();

        string buffer_s = frame_b64_ostream.str();
        // important! remove any possible new lines in message
        buffer_s.erase(std::remove(buffer_s.begin(), buffer_s.end(), '\n'), buffer_s.end());
        buffer_s.erase(std::remove(buffer_s.begin(), buffer_s.end(), '\r'), buffer_s.end());                 

        cout << "Sending frame... " << endl;

        Poco::Net::HTTPRequest request("PUT", "/core/frame");
        request.set("Authorization", ws_token);
        string body("{ \"frame\": \"");
        body.append(buffer_s); 
        body.append("\"}");
        request.setContentLength(body.length());
        ws_session.sendRequest(request) << body;
        cout << "obtaining response" << endl;
        Poco::Net::HTTPResponse response;
        ws_session.receiveResponse(response);
        cout << response.getStatus() << endl;


        // for(int i=0; i < 100000; i++) {
        //     if( i % steps_per_frame == 0) {
        //         cout << i << endl;
        //     }
        //     integrator->step(1);
        // }

        // // first pass the files through the b64 decoder, then inflate it.

        
        // cout << "creating request" << endl;
        // Poco::Net::HTTPRequest request("POST", "/managers");
        // cout << "sending request" << endl;
        // string body("{\"email\":\"proteneer@gmail.com\", \"password\": \"foo\"}");
        // request.setContentLength(body.length());
        // session.sendRequest(request) << body;
        // cout << "obtaining response" << endl;
        // Poco::Net::HTTPResponse response;
        // session.receiveResponse(response);
        // cout << response.getStatus() << endl;

        // cout << "creating request" << endl;
        // Poco::Net::HTTPRequest request("POST", "/auth");
        // cout << "sending request" << endl;
        // string body("{ \"email\": \"proteneer@gmail.com\", \"password\": \"foo\" }");
        // request.setContentLength(body.length());
        // session.sendRequest(request) << body;
        // cout << "obtaining response" << endl;
        // Poco::Net::HTTPResponse response;
        // cout << session.receiveResponse(response).rdbuf() << endl;
        // cout << response.getStatus() << endl;
        

        return 0;

    } catch(Exception &e) {
        cout << "Exception caught!" << endl;
        cout << e.what() << endl;
        cout << e.displayText() << endl;
        cout << e.message() << endl;
    }

}