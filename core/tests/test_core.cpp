#include <map>
#include <string>
#include <stdexcept>
#include <iostream>
#include <signal.h>
#include <ctime>
#include <sstream>
#include <fstream>

#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/StreamCopier.h>

#define private public
#define protected public

#include <Core.h>

using namespace std;

string gen_random(const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    string s(len, '0');
    for (int i = 0; i < s.length(); ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return s;
}

/*

void test_should_send_checkpoint() {
    int checkpoint_increment = 6;
    Core core(checkpoint_increment, "openmm", "6.0");
    time_t current_time = time(0);
    if(core.shouldSendCheckpoint()) {
        throw std::runtime_error("1. should_checkpoint() returned true");
    }
    sleep(checkpoint_increment+2);
    if(!core.shouldSendCheckpoint()) {
        throw std::runtime_error("2. should_checkpoint() returned false");
    }
    if(core.shouldSendCheckpoint()) {
        throw std::runtime_error("3. should_checkpoint() returned true");
    }
    sleep(checkpoint_increment+2);
    if(!core.shouldSendCheckpoint()) {
        throw std::runtime_error("2. should_checkpoint() returned false");
    }
}

void test_set_target_id() {
    ifstream targets_file("target_ids.log");
    string custom_target_id;
    targets_file >> custom_target_id;
    Core core(150, "openmm", "6.0");
    Poco::URI uri("https://127.0.0.1:8980/core/assign");
    core.target_id = custom_target_id;
    map<string, string> stream_files;
    cout << "starting stream" << endl;
    core.startStream(uri, stream_files);
}

void test_donor_token() {
    Core core(150, "openmm", "6.0");
    Poco::URI uri("https://127.0.0.1:8980");
    Poco::Net::Context::Ptr context = new Poco::Net::Context(
    Poco::Net::Context::CLIENT_USE, "", 
    Poco::Net::Context::VERIFY_NONE, 9, false);
    Poco::Net::HTTPSClientSession cc_session(uri.getHost(),
                                             uri.getPort(),
                                             context);
    Poco::Net::HTTPRequest request("POST", "/donors/auth");
    string body;
    body += "{\"username\": \"test_donor\", \"password\": \"test_donor_pass\"}";
    request.setContentLength(body.length());
    cc_session.sendRequest(request) << body;

    Poco::Net::HTTPResponse response;
    istream &content_stream = cc_session.receiveResponse(response);
    if(response.getStatus() != 200) {
        throw std::runtime_error("Bad authorizaton token!");
    }
    string content;
    Poco::StreamCopier::copyToString(content_stream, content);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(content);
    Poco::JSON::Object::Ptr object = result.extract<Poco::JSON::Object::Ptr>();
    string token;
    token = object->get("token").convert<std::string>();

    core.donor_token = token;
    map<string, string> stream_files;

    Poco::URI uri2("https://127.0.0.1:8980/core/assign");
    core.startStream(uri2, stream_files);
}

*/

void testStartStream(string donor_token="", string target_id="") { 
    
    ifstream core_keys("core_keys.log");
    string key;
    core_keys >> key;

    Core core("openmm", key);
    string uri("127.0.0.1:8980");
    core.startStream(uri, donor_token, target_id);

    map<string, string> &stream_files = core.files_;
    if(stream_files.find("system.xml") == stream_files.end())
        throw std::runtime_error("system.xml not in stream_files!");
    if(stream_files.find("integrator.xml") == stream_files.end())
        throw std::runtime_error("integrator.xml not in stream_files!");
    if(stream_files.find("state.xml") == stream_files.end())
        throw std::runtime_error("state.xml not in stream_files!");
    // its important we actually use a valid state.xml here to not break the
    // other openmm tests
    string test_state = stream_files["state.xml"];
    for(int i=0; i < 10; i++) {
        string filename1("frames.xtc");
        string filedata1 = gen_random(100);
        string filename2("log.txt");
        string filedata2("derpderp.txt");
        map<string, string> frame_files;
        frame_files[filename1] = filedata1;
        frame_files[filename2] = filedata2;
        core.sendFrame(frame_files);
    }
    core.sendHeartbeat();
    for(int i=0; i < 10; i++) {
        string filename1("frames.xtc");
        string filedata1 = gen_random(100);
        string filename2("log.txt");
        string filedata2("derpderp.txt");
        map<string, string> frame_files;
        frame_files[filename1] = filedata1;
        frame_files[filename2] = filedata2;
        core.sendFrame(frame_files, 1);
    }
    core.sendHeartbeat();
    for(int i=0; i < 10; i++) {
        string filename1("frames.xtc");
        string filedata1 = gen_random(100);
        string filename2("log.txt");
        string filedata2("derpderp.txt");
        map<string, string> frame_files;
        frame_files[filename1] = filedata1;
        frame_files[filename2] = filedata2;
        core.sendFrame(frame_files, 1, true);
    }
    core.sendHeartbeat();
    string c_filename("state.xml");
    map<string, string> checkpoint_files;
    checkpoint_files[c_filename] = test_state;
    core.sendCheckpoint(checkpoint_files, true);
    for(int i=0; i < 10; i++) {
        string filename1("frames.xtc");
        string filedata1 = gen_random(100);
        string filename2("log.txt");
        string filedata2("derpderp.txt");
        map<string, string> frame_files;
        frame_files[filename1] = filedata1;
        frame_files[filename2] = filedata2;
        int count = (rand()%100)+1;
        core.sendFrame(frame_files, count);
    }
    core.sendHeartbeat();
    core.sendCheckpoint(checkpoint_files, true);
    core.stopStream();
}

int main() {
    testStartStream();
    /*
    test_set_target_id();
    test_sigint_signal();
    test_sigterm_signal();
    test_donor_token();
    test_should_send_checkpoint();
    test_initialize_and_start();
    */
    return 0;
}