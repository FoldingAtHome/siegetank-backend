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
    ifstream donor_tokens("donor_tokens.log");
    string donor_token;
    donor_tokens >> donor_token;
    ifstream target_ids("target_ids.log");
    string target_id;
    target_ids >> target_id;
    testStartStream();
    testStartStream(donor_token);
    testStartStream("", target_id);
    testStartStream(donor_token, target_id);
    return 0;
}