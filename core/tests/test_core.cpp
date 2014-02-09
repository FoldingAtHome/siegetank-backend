#include <Core.h>
#include <map>
#include <string>
#include <stdexcept>
#include <iostream>
#include <signal.h>
#include <ctime>

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

void test_sigint_signal() {
    Core core(150, "openmm", "6.0");
    if(core.exit() == true) {
        throw std::runtime_error("exit() returned true before signal");
    }
    raise(SIGINT);
    if(core.exit() == false) {
        throw std::runtime_error("exit() returned false before signal");
    }
}

void test_sigterm_signal() {
    Core core(150, "openmm", "6.0");
    if(core.exit() == true) {
        throw std::runtime_error("exit() returned true before signal");
    }
    raise(SIGTERM);
    if(core.exit() == false) {
        throw std::runtime_error("exit() returned false before signal");
    }
}

void test_should_send_checkpoint() {
    int checkpoint_increment = 6;
    Core core(checkpoint_increment, "openmm", "6.0");
    time_t current_time = time(0);
    if(core.should_send_checkpoint()) {
        throw std::runtime_error("1. should_checkpoint() returned true");
    }
    sleep(checkpoint_increment+2);
    if(!core.should_send_checkpoint()) {
        throw std::runtime_error("2. should_checkpoint() returned false");
    }
    if(core.should_send_checkpoint()) {
        throw std::runtime_error("3. should_checkpoint() returned true");
    }
    sleep(checkpoint_increment+2);
    if(!core.should_send_checkpoint()) {
        throw std::runtime_error("2. should_checkpoint() returned false");
    }
}

void test_should_heartbeat() {
    
}

void test_initialize_and_start() { 
    Core core(150, "openmm", "6.0");
    Poco::URI uri("https://127.0.0.1:8980/core/assign");

    map<string, string> target_files;
    map<string, string> stream_files;
    string stream_id;
    string target_id;

    core.start_stream(uri, stream_id, target_id, target_files, stream_files);

    if(target_files.find("system.xml") == target_files.end())
        throw std::runtime_error("system.xml not in target_files!");
    if(target_files.find("integrator.xml") == target_files.end())
        throw std::runtime_error("integrator.xml not in target_files!");
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
        core.send_frame_files(frame_files, 1);
    }

    core.send_heartbeat();

    for(int i=0; i < 10; i++) {
        string filename1("frames.xtc");
        string filedata1 = gen_random(100);
        string filename2("log.txt");
        string filedata2("derpderp.txt");
        map<string, string> frame_files;
        frame_files[filename1] = filedata1;
        frame_files[filename2] = filedata2;
        core.send_frame_files(frame_files, 1, true);
    }

    core.send_heartbeat();

    string c_filename("state.xml");
    map<string, string> checkpoint_files;
    checkpoint_files[c_filename] = test_state;
    core.send_checkpoint_files(checkpoint_files, true);

    for(int i=0; i < 10; i++) {
        string filename1("frames.xtc");
        string filedata1 = gen_random(100);
        string filename2("log.txt");
        string filedata2("derpderp.txt");
        map<string, string> frame_files;
        frame_files[filename1] = filedata1;
        frame_files[filename2] = filedata2;
        int count = (rand()%100)+1;
        core.send_frame_files(frame_files, count);
    }

    core.send_heartbeat();

    core.send_checkpoint_files(checkpoint_files, true);

    core.stop_stream();

}

int main() {
    test_sigint_signal();
    test_sigterm_signal();
    test_should_send_checkpoint();
    test_initialize_and_start();
    return 0;
}