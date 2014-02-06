#include <Core.h>
#include <map>
#include <string>
#include <stdexcept>
#include <iostream>
#include <signal.h>

using namespace std;

void test_sigint_signal() {
    Core core(25, 150);
    if(core.exit() == true) {
        throw std::runtime_error("exit() returned true before signal");
    }
    raise(SIGINT);
    if(core.exit() == false) {
        throw std::runtime_error("exit() returned false before signal");
    }
}

void test_sigterm_signal() {
    Core core(25, 150);
    if(core.exit() == true) {
        throw std::runtime_error("exit() returned true before signal");
    }
    raise(SIGTERM);
    if(core.exit() == false) {
        throw std::runtime_error("exit() returned false before signal");
    }
}

void test_initialize_and_start() { 
    Core core(25, 150);
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

    for(int i=0; i < 10; i++) {
        string filename1("frames.xtc");
        string filedata1("8gdjrp24u6pjasdfpoi2345");
        string filename2("log.txt");
        string filedata2("derpderp.txt");
        map<string, string> frame_files;
        frame_files[filename1] = filedata1;
        frame_files[filename2] = filedata2;
        core.send_frame_files(frame_files);
    }

    for(int i=0; i < 10; i++) {
        string filename1("frames.xtc");
        string filedata1("8gdjrp24u6pjasdfpoi2345");
        string filename2("log.txt");
        string filedata2("derpderp.txt");
        map<string, string> frame_files;
        frame_files[filename1] = filedata1;
        frame_files[filename2] = filedata2;
        core.send_frame_files(frame_files, true);
    }

    string c_filename("state.xml");
    string c_filedata("<XML>");
    map<string, string> checkpoint_files;
    checkpoint_files[c_filename] = c_filedata;
//    core.send_checkpoint_files(checkpoint_files);
    core.send_checkpoint_files(checkpoint_files, true);

    core.stop_stream();

}

int main() {
    test_sigint_signal();
    test_sigterm_signal();
    test_initialize_and_start();
    return 0;
}