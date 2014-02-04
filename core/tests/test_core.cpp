#include <Core.h>
#include <map>
#include <iostream>

using namespace std;

void test_initialize_and_start() { 
    Core core(25, 150);
    Poco::URI uri("https://127.0.0.1:8980/core/assign");
    core.initialize_session(uri);

    map<string, string> target_files;
    map<string, string> stream_files;
    string stream_id;
    string target_id;

    core.start_stream(stream_id, target_id, target_files, stream_files);

    cout << target_files["system.xml"] << endl;
    cout << target_files["integrator.xml"] << endl;
    cout << stream_files["state.xml"] << endl;
}

int main() {
    test_initialize_and_start();
    return 0;
}