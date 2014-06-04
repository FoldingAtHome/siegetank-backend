#include <OpenMMCore.h>
#include <map>
#include <string>
#include <stdexcept>
#include <iostream>
#include <signal.h>
#include <fstream>

using namespace std;

void test_openmm_core() {
    ifstream core_keys("../../../../core_keys.log");
    string key; core_keys >> key;
    OpenMMCore core(key);
    core.startStream("127.0.0.1:8980", "", "");
}

int main() {
    OpenMMCore::registerComponents();
    test_openmm_core();
}