#include <OpenMMCore.h>
#include <map>
#include <string>
#include <stdexcept>
#include <iostream>
#include <signal.h>

void test_openmm_core() {
    OpenMMCore core(25);
    core.initialize("https://127.0.0.1:8980/core/assign");
    for(int i=9995; i < 10010; i++) {
        core.checkFrameWrite(i);
    }
}

void test_sigint_signal() {
    Core core(150, "openmm", "6.0");
    if(core.exit() == true) {
        throw std::runtime_error("exit() returned true before signal");
    }
    raise(SIGINT);
    if(core.exit() == false) {
        throw std::runtime_error("exit() returned false after signal");
    }
}

void test_sigterm_signal() {
    Core core(150, "openmm", "6.0");
    if(core.exit() == true) {
        throw std::runtime_error("exit() returned true before signal");
    }
    raise(SIGTERM);
    if(core.exit() == false) {
        throw std::runtime_error("exit() returned false after signal");
    }
}

int main() {
    test_openmm_core();
}