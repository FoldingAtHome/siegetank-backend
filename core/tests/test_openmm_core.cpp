#include <openmm/OpenMMCore.h>
#include <map>
#include <string>
#include <stdexcept>
#include <iostream>
#include <signal.h>

void test_openmm_core() {
    OpenMMCore core(25);
    core.initialize();
    for(int i=9995; i < 10010; i++) {
        core.check_step(i);
    }
}

int main() {
    test_openmm_core();
}