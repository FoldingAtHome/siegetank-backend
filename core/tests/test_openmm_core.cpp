#include <openmm/OpenMMCore.h>
#include <map>
#include <string>
#include <stdexcept>
#include <iostream>
#include <signal.h>

void test_main_loop() {
    OpenMMCore core(15,25);
    core.main();
}

int main() {
    test_main_loop();
}