#include "OpenMMCore.h"

int main() {
    OpenMMCore core(25);
    core.initialize("https://127.0.0.1:8980/core/assign");
    core.main();
}