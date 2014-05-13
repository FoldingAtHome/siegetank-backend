#include <signal.h>
#include "ExitSignal.h"

static sig_atomic_t global_exit = false;

static void exit_signal_handler(int param) {
    global_exit = true;
}

void ExitSignal::init() {
    signal(SIGINT, exit_signal_handler);
    signal(SIGTERM, exit_signal_handler);
}

bool ExitSignal::shouldExit() {
	return global_exit;
}