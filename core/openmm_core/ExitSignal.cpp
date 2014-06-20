#include "ExitSignal.h"
#include <errno.h>

static sig_atomic_t global_exit = false;


#ifdef FAH_CORE
static pid_t global_lifeline_pid = -1;
void ExitSignal::setLifeline(pid_t pid) {
	global_lifeline_pid = pid;
}
#endif


static void exit_signal_handler(int param) {
    global_exit = true;
}

void ExitSignal::init() {
    signal(SIGINT, exit_signal_handler);
    signal(SIGTERM, exit_signal_handler);
}

bool ExitSignal::shouldExit() {
#ifdef FAH_CORE
	if(global_lifeline_pid != -1) {
		int err = kill(global_lifeline_pid, 0);
		if(err == -1 && errno == ESRCH) {
			return true;
		}
	}
#endif
	return global_exit;
}