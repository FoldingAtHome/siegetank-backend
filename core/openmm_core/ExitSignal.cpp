#include "ExitSignal.h"
#include <errno.h>

static sig_atomic_t global_exit = false;

#ifdef FAH_CORE
#ifdef _WIN32
static DWORD global_lifeline_pid = 0;
void ExitSignal::setLifeline(DWORD pid) {
	global_lifeline_pid = pid;
}

static bool pid_is_dead() {
	if(global_lifeline_pid != 0) {
	    HANDLE process = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, global_lifeline_pid);
		if(process == NULL) {
			return false;
		}
		DWORD exitCode;
		if(GetExitCodeProcess(process, &exitCode)) {
			CloseHandle(process);
			if(exitCode == STILL_ACTIVE) {
				return false;
			} else {
				return true;
			}
		} else {
			CloseHandle(process);
			return false;
		}
	} else {
		return false;
	}
}

#else
static pid_t global_lifeline_pid = -1;
void ExitSignal::setLifeline(pid_t pid) {
	global_lifeline_pid = pid;
}

static bool pid_is_dead() {
	if(global_lifeline_pid != -1) {
		int err = kill(global_lifeline_pid, 0);
		if(err == -1 && errno == ESRCH) {
			return true;
		} else {
			return false;
		}
	} else {
		return false;
	}
}

#endif
#endif


static void exit_signal_handler(int param) {
    global_exit = true;
}

void ExitSignal::init() {
    signal(SIGINT, exit_signal_handler);
    signal(SIGTERM, exit_signal_handler);
}

bool ExitSignal::shouldExit() {
	return pid_is_dead() || global_exit;
}