#ifndef EXIT_SIGNAL_H_
#define EXIT_SIGNAL_H_

#include <signal.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
#endif

namespace ExitSignal {

#ifdef FAH_CORE
#ifdef _WIN32
	void setLifeline(DWORD pid);
#else
	void setLifeline(pid_t pid);
#endif
#endif

void init();

bool shouldExit();

}

#endif