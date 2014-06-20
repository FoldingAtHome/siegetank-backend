#ifndef EXIT_SIGNAL_H_
#define EXIT_SIGNAL_H_

#include <signal.h>

namespace ExitSignal {

#ifdef FAH_CORE
void setLifeline(pid_t pid);
#endif

void init();

bool shouldExit();

}

#endif