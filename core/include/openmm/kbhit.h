#ifndef KBHIT_H_
#define KBHIT_H_

#ifdef _WIN32
#include <conio.h> /* kbhit(), getch() */
#else
void changemode(bool enable);
int kbhit();
#endif

#endif