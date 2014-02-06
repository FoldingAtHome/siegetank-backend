#ifdef _WIN32
#include <conio.h> /* kbhit(), getch() */
#else
void changemode(int dir);
int kbhit();
#endif