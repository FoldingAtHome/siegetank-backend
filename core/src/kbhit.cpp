#include <termios.h> /* tcgetattr(), tcsetattr() */
#include <unistd.h> /* read() */
#include <cstring>
#include <iostream>
#include <sys/select.h>

/* Usage:

int main() {
    changemode(1);
    while(true) {
        if(kbhit()) {
            cout << "keyboard event: " << char(getch()) << endl;
        }
    }
}

*/

void changemode(int dir) {
#ifndef _WIN32
  static struct termios oldt, newt;
 
  if ( dir == 1 )
  {
    tcgetattr( STDIN_FILENO, &oldt);
    newt = oldt;
    newt.c_lflag &= ~( ICANON | ECHO );
    tcsetattr( STDIN_FILENO, TCSANOW, &newt);
  }
  else
    tcsetattr( STDIN_FILENO, TCSANOW, &oldt);
#endif
}
 
int kbhit() {
  struct timeval tv;
  fd_set rdfs;
 
  tv.tv_sec = 0;
  tv.tv_usec = 0;
 
  FD_ZERO(&rdfs);
  FD_SET (STDIN_FILENO, &rdfs);
 
  select(STDIN_FILENO+1, &rdfs, NULL, NULL, &tv);
  return FD_ISSET(STDIN_FILENO, &rdfs);
}
