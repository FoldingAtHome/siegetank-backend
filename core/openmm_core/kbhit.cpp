#ifndef WIN32
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

// if enable == true, then the terminal no longer echos input character
// and the terminal no longer waits for an enter even to process the
// input key.
// if enable == false, then the terminal reverts to the standard behavior
void changemode(bool enable) {
#ifdef FAH_CORE
    struct termios old_state;
    tcgetattr(STDIN_FILENO, &old_state);
    struct termios new_state = old_state;
    if(enable) {
        new_state.c_lflag &= ~(ICANON | ECHO);
        tcsetattr(STDIN_FILENO, TCSANOW, &new_state);
    } else {
        new_state.c_lflag |= (ICANON | ECHO);
        tcsetattr(STDIN_FILENO, TCSANOW, &new_state);
    }
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

#else

void changemode(bool enable) {

}
 
#endif