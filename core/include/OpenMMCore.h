#include "Core.h"

class OpenMMCore : public Core {

public:

    OpenMMCore(int frame_send_interval, int checkpoint_send_interval);
    ~OpenMMCore();

    virtual void main();

};