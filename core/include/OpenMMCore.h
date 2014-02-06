#include "Core.h"
#include <OpenMM.h>

class OpenMMCore : public Core {

public:

    OpenMMCore(int frame_send_interval, int checkpoint_send_interval);
    ~OpenMMCore();

    virtual void main();

private:

    OpenMM::Context* _core_context;
    OpenMM::Context* _ref_context;

};