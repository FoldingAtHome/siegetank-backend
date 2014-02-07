#include "Core.h"
#include <OpenMM.h>
#include <sstream>

class OpenMMCore : public Core {

public:

    OpenMMCore(int checkpoint_send_interval);
    ~OpenMMCore();

    virtual void main();

    /* check the step and determine if we need to 1) write a frame, or
    2) send a frame, or 3) send a checkpoint */
    void check_step(int current_step);

    void initialize();


private:


    /* send _checkpoint_xml to the server safely. This method is idempotent.

    if _checkpoint_xml is empty, then nothing is sent. Once a send succeeds
    then _checkpoint_xml is cleared().

    */
    void _send_saved_checkpoint();

    OpenMM::Context* _ref_context;
    OpenMM::Context* _core_context;
    OpenMM::System* _shared_system;

    std::string _checkpoint_xml;

    void _setup_system(OpenMM::System *system, int randomSeed) const;

};