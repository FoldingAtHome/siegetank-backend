#include "Core.h"
#include <OpenMM.h>
#include <sstream>

class OpenMMCore : public Core {

public:

    OpenMMCore(int checkpoint_send_interval);
    ~OpenMMCore();

    virtual void main();

    void check_step(int current_step);

    void initialize();

    // send _checkpoint_xml to the server. The method is idempotent,
    // so sending multiple checkpoint is safe.
    void send_saved_checkpoint();

private:

    OpenMM::Context* _ref_context;
    OpenMM::Context* _core_context;
    OpenMM::System* _shared_system;

    std::string _checkpoint_xml;

    void _setup_system(OpenMM::System *system, int randomSeed) const;

};