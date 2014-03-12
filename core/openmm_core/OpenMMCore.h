#ifndef OPENMM_CORE_HH_
#define OPENMM_CORE_HH_

#include "Core.h"
#include <OpenMM.h>
#include <sstream>
#include <Benchmark.h>

class OpenMMCore : public Core {

public:

    OpenMMCore(int checkpoint_send_interval);
    ~OpenMMCore();

    virtual void main();

    /* initialize the core */
    void initialize(std::string uri);

    /* check the step and determine if we need to 1) write frame/send  frame, 
    or 2) send a checkpoint */
    void checkFrameWrite(int current_step);

    /* get time per frame in seconds */
    int timePerFrame(long long steps_completed) const;

    /* get nanoseconds per day of the current simulation */
    float nsPerDay(long long steps_completed) const;

    /* verify the openmm state */
    void checkState(const OpenMM::State &core_state) const;

private:

    /* send _checkpoint_xml to the server safely. This method is idempotent.

    if _checkpoint_xml is empty, then nothing is sent. Once a send succeeds
    then _checkpoint_xml is cleared().

    */
    void _send_saved_checkpoint();

    OpenMM::Context* _ref_context;
    OpenMM::Context* _core_context;
    OpenMM::System* _shared_system;

#ifdef USE_BENCHMARK
    Benchmark* _benchmark;
    double _current_speed;
#endif

    std::string _checkpoint_xml;

    void _setup_system(OpenMM::System *system, int randomSeed) const;

};

#endif