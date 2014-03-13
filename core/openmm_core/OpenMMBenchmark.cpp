#include "OpenMMBenchmark.h"
#include <sstream>
#include <fstream>
#include <iostream>
#include <sys/time.h>

using namespace OpenMM;
using namespace std;

OpenMMBenchmark::OpenMMBenchmark(string platformName, 
                                 std::map<std::string, std::string> contextProperties) :
                                 total_iterations(0),
                                 time_elapsed(5) {
    
    timeval start;
    gettimeofday(&start, NULL);
    ifstream sys_stream("B_System.xml", ios::in | ios::binary);
    sys = OpenMM::XmlSerializer::deserialize<OpenMM::System>(sys_stream);
    ifstream state_stream("B_State.xml", ios::in | ios::binary);
    initial_state = OpenMM::XmlSerializer::deserialize<OpenMM::State>(state_stream);
    ifstream intg_stream("B_Integrator.xml", ios::in | ios::binary);
    intg = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(intg_stream);
    ctxt = new Context(*sys, *intg, OpenMM::Platform::getPlatformByName(platformName), contextProperties);
    ctxt->setState(*initial_state);
    intg->step(1);

    timeval end;
    gettimeofday(&end, NULL);
    double diff_sec = (end.tv_sec+end.tv_usec/1e6) - 
                      (start.tv_sec+start.tv_usec/1e6);

    cout << "Initializing constructors took " << diff_sec << " sec" << endl;
}

double OpenMMBenchmark::speed() {
    timeval start;
    gettimeofday(&start, NULL);
    const int iterations = 15;
    total_iterations += iterations;
    ctxt->setState(*initial_state);
    intg->step(iterations);
    ctxt->getState(OpenMM::State::Positions  | OpenMM::State::Velocities | 
                   OpenMM::State::Parameters | OpenMM::State::Energy | 
                   OpenMM::State::Forces);
    timeval end;
    gettimeofday(&end, NULL);
    time_elapsed += (end.tv_sec+end.tv_usec/1e6) - 
                    (start.tv_sec+start.tv_usec/1e6);
    average = total_iterations/time_elapsed;

    return average;
}

OpenMMBenchmark::~OpenMMBenchmark() {
    delete ctxt;
    delete intg;
    delete initial_state;
    delete sys;
}