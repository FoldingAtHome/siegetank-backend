// Authors: Yutong Zhao <proteneer@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// OS X SSL instructions:
// ./Configure darwin64-x86_64-cc -noshared
// g++ -I ~/poco/include Core.cpp ~/poco/lib/libPoco* /usr/local/ssl/lib/lib*
// Linux:
// g++ -I/home/yutong/openmm_install/include -I/usr/local/ssl/include -I/home/yutong/poco152_install/include -L/home/yutong/poco152_install/lib -L/usr/local/ssl/lib/ Core.cpp -lpthread -lPocoNetSSL -lPocoCrypto -lssl -lcrypto -lPocoUtil -lPocoJSON -ldl -lPocoXML -lPocoNet -lPocoFoundation -L/home/yutong/openmm_install/lib -L/home/yutong/openmm_install/lib/plugins -lOpenMMOpenCL_static /usr/lib/nvidia-current/libOpenCL.so -lOpenMMCUDA_static /usr/lib/nvidia-current/libcuda.so /usr/local/cuda/lib64/libcufft.so -lOpenMMCPU_static -lOpenMMPME_static -L/home/yutong/fftw_install/lib/ -lfftw3f -lfftw3f_threads -lOpenMM_static; ./a.out 

// Linux:
// g++ -I/Users/yutongzhao/openmm_install/include -I/usr/local/ssl/include -I/users/yutongzhao/poco152_install/include -L/users/yutongzhao/poco152_install/lib -L/usr/local/ssl/lib/ Core.cpp -lpthread -lPocoNetSSL -lPocoCrypto -lssl -lcrypto -lPocoUtil -lPocoJSON -ldl -lPocoXML -lPocoNet -lPocoFoundation -L/Users/yutongzhao/openmm_install/lib -lOpenMMCPU_static -L/Users/yutongzhao/openmm_install/lib/plugins -lOpenMM_static; ./a.out 

//  ./configure --static --prefix=/home/yutong/poco152_install --omit=Data/MySQL,Data/ODBC

#include <iostream>
#include <iomanip>
#include <ctime>
#include <sstream>
#include <OpenMM.h>
#include <signal.h>

#include "XTCWriter.h"
#include "OpenMMCore.h"
#include "kbhit.h"
#include "StateTests.h"

#include <signal.h>
using namespace std;

extern "C" void registerSerializationProxies();
extern "C" void registerCpuPlatform();
extern "C" void registerOpenCLPlatform();
extern "C" void registerCudaPlatform();
#ifdef USE_PME_PLUGIN
    extern "C" void registerCpuPmeKernelFactories();
#endif

static sig_atomic_t global_exit = false;

static void exit_signal_handler(int param) {
    global_exit = true;
}

OpenMMCore::OpenMMCore(string engine, string core_key) :
    Core(engine, core_key),
    checkpoint_send_interval_(6000),
    heartbeat_interval_(60),
    ref_context_(NULL),
    core_context_(NULL) {

    registerSerializationProxies();
#ifdef OPENMM_CPU
    registerCpuPlatform();
    #ifdef USE_PME_PLUGIN
        registerCpuPmeKernelFactories();
    #endif
    platform_name_ = "CPU";
#elif OPENMM_CUDA
    registerCudaPlatform();
    platform_name_ = "CUDA";
#elif OPENMM_OPENCL
    registerOpenCLPlatform();
    platform_name_ = "OpenCL";
#else
    BAD DEFINE
#endif

    global_exit = false;
    signal(SIGINT, exit_signal_handler);
    signal(SIGTERM, exit_signal_handler);

}

OpenMMCore::~OpenMMCore() {
    // renable proper keyboard input
    delete ref_context_;
    delete core_context_;
    changemode(0);
}

void OpenMMCore::setCheckpointSendInterval(int interval) {
    checkpoint_send_interval_ = interval;
}

void OpenMMCore::setHeartbeatInterval(int interval) {
    heartbeat_interval_ = interval;
}

static vector<string> setupForceGroups(OpenMM::System *sys) {
    vector<string> forceGroupNames(3);
    for(int i=0;i<sys->getNumForces();i++) {
        OpenMM::Force &force = sys->getForce(i);
        forceGroupNames[0]="Everything Else";
        try {
            OpenMM::NonbondedForce &nonbonded = dynamic_cast<OpenMM::NonbondedForce &>(force);
            nonbonded.setForceGroup(1);
            forceGroupNames[1]="Nonbonded Direct Space";
            if(nonbonded.getNonbondedMethod() == OpenMM::NonbondedForce::PME) {
                nonbonded.setReciprocalSpaceForceGroup(2);
                forceGroupNames[2]="Nonbonded Reciprocal Space";
            }
        } catch(const std::bad_cast &c  ) {
            force.setForceGroup(0);
        }
    }
    return forceGroupNames;
}


void OpenMMCore::setupSystem(OpenMM::System *sys, int randomSeed) const {
    vector<string> forceGroupNames;
    cout << "setup system" << endl;
    forceGroupNames = setupForceGroups(sys);
    for(int i=0;i<forceGroupNames.size();i++) {
        cout << "    Group " << i << ": " << forceGroupNames[i] << endl;
    }
    for(int i=0; i<sys->getNumForces(); i++) {
        OpenMM::Force &force = sys->getForce(i);
        try {
            OpenMM::AndersenThermostat &ATForce = dynamic_cast<OpenMM::AndersenThermostat &>(force);
            ATForce.setRandomNumberSeed(randomSeed);
            cout << "  Found AndersenThermostat @ " << ATForce.getDefaultTemperature() << " (default) Kelvin, " 
                       << ATForce.getDefaultCollisionFrequency() << " (default) collision frequency. " << endl; 
            continue;
        } catch(const std::bad_cast &bc) {}
        try {
            OpenMM::MonteCarloBarostat &MCBForce = dynamic_cast<OpenMM::MonteCarloBarostat &>(force);
            MCBForce.setRandomNumberSeed(randomSeed);
            cout << "  Found MonteCarloBarostat @ " << MCBForce.getDefaultPressure() << " (default) Bar, " << MCBForce.getTemperature() 
                       << " Kelvin, " << MCBForce.getFrequency() << " pressure change frequency." << endl;     
            continue;
        } catch(const std::bad_cast &bc) {}
        try {
            OpenMM::NonbondedForce &NBForce = dynamic_cast<OpenMM::NonbondedForce &>(force);
        } catch(const std::bad_cast &bc) {}
    }
    int numAtoms = sys->getNumParticles();
    cout << "    Found: " << numAtoms << " atoms, " << sys->getNumForces() << " forces." << std::endl;
}


static string format_time(int input_seconds) {

    int hours = input_seconds/(60*60);
    int minutes = (input_seconds-hours*60*60)/60;
    int seconds = input_seconds%60;

    stringstream tpf;
    // hours are added conditionally
    if(hours > 0) {
        tpf << hours << ":";
    }
    // always add minutes
    if(minutes > 0) {
        if(minutes < 10) {
            tpf << "0" << minutes << ":";
        } else {
            tpf << minutes << ":";
        }
    } else {
        tpf << "00:";
    }
    // always add seconds
    if(seconds > 0) {
        if(seconds < 10) {
            tpf << "0" << seconds;
        } else {
            tpf << seconds;
        }
    } else {
        tpf << "00";
    }

    return tpf.str();
}

static void update_status(string target_id,
                          string stream_id,
                          int seconds_per_frame, 
                          float ns_per_day,
                          int frames,
                          long long steps,
                          ostream &out = cout) {

    out << "\r";
    out << setw(10) << target_id.substr(0,8) 
        << setw(10) << stream_id.substr(0,8)
        << setw(10) << format_time(seconds_per_frame) << "  "
        << setw(7) << std::fixed << std::setprecision(2) << ns_per_day
        << setw(8) << frames
        << setw(11) << steps;
    out << flush;
}

static void status_header(ostream &out) {
    out << "\r";
    out << setw(10) << "target"
        << setw(10) << "stream"
        << setw(10) << "tpf"
        << setw(9) << "ns/day"
        << setw(8) << "frames"
        << setw(11) << "steps";
    out << "\n";
}


void OpenMMCore::startStream(const string &cc_uri,
                             const string &donor_token,
                             const string &target_id) {
    start_time_ = time(NULL);
    cout << "A" << endl;
    Core::startStream(cc_uri, donor_token, target_id);
    cout << "B" << endl;
    steps_per_frame_ = static_cast<int>(getOption<double>("steps_per_frame")+0.5);
    cout << steps_per_frame_ << endl;
    OpenMM::System *shared_system;
    OpenMM::State *initial_state;
    OpenMM::Integrator *ref_intg;
    OpenMM::Integrator *core_intg;
    if(files_.find("system.xml") != files_.end()) {
        istringstream system_stream(files_["system.xml"]);
        shared_system = OpenMM::XmlSerializer::deserialize<OpenMM::System>(system_stream);
    } else {
        throw std::runtime_error("Cannot find system.xml");
    }
    cout << "C" << endl;
    if(files_.find("state.xml") != files_.end()) {
        cout << "C.1" << endl;
        istringstream state_stream(files_["state.xml"]);
        initial_state = OpenMM::XmlSerializer::deserialize<OpenMM::State>(state_stream);
    } else {
        cout << "C.2" << endl;
        throw std::runtime_error("Cannot find state.xml");
    }
    cout << "D" << endl;
    if(files_.find("integrator.xml") != files_.end()) {
        istringstream core_integrator_stream(files_["integrator.xml"]);
        core_intg = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(core_integrator_stream);
        istringstream ref_integrator_stream(files_["integrator.xml"]);
        ref_intg = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(ref_integrator_stream);
    } else {
        throw std::runtime_error("Cannot find integrator.xml");
    }
    int random_seed = time(NULL);
    setupSystem(shared_system, random_seed);
    cout << "\r                                                             " << flush;
    cout << "\rcreating contexts: reference... " << flush;
    ref_context_ = new OpenMM::Context(*shared_system, *ref_intg,
        OpenMM::Platform::getPlatformByName("Reference"));
    cout << "core..." << flush;
    core_context_ = new OpenMM::Context(*shared_system, *core_intg,
        OpenMM::Platform::getPlatformByName(platform_name_));
    cout << "ok";
    ref_context_->setState(*initial_state);
    core_context_->setState(*initial_state);
    delete(initial_state);
    changemode(0);
}


void OpenMMCore::stopStream(string error_msg) {
    Core::stopStream(error_msg);
    delete ref_context_;
    ref_context_ = NULL;
    delete core_context_;
    ref_context_ = NULL;
}

void OpenMMCore::flushCheckpoint() {
    if(last_checkpoint_.size() == 0)
        return;
    map<string, string> checkpoint_files;
    checkpoint_files["state.xml"] = last_checkpoint_;
    sendCheckpoint(checkpoint_files, true);
    last_checkpoint_.clear();
}

void OpenMMCore::checkState(const OpenMM::State &core_state) const {

    ref_context_->setState(core_state);
    OpenMM::State reference_state = ref_context_->getState(
        OpenMM::State::Energy | 
        OpenMM::State::Forces);

    StateTests::checkForNans(core_state);
    StateTests::checkForDiscrepancies(core_state);
    StateTests::compareForcesAndEnergies(reference_state, core_state);

}

void OpenMMCore::checkFrameWrite(int current_step) {
    // nothing is written on the first step;
    if(current_step > 0 && current_step % steps_per_frame_ == 0) {
        OpenMM::State state = core_context_->getState(
            OpenMM::State::Positions | 
            OpenMM::State::Velocities | 
            OpenMM::State::Parameters | 
            OpenMM::State::Energy | 
            OpenMM::State::Forces);

        // todo: check against reference context.
        checkState(state);

        state.getTime();
        OpenMM::Vec3 a,b,c;
        state.getPeriodicBoxVectors(a,b,c);
        vector<vector<float> > box(3, vector<float>(3, 0));
        box[0][0] = a[0]; box[0][1] = a[1]; box[0][2] = a[2];
        box[1][0] = b[0]; box[1][1] = b[1]; box[1][2] = b[2];
        box[2][0] = c[0]; box[2][1] = c[1]; box[2][2] = b[2];
        vector<OpenMM::Vec3> state_positions = state.getPositions();
        vector<vector<float> > positions(state_positions.size(),
                                         vector<float>(3,0));
        for(int i=0; i<state_positions.size(); i++) {
            for(int j=0; j<3; j++) {
                positions[i][j] = state_positions[i][j];
            }
        }
        // write frame
        ostringstream frame_stream;
        XTCWriter xtcwriter(frame_stream);
        xtcwriter.append(current_step, state.getTime(), box, positions);
        map<string, string> frame_files;
        frame_files["frames.xtc"] = frame_stream.str();
        sendFrame(frame_files);
        // write checkpoint
        ostringstream checkpoint;
        OpenMM::XmlSerializer::serialize<OpenMM::State>(&state, "State", checkpoint);
        last_checkpoint_ = checkpoint.str();
    }
}

int OpenMMCore::timePerFrame(long long steps_completed) const {
    int time_diff = time(NULL)-start_time_;
    if(steps_completed == 0)
        return 0;
    return int(double(steps_per_frame_)*(time_diff)/steps_completed);
}

float OpenMMCore::nsPerDay(long long steps_completed) const {
    int time_diff = time(NULL)-start_time_;
    if(time_diff == 0)
        return 0;
    // time_step is in picoseconds
    double time_step = core_context_->getIntegrator().getStepSize();
    return (double(steps_completed)/time_diff)*(time_step/1e3)*86400;
}

void OpenMMCore::main() {
    try {
        long long current_step = 0;
        changemode(1);
        status_header(cout);

        double next_checkpoint = time(NULL) + checkpoint_send_interval_;
        double next_heartbeat = time(NULL) + heartbeat_interval_;

        while(true) {
            if(current_step % 10 == 0) {
                update_status(target_id_,
                              stream_id_,
                              timePerFrame(current_step),
                              nsPerDay(current_step),
                              current_step/steps_per_frame_,
                              current_step);
            }
            if(global_exit) {
                changemode(0);
                break;
            }
            if(kbhit()) {
                if('c' == char(getchar())) {
                    cout << "\rsending checkpoint" << flush;
                    flushCheckpoint();
                }
            }
            checkFrameWrite(current_step);
            if(time(NULL) > next_heartbeat) { 
                sendHeartbeat();
                next_heartbeat = time(NULL) + heartbeat_interval_;
            }
            if(time(NULL) > next_checkpoint) {
               flushCheckpoint();
               next_checkpoint = time(NULL) + checkpoint_send_interval_;
            }
            core_context_->getIntegrator().step(1);
            current_step++;
        }
        stopStream();
    } catch(exception &e) {
        stopStream(e.what());
        cout << e.what() << endl;
    }

}
