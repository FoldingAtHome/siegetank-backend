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

#include <iostream>
#include <iomanip>
#include <ctime>
#include <sstream>
#include <fstream>
#include <OpenMM.h>

#include "XTCWriter.h"
#include "OpenMMCore.h"
#include "kbhit.h"
#include "StateTests.h"
#include "ExitSignal.h"

#ifdef _WIN32
	#include <cstdint>
#endif

using namespace std;

extern "C" void registerSerializationProxies();
extern "C" void registerCpuPlatform();
extern "C" void registerOpenCLPlatform();
extern "C" void registerCudaPlatform();
#ifdef USE_PME_PLUGIN
    extern "C" void registerCpuPmeKernelFactories();
#endif

void OpenMMCore::registerComponents() {
    registerSerializationProxies();
#ifdef OPENMM_CPU
    registerCpuPlatform();
    #ifdef USE_PME_PLUGIN
        registerCpuPmeKernelFactories();
    #endif
    #define PLATFORM_NAME "CPU"
#elif OPENMM_CUDA
    registerCudaPlatform();
    #define PLATFORM_NAME "CUDA"
#elif OPENMM_OPENCL
    registerOpenCLPlatform();
    #define PLATFORM_NAME "OpenCL"
#else
    BAD DEFINE
#endif
}

OpenMMCore::OpenMMCore(string core_key, map<string, string> properties, std::ostream &logStream) :
    Core(core_key, logStream),
    checkpoint_send_interval_(6000),
    heartbeat_interval_(60),
    current_step_(0),
    last_checkpoint_step_(0),
    ref_context_(NULL),
    core_context_(NULL),
    ref_intg_(NULL),
    core_intg_(NULL),
    shared_system_(NULL),
    properties_(properties) {
}

OpenMMCore::~OpenMMCore() {
    logStream << "cleaning up" << endl;
    delete ref_context_;
    ref_context_ = NULL;
    delete core_context_;
    core_context_ = NULL;
    delete ref_intg_;
    ref_intg_ = NULL;
    delete core_intg_;
    core_intg_ = NULL;
    delete shared_system_;
    shared_system_ = NULL;
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
    /*
    vector<string> forceGroupNames;
    forceGroupNames = setupForceGroups(sys);
    for(int i=0;i<forceGroupNames.size();i++) {
        logStream << "    Group " << i << ": " << forceGroupNames[i] << endl;
    }
    */
    for(int i=0; i<sys->getNumForces(); i++) {
        OpenMM::Force &force = sys->getForce(i);
        try {
            OpenMM::AndersenThermostat &ATForce = dynamic_cast<OpenMM::AndersenThermostat &>(force);
            ATForce.setRandomNumberSeed(randomSeed);
            /*
            logStream << "Found AndersenThermostat @ " << ATForce.getDefaultTemperature() << " (default) Kelvin, " 
                       << ATForce.getDefaultCollisionFrequency() << " (default) collision frequency. " << endl; 
            */
            continue;
        } catch(const std::bad_cast &bc) {}
        try {
            OpenMM::MonteCarloBarostat &MCBForce = dynamic_cast<OpenMM::MonteCarloBarostat &>(force);
            MCBForce.setRandomNumberSeed(randomSeed);
            /*
            logStream << "Found MonteCarloBarostat @ " << MCBForce.getDefaultPressure() << " (default) Bar, " << MCBForce.getTemperature() 
                       << " Kelvin, " << MCBForce.getFrequency() << " pressure change frequency." << endl; 
            */    
            continue;
        } catch(const std::bad_cast &bc) {}
        try {
            OpenMM::NonbondedForce &NBForce = dynamic_cast<OpenMM::NonbondedForce &>(force);
        } catch(const std::bad_cast &bc) {}
    }
    int numAtoms = sys->getNumParticles();
    logStream << "System size: " << numAtoms << " atoms, " << sys->getNumForces() << " types of forces." << std::endl;
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

static void update_status(int seconds_per_frame, 
                          float ns_per_day,
                          int frames,
                          long long steps,
                          ostream &out = cout) {

    out << "\r";

    time_t current_time = time(NULL);
    // NOT THREADSAFE, but shouldn't matter for all practical reasons
    tm* timeinfo = std::localtime(&current_time);
    char buffer[80];
    std::strftime(buffer,80,"%b/%d %I:%M:%S%p",timeinfo);

    out << setw(17) << buffer
        << setw(10) << format_time(seconds_per_frame) << "  "
        << setw(7) << std::fixed << std::setprecision(2) << ns_per_day
        << setw(8) << frames
        << setw(11) << steps;
    out << " " << flush;

}

static void status_header(ostream &out) {
    out << "\r";
    out << setw(6) << "date"
        << setw(11) << "time"
        << setw(10) << "tpf"
        << setw(9) << "ns/day"
        << setw(8) << "frames"
        << setw(11) << "steps";
    out << "\n";
}

void OpenMMCore::startStream(const string &cc_uri,
                             const string &donor_token,
                             const string &target_id,
                             const string &proxy_string) {
    start_time_ = time(NULL);
    Core::startStream(cc_uri, donor_token, target_id, proxy_string);
    steps_per_frame_ = static_cast<int>(getOption<double>("steps_per_frame")+0.5);
    OpenMM::State *initial_state;
    cout << "start deserialization" << endl;
    if(files_.find("system.xml") != files_.end()) {
        istringstream system_stream(files_["system.xml"]);
        shared_system_ = OpenMM::XmlSerializer::deserialize<OpenMM::System>(system_stream);
    } else {
        throw std::runtime_error("Cannot find system.xml");
    }
    cout << "deserialized system" << endl;
    if(files_.find("state.xml") != files_.end()) {
        istringstream state_stream(files_["state.xml"]);
        initial_state = OpenMM::XmlSerializer::deserialize<OpenMM::State>(state_stream);
    } else {
        throw std::runtime_error("Cannot find state.xml");
    }
    cout << "deserialized state" << endl;
    if(files_.find("integrator.xml") != files_.end()) {
        istringstream core_integrator_stream(files_["integrator.xml"]);
        core_intg_ = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(core_integrator_stream);
        istringstream ref_integrator_stream(files_["integrator.xml"]); 
        ref_intg_ = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(ref_integrator_stream);
    } else {
        throw std::runtime_error("Cannot find integrator.xml");
    }
    cout << "deserialized integrator" << endl;
    int random_seed = time(NULL);
    cout << "start setting up system" << endl;
    setupSystem(shared_system_, random_seed);
    logStream << "\r                                                             " << flush;
    logStream << "\rcreating contexts: reference... " << flush;
    ref_context_ = new OpenMM::Context(*shared_system_, *ref_intg_,
        OpenMM::Platform::getPlatformByName("Reference"));
    logStream << "core... " << endl;
    core_context_ = new OpenMM::Context(*shared_system_, *core_intg_,
        OpenMM::Platform::getPlatformByName(PLATFORM_NAME), properties_);
    logStream << "setting initial states..." << endl;
    ref_context_->setState(*initial_state);
    core_context_->setState(*initial_state);
    logStream << "checking states for discrepancies..." << endl;
    checkState(*initial_state);
    checkState(core_context_->getState((
        OpenMM::State::Positions | 
        OpenMM::State::Velocities | 
        OpenMM::State::Parameters | 
        OpenMM::State::Energy | 
        OpenMM::State::Forces)));
    delete(initial_state);
    changemode(0);
}

void OpenMMCore::stopStream(string error_msg) {
    logStream << "stopping stream..." << endl;
    flushCheckpoint();
    Core::stopStream(error_msg);
}

void OpenMMCore::flushCheckpoint() {
    OpenMM::State state = core_context_->getState(
        OpenMM::State::Positions | 
        OpenMM::State::Velocities | 
        OpenMM::State::Parameters | 
        OpenMM::State::Energy | 
        OpenMM::State::Forces);
    checkState(state);
    ostringstream checkpoint;
    OpenMM::XmlSerializer::serialize<OpenMM::State>(&state, "State", checkpoint);
    map<string, string> checkpoint_files;
    checkpoint_files["state.xml"] = checkpoint.str();
    stringstream partial_steps;
    partial_steps << (current_step_ % steps_per_frame_);
    logStream << "partial frames: " << partial_steps.str() << endl;
    checkpoint_files["partial_steps"] = partial_steps.str();
    double frames = double(current_step_-last_checkpoint_step_)/double(steps_per_frame_);
    sendCheckpoint(checkpoint_files, frames, true);
    last_checkpoint_step_ = current_step_;
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

void OpenMMCore::checkFrameWrite() {
    // nothing is written on the first step;
    if(current_step_ > 0 && current_step_ % steps_per_frame_ == 0) {
        OpenMM::State state = core_context_->getState(
            OpenMM::State::Positions | 
            OpenMM::State::Velocities | 
            OpenMM::State::Parameters | 
            OpenMM::State::Energy | 
            OpenMM::State::Forces);
        checkState(state);
        state.getTime();
        OpenMM::Vec3 a,b,c;
        state.getPeriodicBoxVectors(a,b,c);
        vector<vector<float> > box(3, vector<float>(3, 0));
        box[0][0] = a[0]; box[0][1] = a[1]; box[0][2] = a[2];
        box[1][0] = b[0]; box[1][1] = b[1]; box[1][2] = b[2];
        box[2][0] = c[0]; box[2][1] = c[1]; box[2][2] = c[2];
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
        xtcwriter.append(current_step_, state.getTime(), box, positions);
        map<string, string> frame_files;
        frame_files["frames.xtc"] = frame_stream.str();
        sendFrame(frame_files);
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
    logStream << "entering main md loop..." << endl;
    try {
        changemode(1);

        double next_checkpoint = time(NULL) + checkpoint_send_interval_;
        double next_heartbeat = time(NULL) + heartbeat_interval_;

        if(files_.find("partial_steps") != files_.end()) {
            stringstream buffer(files_["partial_steps"]);
            buffer >> current_step_;
            last_checkpoint_step_ = current_step_;
        }

        long long starting_step = current_step_;
        cout << "resuming from step " << current_step_ << endl;
        status_header(logStream);

        while(true) {
#ifdef FAH_CORE
            if(current_step_ % 300 == 0) {
                string info_path = "./"+wu_dir+"/wuinfo_01.dat";
                ofstream file(info_path.c_str(), ios::binary);
                uint32_t unitType = 101;     ///< UNIT_FAH (101) for Folding@home work units
                char unitName[80] = "Streaming"; ///< Protein name
                uint32_t framesTotal = steps_per_frame_;  ///< Total # frames
                uint32_t framesDone = current_step_ % steps_per_frame_;   ///< # Frames complete
                uint32_t frameSteps = 1;   ///< # Dynamic steps per frame
                char reserved[416] = "";
                file.write((char *)&unitType, sizeof(unitType));
                file.write((char *)&unitName, 80);
                file.write((char *)&framesTotal, sizeof(framesTotal));
                file.write((char *)&framesDone, sizeof(framesDone));
                file.write((char *)&frameSteps, sizeof(frameSteps));
                file.write((char *)&reserved, 416);
                file.close();
            } 
#else
           if(current_step_ % 10 == 0) {
                update_status(timePerFrame(current_step_-starting_step),
                              nsPerDay(current_step_-starting_step),
                              current_step_/steps_per_frame_,
                              current_step_);
            }
#endif
            if(ExitSignal::shouldExit()) {
                changemode(0);
                break;
            }
            checkFrameWrite();
            if(time(NULL) > next_heartbeat) { 
                sendHeartbeat();
                next_heartbeat = time(NULL) + heartbeat_interval_;
            }
            if(time(NULL) > next_checkpoint) {
               flushCheckpoint();
               next_checkpoint = time(NULL) + checkpoint_send_interval_;
            }
            core_context_->getIntegrator().step(1);
            current_step_++;
        }
        stopStream();
    } catch(exception &e) {
        stopStream(e.what());
        logStream << e.what() << endl;
    }
}
