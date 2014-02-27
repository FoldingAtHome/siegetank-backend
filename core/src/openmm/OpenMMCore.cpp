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
#include <XTCWriter.h>
#include <OpenMMCore.h>
#include <kbhit.h>
#include <StateTests.h>

using namespace std;

extern "C" void registerSerializationProxies();
extern "C" void registerCpuPlatform();
extern "C" void registerOpenCLPlatform();
extern "C" void registerCudaPlatform();
#ifdef USE_PME_PLUGIN
    extern "C" void registerCpuPmeKernelFactories();
#endif

OpenMMCore::OpenMMCore(int checkpoint_send_interval):
    Core(checkpoint_send_interval, "openmm", "6.0") {

}

OpenMMCore::~OpenMMCore() {
    // renable proper keyboard input
    changemode(0);
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

void OpenMMCore::_setup_system(OpenMM::System *sys, int randomSeed) const {
    vector<string> forceGroupNames;
    _logstream << "setup system" << endl;
    forceGroupNames = setupForceGroups(sys);
    for(int i=0;i<forceGroupNames.size();i++) {
        _logstream << "    Group " << i << ": " << forceGroupNames[i] << endl;
    }
    for(int i=0; i<sys->getNumForces(); i++) {
        OpenMM::Force &force = sys->getForce(i);
        try {
            OpenMM::AndersenThermostat &ATForce = dynamic_cast<OpenMM::AndersenThermostat &>(force);
            ATForce.setRandomNumberSeed(randomSeed);
            _logstream << "  Found AndersenThermostat @ " << ATForce.getDefaultTemperature() << " (default) Kelvin, " 
                       << ATForce.getDefaultCollisionFrequency() << " (default) collision frequency. " << endl; 
            continue;
        } catch(const std::bad_cast &bc) {}
        try {
            OpenMM::MonteCarloBarostat &MCBForce = dynamic_cast<OpenMM::MonteCarloBarostat &>(force);
            MCBForce.setRandomNumberSeed(randomSeed);
            _logstream << "  Found MonteCarloBarostat @ " << MCBForce.getDefaultPressure() << " (default) Bar, " << MCBForce.getTemperature() 
                       << " Kelvin, " << MCBForce.getFrequency() << " pressure change frequency." << endl;     
            continue;
        } catch(const std::bad_cast &bc) {}
        try {
            OpenMM::NonbondedForce &NBForce = dynamic_cast<OpenMM::NonbondedForce &>(force);
        } catch(const std::bad_cast &bc) {}
    }
    int numAtoms = sys->getNumParticles();
    _logstream << "    Found: " << numAtoms << " atoms, " << sys->getNumForces() << " forces." << std::endl;
}

static void update_status(string target_id, string stream_id, int seconds_per_frame, 
                          float ns_per_day, int frames, long long steps, ostream &out = cout) {

    int hours = seconds_per_frame/(60*60);
    int minutes = (seconds_per_frame-hours*60*60)/60;
    int seconds = seconds_per_frame%60;

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
    out << "\r";
    out << setw(10) << target_id.substr(0,8) 
        << setw(10) << stream_id.substr(0,8)
        << setw(10) << tpf.str() << "  "
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

void OpenMMCore::initialize(string cc_uri) {
    registerSerializationProxies();
#ifdef OPENMM_CPU
    registerCpuPlatform();
#ifdef USE_PME_PLUGIN
    registerCpuPmeKernelFactories();
#endif
    string platform_name("CPU");
#elif OPENMM_CUDA
    registerCudaPlatform();
    string platform_name("CUDA");
#elif OPENMM_OPENCL
    registerOpenCLPlatform();
    string platform_name("OpenCL");
#else
    BAD DEFINE
#endif

    Poco::URI uri(cc_uri);
    map<string, string> target_files;
    map<string, string> stream_files;

    startStream(uri, target_files, stream_files);
    // eg. _frame_send_interval = 50000 for OpenMM simulations
    _frame_send_interval = _frame_write_interval;
        
    OpenMM::System *shared_system;
    OpenMM::State *initial_state;
    OpenMM::Integrator *ref_intg;
    OpenMM::Integrator *core_intg;
    if(target_files.find("system.xml") != target_files.end()) {
        istringstream system_stream(target_files["system.xml"]);
        shared_system = OpenMM::XmlSerializer::deserialize<OpenMM::System>(system_stream);
    } else if(stream_files.find("system.xml") != stream_files.end()) {
        istringstream system_stream(stream_files["system.xml"]);
        shared_system = OpenMM::XmlSerializer::deserialize<OpenMM::System>(system_stream);
    } else {
        throw std::runtime_error("Cannot find system.xml");
    }
    if(target_files.find("state.xml") != target_files.end()) {
        istringstream state_stream(target_files["state.xml"]);
        initial_state = OpenMM::XmlSerializer::deserialize<OpenMM::State>(state_stream);
    } else if(stream_files.find("state.xml") != stream_files.end()) {
        istringstream state_stream(stream_files["state.xml"]);
        initial_state = OpenMM::XmlSerializer::deserialize<OpenMM::State>(state_stream);
    } else {
        throw std::runtime_error("Cannot find state.xml");
    }
    if(target_files.find("integrator.xml") != target_files.end()) {
        istringstream core_integrator_stream(target_files["integrator.xml"]);
        core_intg = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(core_integrator_stream);
        istringstream ref_integrator_stream(target_files["integrator.xml"]);
        ref_intg = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(ref_integrator_stream);
    } else if(stream_files.find("integrator.xml") != stream_files.end()) {
        istringstream core_integrator_stream(stream_files["integrator.xml"]);
        core_intg = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(core_integrator_stream);
        istringstream ref_integrator_stream(stream_files["integrator.xml"]);
        ref_intg = OpenMM::XmlSerializer::deserialize<OpenMM::Integrator>(ref_integrator_stream);
    } else {
        throw std::runtime_error("Cannot find integrator.xml");
    }
    int random_seed = time(NULL);
    _setup_system(shared_system, random_seed);
    cout << "\r                                                             " << flush;
    cout << "\rcreating contexts: reference... " << flush;
    _ref_context = new OpenMM::Context(*shared_system, *ref_intg, OpenMM::Platform::getPlatformByName("Reference"));
    cout << "core..." << flush;
    _core_context = new OpenMM::Context(*shared_system, *core_intg, OpenMM::Platform::getPlatformByName(platform_name));
    cout << "ok";
    _ref_context->setState(*initial_state);
    _core_context->setState(*initial_state);
    changemode(0);
}

void OpenMMCore::_send_saved_checkpoint() {
    // do not send a checkpoint if there's nothing there
    if(_checkpoint_xml.size() == 0) {
        return;
    }
    map<string, string> checkpoint_files;
    checkpoint_files["state.xml"] = _checkpoint_xml;
    sendCheckpointFiles(checkpoint_files, true);
    // flush
    _checkpoint_xml.clear();
}


void OpenMMCore::checkState(const OpenMM::State &core_state) const {

    _ref_context->setState(core_state);
    OpenMM::State reference_state = _ref_context->getState(
        OpenMM::State::Energy | 
        OpenMM::State::Forces);

    StateTests::checkForNans(core_state);
    StateTests::checkForDiscrepancies(core_state);
    StateTests::compareForcesAndEnergies(reference_state, core_state);

}

void OpenMMCore::checkFrameWrite(int current_step) {
    // nothing is written on the first step;
    if(current_step > 0 && current_step % _frame_write_interval == 0) {
        OpenMM::State state = _core_context->getState(
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
        sendFrameFiles(frame_files);
        // write checkpoint
        ostringstream checkpoint;
        OpenMM::XmlSerializer::serialize<OpenMM::State>(&state, "State", checkpoint);
        _checkpoint_xml = checkpoint.str();
    }
}

int OpenMMCore::timePerFrame(long long steps_completed) const {
    int time_diff = time(NULL)-_start_time;
    if(steps_completed == 0)
        return 0;
    return int(double(_frame_write_interval)*(time_diff)/steps_completed);
}

float OpenMMCore::nsPerDay(long long steps_completed) const {
    int time_diff = time(NULL)-_start_time;
    if(time_diff == 0)
        return 0;
    // time_step is in picoseconds
    double time_step = _core_context->getIntegrator().getStepSize();
    return (double(steps_completed)/time_diff)*(time_step/1e3)*86400;
}

void OpenMMCore::main() {
    try {
        long long current_step = 0;
        changemode(1);
        status_header(cout);
        while(true) {
            if(current_step % 10 == 0) {
                update_status(target_id, _stream_id, timePerFrame(current_step), nsPerDay(current_step),
                              current_step/_frame_write_interval, current_step);
            }
            if(exit()) {
                changemode(0);
                break;
            }
            if(kbhit()) {
                // handle keyboard events
                // c sends the previous checkpoints
                if('c' == char(getchar())) {
                    cout << "\rsending checkpoint" << flush;
                    _send_saved_checkpoint();
                }
            }
            checkFrameWrite(current_step);
            if(shouldHeartbeat()) {
                sendHeartbeat();
            }
            if(shouldSendCheckpoint()) {
               _send_saved_checkpoint();
            }
            _core_context->getIntegrator().step(1);
            current_step++;
        }
        stopStream();
    } catch(exception &e) {
        stopStream(e.what());
        cout << e.what() << endl;
    }

}
