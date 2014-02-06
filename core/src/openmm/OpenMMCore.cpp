// OS X SSL instructions:
// ./Configure darwin64-x86_64-cc -noshared
// g++ -I ~/poco/include Core.cpp ~/poco/lib/libPoco* /usr/local/ssl/lib/lib*
// Linux:
// g++ -I/home/yutong/openmm_install/include -I/usr/local/ssl/include -I/home/yutong/poco152_install/include -L/home/yutong/poco152_install/lib -L/usr/local/ssl/lib/ Core.cpp -lpthread -lPocoNetSSL -lPocoCrypto -lssl -lcrypto -lPocoUtil -lPocoJSON -ldl -lPocoXML -lPocoNet -lPocoFoundation -L/home/yutong/openmm_install/lib -L/home/yutong/openmm_install/lib/plugins -lOpenMMOpenCL_static /usr/lib/nvidia-current/libOpenCL.so -lOpenMMCUDA_static /usr/lib/nvidia-current/libcuda.so /usr/local/cuda/lib64/libcufft.so -lOpenMMCPU_static -lOpenMMPME_static -L/home/yutong/fftw_install/lib/ -lfftw3f -lfftw3f_threads -lOpenMM_static; ./a.out 

// Linux:
// g++ -I/Users/yutongzhao/openmm_install/include -I/usr/local/ssl/include -I/users/yutongzhao/poco152_install/include -L/users/yutongzhao/poco152_install/lib -L/usr/local/ssl/lib/ Core.cpp -lpthread -lPocoNetSSL -lPocoCrypto -lssl -lcrypto -lPocoUtil -lPocoJSON -ldl -lPocoXML -lPocoNet -lPocoFoundation -L/Users/yutongzhao/openmm_install/lib -lOpenMMCPU_static -L/Users/yutongzhao/openmm_install/lib/plugins -lOpenMM_static; ./a.out 

//  ./configure --static --prefix=/home/yutong/poco152_install --omit=Data/MySQL,Data/ODBC

#include <iostream>

#include <ctime>
#include <sstream>
#include <OpenMM.h>
#include <XTCWriter.h>
#include <OpenMMCore.h>
#include <kbhit.h>

using namespace std;
using namespace Poco;

#define OPENMM_CPU

extern "C" void registerSerializationProxies();
extern "C" void registerCpuPlatform();
extern "C" void registerOpenCLPlatform();

OpenMMCore::OpenMMCore(int frame_send_interval, int checkpoint_send_interval):
    Core(frame_send_interval, checkpoint_send_interval, "openmm", "6.0") {

}

OpenMMCore::~OpenMMCore() {
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
        } catch(const std::bad_cast &c) {
            force.setForceGroup(0);
        }
    }
    return forceGroupNames;
}

void OpenMMCore::_setup_system(OpenMM::System *sys, int randomSeed) const {
    vector<string> forceGroupNames;
    cout << "setup system" << endl;
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

void OpenMMCore::main() {
    registerSerializationProxies();
#ifdef OPENMM_CPU
    registerCpuPlatform();
#elif OPENMM_CUDA
    registerCudaPlatform();
#elif OPENMM_OPENCL
    registerOpenCLPlatform();
#else
    BAD DEFINE
#endif

    Poco::URI uri("https://127.0.0.1:8980/core/assign");
    string stream_id;
    string target_id;
    map<string, string> target_files;
    map<string, string> stream_files;

    start_stream(uri, stream_id, target_id, target_files, stream_files);

    OpenMM::System *shared_system;
    OpenMM::State *initial_state;
    OpenMM::Integrator *ref_intg;
    OpenMM::Integrator *core_intg;

    cout << "system" << endl;
    if(target_files.find("system.xml") != target_files.end()) {
        istringstream system_stream(target_files["system.xml"]);
        shared_system = OpenMM::XmlSerializer::deserialize<OpenMM::System>(system_stream);
    } else if(stream_files.find("system.xml") != stream_files.end()) {
        istringstream system_stream(stream_files["system.xml"]);
        shared_system = OpenMM::XmlSerializer::deserialize<OpenMM::System>(system_stream);
    } else {
        throw std::runtime_error("Cannot find system.xml");
    }

    cout << "state" << endl;
    if(target_files.find("state.xml") != target_files.end()) {
        istringstream state_stream(target_files["state.xml"]);
        initial_state = OpenMM::XmlSerializer::deserialize<OpenMM::State>(state_stream);
    } else if(stream_files.find("state.xml") != stream_files.end()) {
        istringstream state_stream(stream_files["state.xml"]);
        initial_state = OpenMM::XmlSerializer::deserialize<OpenMM::State>(state_stream);
    } else {
        throw std::runtime_error("Cannot find state.xml");
    }

    cout << "integrator" << endl;
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

    cout << "setting up system" << endl;

    int random_seed = time(NULL);
    _setup_system(shared_system, random_seed);

    // allow for kbhit's keyboard inputs
    changemode(1);

/*
    try {
        start_stream(uri, stream_id, target_id, target_files, stream_files);
        // take ostep();
        long long step = 0;
        while(true) {
            if(exit()) {
                // send checkpoint
                break;
            }
            if(kbhit()) {
                // handle keyboard events
                // c sends the previous checkpoints 
                // n sends the next available checkpoint
                cout << "keyboard event: " << char(getchar()) << endl;
            }
            if(step % _frame_write_interval == 0) {
                // write frame
            }
            if(step % _frame_send_interval == 0) {
                // get state
                // write checkpoint
                // send frame
            }
            if(step % _checkpoint_send_interval == 0) {

            }
        }

    } catch(exception &e) {
        cout << e.what() << endl;
    }
    stop_stream();
*/
}