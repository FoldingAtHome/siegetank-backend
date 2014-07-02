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

#ifdef WIN32
      #define NOMINMAX
#endif

#include "OpenMMCore.h"
#include "ezOptionParser.h"
#include "ExitSignal.h"

#include <string>
#include <iostream>
#include <fstream>
#include <cstdlib>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
    #include <windows.h>
    void sleep(unsigned seconds)
    {
        Sleep(seconds*1000);
    }
#else
    #include <unistd.h>
#endif

using namespace std;

#ifdef OPENMM_OPENCL 
#include "gpuinfo.h"
#ifdef FAH_CORE
#include <CL/cl.h>
static string guessPlatformId() {
    string platformId;
    cl_platform_id platforms[100];
    cl_uint platforms_n = 0;
    clGetPlatformIDs(100, platforms, &platforms_n);
    for (unsigned i=0; i<platforms_n; i++) {
        char buffer[10240];
        clGetPlatformInfo(platforms[i], CL_PLATFORM_VENDOR, 10240, buffer, NULL);
        string VENDOR(buffer);
        if((VENDOR.find("NVIDIA") != string::npos) || (VENDOR.find("Advanced Micro Devices") != string::npos)) {
            stringstream b;
            b << i;
            platformId = b.str();
        }
    }
    if(platformId.size() != 1) {
        throw(std::runtime_error(string("Bad platformId size.\n")+platformId));
    }
    return platformId;
}

static string getPlatformId(const string& gpuvendor) {
    string substr;
    if(gpuvendor.compare("amd") == 0) {
        substr = "Advanced Micro Devices";
    } else if(gpuvendor.compare("ati") == 0) {
        substr = "Advanced Micro Devices";
    } else if(gpuvendor.compare("nvidia") == 0) {
        substr = "NVIDIA";
    } else {
        throw(std::runtime_error("Bad gpu-vendor flag passed"));
    }
    string platformId;
    cl_platform_id platforms[100];
    cl_uint platforms_n = 0;
    clGetPlatformIDs(100, platforms, &platforms_n);
    for (unsigned i=0; i<platforms_n; i++) {
        char buffer[10240];
        clGetPlatformInfo(platforms[i], CL_PLATFORM_VENDOR, 10240, buffer, NULL);
        string VENDOR(buffer);
        if(VENDOR.find(substr) != string::npos) {
            stringstream b;
            b << i;
            platformId = b.str();
        }
    }
    if(platformId.size() != 1) {
        throw(std::runtime_error(string("Bad platformId size.\n")+platformId));
    }
    return platformId;
}
#endif // #ifdef FAH_CORE
#elif OPENMM_CUDA
#include "gpuinfo.h"
#endif

static void write_spoiler(ostream &outstream) {
    outstream << "                                          O              O                     " << std::endl;
    outstream << "   P R O T E N E E R     C--N              \\              \\               N    " << std::endl;
    outstream << "                         |                  C              C=O           / \\-C " << std::endl;
    outstream << "                         C                 /               |          N-C     \\" << std::endl;
    outstream << "  .C-C                 C/                  C               C           |      C" << std::endl;
    outstream << " /    \\          O     |                   |               /           N      |" << std::endl;
    outstream << "C     C          |     |           O       C              C                 /-C" << std::endl;
    outstream << " \\_N_/ \\   N    _C_    C           |      /         O    /                 C   " << std::endl;
    outstream << "        C-/ \\_C/   \\N-/ \\    N   /-C-\\   C          |    |           O    /    " << std::endl;
    outstream << "        |     |           C-/ \\C/     N-/ \\_   N\\  /C\\  -C      N    |    |    " << std::endl;
    outstream << "        O     |           |    |            \\C/  C/   N/  \\_C__/ \\   C-\\  C    " << std::endl;
    outstream << "              C           O    |             |   |          |     C-/   N/ \\-C" << std::endl;
    outstream << "               \\_C             C             O   |          O     |          | " << std::endl;
    outstream << "                  \\             \\-O              C                C          O " << std::endl;
    outstream << "                  |                               \\                \\           " << std::endl;
    outstream << "                  C    N         Folding@Home      C--N             C          " << std::endl;
    outstream << "                   \\   |            OCore          |                |          " << std::endl;
    outstream << "                    N--C                           O                |          " << std::endl;
    outstream << "                        \\        Yutong Zhao                       C=O        " << std::endl;
    outstream << "                         N    proteneer@gmail.com                 /           " << std::endl;
    outstream << "                                                                 O            " << std::endl;
    outstream << "                                  version "<< CORE_VERSION << "                   " << std::endl;
    outstream << "===============================================================================" << std::endl;
}

int main(int argc, const char * argv[]) {

    // parse options here
    ez::ezOptionParser opt;

    opt.overview = "Folding@Home OpenMM Core";
    opt.syntax = "ocore [OPTIONS]";
    opt.example = "ocore --checkpoint 3600\n";

    opt.add(
        "", // Default.
        0, // Required?
        0, // Number of args expected.
        0, // Delimiter if expecting multiple args.
        "Display usage instructions.", // Help description.
        "-h",     // Flag token. 
        "-help",  // Flag token.
        "--help" // Flag token.
    );

    opt.add(
        "",
        0,
        0,
        0,
        "Display version and exit.",
        "--version"
    );

    opt.add(
        //"cc.proteneer.com", // Default.
        "127.0.0.1:8980",
        0, // Required?
        1, // Number of args expected.
        0, // Delimiter if expecting multiple args.
        "Command Center URI", // Help description.
        "--cc"
    );

    opt.add(
        "7200", // Default.
        0, // Required?
        1, // Number of args expected.
        0, // Delimiter if expecting multiple args.
        "Checkpoint interval in seconds", // Help description.
        "--checkpoint"     // Flag token. 
    );

    opt.add(
        "",
        0,
        0,
        0,
        "Hide startup spoiler",
        "--nospoiler"
    );

    opt.add(
        "",
        0,
        1,
        0,
        "Fully qualified 36 digit target_id",
        "--target",
        "--target_id");

    opt.add(
        "",
        0,
        1,
        0,
        "Donor's access token",
        "--donor_token",
        "--token");

    opt.add(
        "",
        0,
        1,
        0,
        "Proxy string, [username:password@]host:port. Ex: localhost:8080, ytz:random_pass@localhost:8080",
        "--proxy");

    opt.add(
        "",
        0,
        1,
        0,
        "Number of seconds the core should run before exiting",
        "--duration");

#ifdef FAH_CORE
    opt.add(
        "",
        0,
        1,
        0,
        "FAHClient directory",
        "-dir");

    opt.add(
        "",
        0,
        1,
        0,
        "GPU Vendor (either nvidia or amd)",
        "-gpu-vendor");

    opt.add(
        "0",
        0,
        1,
        0,
        "GPU Index",
        "-gpu");

    opt.add(
        "0",
        0,
        1,
        0,
        "Lifeline of the parent process",
        "-lifeline");
#endif

#ifdef OPENMM_OPENCL
    opt.add(
        "",
        0,
        1,
        0,
        "Which OpenCL platform to use",
        "--platformId");

    opt.add(
        "",
        0,
        1,
        0,
        "Which OpenCL device to use",
        "--deviceId");

    opt.add(
        "",
        0,
        0,
        0,
        "List all OpenCL platforms and devices",
        "--devices");
#elif OPENMM_CUDA
    opt.add(
        "",
        0,
        1,
        0,
        "Which CUDA device to use",
        "--deviceId");

    opt.add(
        "",
        0,
        0,
        0,
        "List all CUDA devices",
        "--devices");
#endif 

    opt.parse(argc, argv);

    if(opt.isSet("-h")) {
        std::string usage;
        opt.getUsage(usage);
        std::cout << usage;
        return 0;
    }
    if(opt.isSet("--version")) {
        std::cout << CORE_VERSION << endl;
        return 0;
    }
    if(!opt.isSet("--nospoiler")) {
        write_spoiler(cout);
    }
    if(opt.isSet("--duration")) {
        int seconds_until_quit;
        opt.get("--duration")->getInt(seconds_until_quit);
        ExitSignal::setExitTime(seconds_until_quit);
    }

    map<string, string> contextProperties;

#ifdef OPENMM_OPENCL
#ifdef FAH_CORE
    string platformId;
    string gpu_vendor;
    opt.get("-gpu-vendor")->getString(gpu_vendor);
    if(gpu_vendor == "VENDOR_NOT_SET") {
        platformId = guessPlatformId();
        cout << "guessing platformId.." << platformId << endl;
    } else {
        platformId = getPlatformId(gpu_vendor);
        cout << "found on platformId " << platformId << endl;
    }
    string dIndex;
    opt.get("-gpu")->getString(dIndex);
    contextProperties["OpenCLDeviceIndex"] = dIndex[0];
    contextProperties["OpenCLPlatformIndex"] = platformId;
    contextProperties["OpenCLPrecision"] = "single";
#else
    if(opt.isSet("--devices")) {
        cout << endl;
        Util::listOpenCLDevices();
        return 1;
    }
    if(opt.isSet("--platformId") != opt.isSet("--deviceId")) {
        cout << "You must either specify both platformId and deviceId, or specify neither" << endl;
        return 1;
    }
    if(opt.isSet("--platformId")) {
        string pid;
        opt.get("--platformId")->getString(pid);
        contextProperties["OpenCLPlatformIndex"] = pid;
    }
    if(opt.isSet("--deviceId")) {
        string did;
        opt.get("--deviceId")->getString(did);
        if(did.find(",") != string::npos) {
            cout << "Using multiple GPUs to run the same simulation is not currently supported" << endl;
            return 1;
        };
        contextProperties["OpenCLDeviceIndex"] = did;
    }
#endif 
#elif OPENMM_CUDA
    if(opt.isSet("--devices")) {
        cout << endl;
        Util::listCUDADevices();
        return 1;
    }
    if(opt.isSet("--deviceId")) {
        string did;
        opt.get("--deviceId")->getString(did);
        if(did.find(",") != string::npos) {
            cout << "Using multiple GPUs to run the same simulation is not currently supported" << endl;
            return 1;
        };
        contextProperties["CudaDeviceIndex"] = did;
    }
#endif

    string cc_uri;
    opt.get("--cc")->getString(cc_uri);
    int checkpoint_frequency;
    opt.get("--checkpoint")->getInt(checkpoint_frequency);
    string donor_token;

    if(opt.isSet("--donor_token")) {
        opt.get("--donor_token")->getString(donor_token);
        if(donor_token.length() != 36) {
            throw std::runtime_error("donor_token must be 36 characters");
        }
    }

    string target_id;
    if(opt.isSet("--target_id")) {
        opt.get("--target_id")->getString(target_id);
        if(target_id.length() != 36) {
            throw std::runtime_error("target_id must be 36 characters");
        }
    }

    string proxy_string;
    if(opt.isSet("--proxy")) {
        opt.get("--proxy")->getString(proxy_string);
    }


    ExitSignal::init();
    OpenMMCore::registerComponents();

#ifdef FAH_CORE
    if(opt.isSet("-lifeline")) {
        int lifeline;
        opt.get("-lifeline")->getInt(lifeline);
        ExitSignal::setLifeline(lifeline);
    }
#endif

    int delay_in_sec = 1;
    time_t next_sleep_time = time(NULL);

    while(!ExitSignal::shouldExit()) {
        try {

#ifdef FAH_CORE
            string wu_dir;
            opt.get("-dir")->getString(wu_dir);
            string logpath("./"+wu_dir+"/logfile_01.txt");
            ofstream logfile(logpath.c_str(), std::ios::binary);
#endif
            OpenMMCore core(ENGINE_KEY,
                            contextProperties
#ifdef FAH_CORE
                            ,
                            logfile
#endif
                            );
#ifdef FAH_CORE
            core.wu_dir = wu_dir;
#endif
            cout << "setting checkpoint interval to " << checkpoint_frequency << " seconds" << endl;
            core.setCheckpointSendInterval(checkpoint_frequency);
            cout << "sleeping for " << delay_in_sec << " seconds" << endl;
            next_sleep_time = time(NULL) + delay_in_sec;
            while(time(NULL) < next_sleep_time) {
                if(ExitSignal::shouldExit()) {
                    exit(0);
                }
            }
            delay_in_sec = min(delay_in_sec * 5, 300);
            core.startStream(cc_uri, donor_token, target_id, proxy_string);
            delay_in_sec = 1;
            core.main();
        } catch(const exception &e) {
            cout << e.what() << endl;
        }
    }
}
