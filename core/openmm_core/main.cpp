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

#include "OpenMMCore.h"
#include "ezOptionParser.h"
#include "ExitSignal.h"

#include <string>
#include <iostream>

#ifdef _WIN32
    #include <windows.h>
#else
    #include <unistd.h>
#endif

#ifdef OPENMM_OPENCL 
#include "gpuinfo.h"
#elif OPENMM_CUDA
#include "gpuinfo.h"
#endif

using namespace std;


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
}

int main(int argc, const char * argv[]) {

    // parse options here
    ez::ezOptionParser opt;

    opt.overview = "Folding@Home OpenMM Core";
    opt.syntax = "ocore [OPTIONS]";
    opt.example = "ocore --cc https://127.0.0.1:8980/core/assign --checkpoint 600\n";

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
        "cc.proteneer.com", // Default.
        0, // Required?
        1, // Number of args expected.
        0, // Delimiter if expecting multiple args.
        "Command Center URI", // Help description.
        "--cc"
    );

    opt.add(
        "600", // Default.
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
        "--target_id");

    opt.add(
        "",
        0,
        1,
        0,
        "Donor's access token",
        "--donor_token");

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
    // not implemented
#endif 
    opt.parse(argc, argv);
    if(opt.isSet("-h")) {
        std::string usage;
        opt.getUsage(usage);
        std::cout << usage;
        return 1;
    }
    if(opt.isSet("--version")) {
        std::cout << CORE_VERSION << endl;
        return 1;
    }
    if(!opt.isSet("--nospoiler")) {
        write_spoiler(cout);
    }
    map<string, string> properties;
#ifdef OPENMM_OPENCL
    if(opt.isSet("--devices")) {
        cout << endl;
        Util::listOpenCLDevices();
        return 0;
    }
    if(opt.isSet("--platformId") != opt.isSet("--deviceId")) {
        cout << "You must either specify both platformId and deviceId, or specify neither" << endl;
        return 0;
    }
    if(opt.isSet("--platformId")) {
        string pid;
        opt.get("--platformId")->getString(pid);
        properties["OpenCLPlatformIndex"] = pid;
    }
    if(opt.isSet("--deviceId")) {
        string did;
        opt.get("--deviceId")->getString(did);
        if(did.find(",") != string::npos) {
            cout << "Using multiple GPUs to run the same simulation is not currently supported" << endl;
            return 0;
        };
        properties["OpenCLDeviceIndex"] = did;
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

    double delay_in_sec = 1;
    ExitSignal::init();
    const string engine = "openmm";
    OpenMMCore core(engine, "70ac3a36-6921-4ddb-997d-6b76f2fa7341", properties);
    while(!ExitSignal::shouldExit()) {
        try {
            sleep(delay_in_sec);
            delay_in_sec = delay_in_sec * 2;
            core.startStream(cc_uri, donor_token, target_id);
            delay_in_sec = 1;
            core.main();
        } catch(const exception &e) {
            cout << e.what() << endl;
        }
    }
}
