#include "OpenMMCore.h"
#include "ezOptionParser.h"

#include <string>
#include <iostream>

using namespace std;

int main(int argc, const char * argv[]) {

    // parse options here

    ez::ezOptionParser opt;

    opt.overview = "Demo of pretty printing everything parsed.";
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
        "https://127.0.0.1:8980/core/assign", // Default.
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

    opt.parse(argc, argv);

    if (opt.isSet("-h")) {
        std::string usage;
        opt.getUsage(usage);
        std::cout << usage;
        return 1;
    }

    string cc_uri;
    opt.get("--cc")->getString(cc_uri);

    int checkpoint_frequency;
    opt.get("--checkpoint")->getInt(checkpoint_frequency);
    cout << cc_uri << " + " << checkpoint_frequency << endl;

    // finished parsing options
    OpenMMCore core(checkpoint_frequency);
    core.initialize(cc_uri);
    core.main();
}