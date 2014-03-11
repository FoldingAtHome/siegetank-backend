#include "CPUBenchmark.h"
#include "OpenCLBenchmark.h"
#include <iostream>
#include <stdexcept>
#include <math.h>
#include <stdlib.h>

using namespace std;

void testEquivalence(int platformId, int deviceId) {
    vector<complex<float> > cpuResult = CPUBenchmark().value();
    vector<complex<float> > oclResult = OpenCLBenchmark(platformId, deviceId).value();

    if(cpuResult.size() != oclResult.size()) {
        throw std::runtime_error("results differ in size");
    }
    for(int i=0; i < cpuResult.size(); i++) {
        complex<float> diff = cpuResult[i] - oclResult[i];
        if(fabs(diff.real()) > 1e-6 || fabs(diff.imag()) > 1e-6) {
            throw std::runtime_error("results differ");
        }
    }
}

void testCPUBenchmarkSpeed() {
    cout << "CPU Speed: " << CPUBenchmark().speed() << endl;
}

void testOpenCLBenchmarkSpeed(int platformId, int deviceId) {
    cout << "OpenCL Speed: " << OpenCLBenchmark(platformId, deviceId).speed() << endl;
}

int main(int argc, char **argv) {
    
    int platformId = 0;
    int deviceId = 0;

    if(argc == 3) {
        platformId = atoi(argv[1]);
        deviceId = atoi(argv[2]);
    }

    testCPUBenchmarkSpeed();
    testEquivalence(platformId, deviceId);
    testOpenCLBenchmarkSpeed(platformId, deviceId);
}