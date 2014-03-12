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
        double tolerance = 1e-3*abs(cpuResult[i]);
        double error = fabs(abs(cpuResult[i])-abs(oclResult[i]));
        if(error > tolerance) {
            cout << "test, threshold: " << error << " " << tolerance << endl;
            cout << cpuResult[i] << endl;
            cout << oclResult[i] << endl;
            cout << i << " " << diff << endl;
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
    testOpenCLBenchmarkSpeed(platformId, deviceId);
    testEquivalence(platformId, deviceId);
}