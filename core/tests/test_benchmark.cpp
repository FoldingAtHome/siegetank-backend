#include "CPUBenchmark.h"
#include "OpenCLBenchmark.h"
#include <iostream>
#include <stdexcept>
#include <math.h>

using namespace std;

void testEquivalence() {
    vector<complex<float> > cpuResult = CPUBenchmark().value();
    vector<complex<float> > oclResult = OpenCLBenchmark(0, 0).value();
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
    CPUBenchmark().speed();
}

void testOpenCLBenchmarkSpeed() {
    CPUBenchmark().speed();
}

int main() {
    testEquivalence();
    testCPUBenchmarkSpeed();
    testOpenCLBenchmarkSpeed();
}