#ifndef OpenCLBenchmark_H_
#define OpenCLBenchmark_H_

#include "Benchmark.h"
#include <clFFT.h>

class OpenCLBenchmark : public Benchmark {

public:

    OpenCLBenchmark(int platformIndex, int deviceIndex);

    double speed();

    std::vector<std::complex<float> > value();

    ~OpenCLBenchmark();

private:

    clfftPlanHandle planHandle;
    cl_command_queue queue;
    cl_context ctx;

    float *host_in;
    cl_mem device_in;
    float *host_out;
    cl_mem device_out;

};

#endif