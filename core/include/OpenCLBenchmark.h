#include "Benchmark.h"
#include <clFFT.h>

class OpenCLBenchmark : public Benchmark {

public:

    OpenCLBenchmark(cl_device_id deviceId, cl_platform_id platformId);

    double speed();

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