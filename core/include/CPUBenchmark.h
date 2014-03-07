#include "Benchmark.h"
#include <fftw3.h>

class CPUBenchmark : public Benchmark {

public:

    CPUBenchmark();

    double speed();

    ~CPUBenchmark();

private:

    fftwf_complex *in, *out;
    fftwf_plan plan;

};