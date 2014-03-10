#ifndef CPUBenchmark_H_
#define CPUBenchmark_H_

#include "Benchmark.h"
#include <fftw3.h>

class CPUBenchmark : public Benchmark {

public:

    CPUBenchmark();

    double speed();

    std::vector<std::complex<float> > value();

    ~CPUBenchmark();

private:

    fftwf_complex *in, *out;
    fftwf_plan plan;

};

#endif