#ifndef CPUBenchmark_H_
#define CPUBenchmark_H_

#include "Benchmark.h"
#include <fftw3.h>

class CPUBenchmark : public Benchmark {

public:

    CPUBenchmark(int fftw_size = Benchmark::default_fftw_size);

    double speed();

    std::vector<std::complex<float> > value();

    ~CPUBenchmark();

private:

    fftwf_complex *in, *out;
    fftwf_plan plan;

};

#endif