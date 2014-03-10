#ifndef Benchmark_H_
#define Benchmark_H_

#include <complex>
#include <vector>

//static const int FFTW_SIZE = 64;
static const int FFTW_SIZE = 8388608;
//static const int FFTW_SIZE = 262144;

class Benchmark {

public:

    Benchmark() {};

    virtual double speed() = 0;

    // get the value of the fft
    virtual std::vector<std::complex<float> > value() = 0;

};

#endif