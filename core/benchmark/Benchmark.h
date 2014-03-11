#ifndef Benchmark_H_
#define Benchmark_H_

#include <complex>
#include <vector>

class Benchmark {

public:

    Benchmark(int fftw_size) : fftw_size(fftw_size) {};

    virtual double speed() = 0;

    // get the value of the fft
    virtual std::vector<std::complex<float> > value() = 0;

protected:

    static const int default_fftw_size = 8388608;

    const int fftw_size;

};

#endif