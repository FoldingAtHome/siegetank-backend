#ifndef Benchmark_H_
#define Benchmark_H_

#include <complex>
#include <vector>

class Benchmark {

public:

    Benchmark(int fftw_size) : fftw_size(fftw_size), average(0), average_n(0) {};

    virtual ~Benchmark() {};

    // return the speed
    virtual double speed() = 0;

    // return the average speed
    double averageSpeed() const { return average; };

    // get the value of the fft
    virtual std::vector<std::complex<float> > value() = 0;

protected:

    static const int default_fftw_size = 8388608;

    const int fftw_size;

    int average_n;

    double average;

};

#endif