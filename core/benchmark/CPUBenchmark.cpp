#include "CPUBenchmark.h"
#include <iostream>
#include <sys/time.h>
#include <stdexcept>
#include <unistd.h>
#include <vector>
#include <complex>
#include <stdlib.h>

using namespace std;

CPUBenchmark::CPUBenchmark(int fftw_size) : Benchmark(fftw_size) {
    in = (fftwf_complex*) fftwf_malloc(sizeof(fftwf_complex) * fftw_size);
    out = (fftwf_complex*) fftwf_malloc(sizeof(fftwf_complex) * fftw_size);
    int ret = fftwf_init_threads();
#ifndef _WIN32
    int numCPU = sysconf( _SC_NPROCESSORS_ONLN );
#endif
    fftwf_plan_with_nthreads(numCPU);
    plan = fftwf_plan_dft_1d(fftw_size, in, out, FFTW_FORWARD, FFTW_ESTIMATE);
    if(!ret)
        throw std::runtime_error("Could not initialize fftw");
    srand(1); // reset seed
    for(int i=0; i < fftw_size; i++) {
        in[i][0] = (float) rand()/RAND_MAX;
        in[i][1] = (float) rand()/RAND_MAX;
    }
}

double CPUBenchmark::speed() {
    timeval start;
    gettimeofday(&start, NULL);
    const int iterations = 2;
    for(int i=0; i < iterations; i++) {
        fftwf_execute(plan);
    }
    timeval end;
    gettimeofday(&end, NULL);
    double diff_sec = (end.tv_sec+end.tv_usec/1e6) - 
                      (start.tv_sec+start.tv_usec/1e6);
    double step_speed = iterations/diff_sec;
    average = (average*average_n+step_speed)/(average_n+1);
    average_n += 1;
    return average;
}

std::vector<std::complex<float> > CPUBenchmark::value() {
    vector<complex<float> > result(fftw_size);
    fftwf_execute(plan);
    for(int i=0; i < result.size(); i++) {
        result[i] = complex<float>(out[i][0], out[i][1]);
    }
    return result;
}

CPUBenchmark::~CPUBenchmark() {
    fftwf_destroy_plan(plan);
    fftwf_free(in);
    fftwf_free(out);
    fftwf_cleanup_threads();
}