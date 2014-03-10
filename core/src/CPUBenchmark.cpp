#include "CPUBenchmark.h"
#include <iostream>
#include <sys/time.h>
#include <stdexcept>
#include <unistd.h>
#include <vector>
#include <complex>

using namespace std;

CPUBenchmark::CPUBenchmark() {
    in = (fftwf_complex*) fftwf_malloc(sizeof(fftwf_complex) * FFTW_SIZE);
    out = (fftwf_complex*) fftwf_malloc(sizeof(fftwf_complex) * FFTW_SIZE);
    int ret = fftwf_init_threads();
#ifndef _WIN32
    int numCPU = sysconf( _SC_NPROCESSORS_ONLN );
#endif
    fftwf_plan_with_nthreads(numCPU);
    plan = fftwf_plan_dft_1d(FFTW_SIZE, in, out, FFTW_FORWARD, FFTW_MEASURE);
    if(!ret)
        throw std::runtime_error("Could not initialize fftw");

    for(int i = 0; i < FFTW_SIZE; i++) {
        in[i][0] = 1;
        in[i][1] = 2;
    }
}

double CPUBenchmark::speed() {
    timeval start;
    gettimeofday(&start, NULL);
    const int iterations = 20;
    for(int i=0; i < iterations; i++) {
        fftwf_execute(plan);
    }
    timeval end;
    gettimeofday(&end, NULL);
    double diff_sec = (end.tv_sec+end.tv_usec/1e6) - 
                      (start.tv_sec+start.tv_usec/1e6);
    return iterations/diff_sec;
}

std::vector<std::complex<float> > CPUBenchmark::value() {
    vector<complex<float> > result(FFTW_SIZE);
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