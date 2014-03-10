#include "OpenCLBenchmark.h"
#include <iostream>
#include <sys/time.h>
#include <stdexcept>
#include <unistd.h>
#include <stdexcept>
#include <vector>
#include <complex>

using namespace std;

OpenCLBenchmark::OpenCLBenchmark(int platformIndex, int deviceIndex) :
    host_in(NULL),
    host_out(NULL) {

    cl_int err;
    cl_context_properties props[3] = { CL_CONTEXT_PLATFORM, 0, 0 };

    const int MAX_PLATFORMS = 10;
    const int MAX_DEVICES = 10;

    cl_platform_id platforms[MAX_PLATFORMS];
    cl_uint platforms_n = 0;
    cl_device_id devices[MAX_DEVICES];
    cl_uint devices_n = 0;
    
    clGetPlatformIDs(MAX_PLATFORMS, platforms, &platforms_n);

    if(platformIndex > platforms_n) {
        throw std::runtime_error("platformIndex < platforms_n");
    }

    cl_platform_id platformId = platforms[platformIndex];

    clGetDeviceIDs(platforms[platformIndex], CL_DEVICE_TYPE_ALL, MAX_DEVICES,
                   devices, &devices_n);
    
    if(deviceIndex > devices_n) {
        throw std::runtime_error("deviceIndex < devices_n");
    }

    cl_device_id deviceId = devices[deviceIndex];

    /* Initialize */
    clfftDim dim = CLFFT_1D;
    size_t clLengths[1] = {FFTW_SIZE};
    props[1] = (cl_context_properties)platformId;
    ctx = clCreateContext(props, 1, &deviceId, NULL, NULL, &err);
    queue = clCreateCommandQueue(ctx, deviceId, 0, &err);
    clfftSetupData fftSetup;
    err = clfftInitSetupData(&fftSetup);
    err = clfftSetup(&fftSetup);
    host_in = (float *)malloc(FFTW_SIZE*2*sizeof(*host_in));
    host_out = (float *)malloc(FFTW_SIZE*2*sizeof(*host_in));
    for(int i=0;i < FFTW_SIZE*2; i++) {
        host_out[i] = 0;
        if(i % 2 == 0)
            host_in[i] = 0.1;
        else
            host_in[i] = -0.2;
    }
    /* Prepare OpenCL memory objects and place data inside them. */
    device_in = clCreateBuffer(ctx, CL_MEM_READ_WRITE,
                               FFTW_SIZE*2*sizeof(*host_in), NULL, &err);
    err = clEnqueueWriteBuffer(queue, device_in, CL_TRUE, 0,
                               FFTW_SIZE*2*sizeof(*host_in), host_in,
                               0, NULL, NULL);
    device_out = clCreateBuffer(ctx, CL_MEM_READ_WRITE,
                                FFTW_SIZE*2*sizeof(*host_out), NULL, &err);
    err = clEnqueueWriteBuffer(queue, device_out, CL_TRUE, 0,
                               FFTW_SIZE*2*sizeof(*host_out), host_out,
                               0, NULL, NULL);
    /* Create a default plan for a complex FFT. */
    err = clfftCreateDefaultPlan(&planHandle, ctx, dim, clLengths);
    /* Set plan parameters. */
    err = clfftSetPlanPrecision(planHandle, CLFFT_SINGLE);
    err = clfftSetLayout(planHandle, CLFFT_COMPLEX_INTERLEAVED,
                         CLFFT_COMPLEX_INTERLEAVED);
    err = clfftSetResultLocation(planHandle, CLFFT_OUTOFPLACE);
    /* Bake the plan. */
    err = clfftBakePlan(planHandle, 1, &queue, NULL, NULL);
}

double OpenCLBenchmark::speed() {
    cl_int err;
    timeval start;
    gettimeofday(&start, NULL);
    const int iterations = 17;
    for(int i=0; i < iterations; i++) {
        err = clfftEnqueueTransform(planHandle, CLFFT_FORWARD, 1,
                                    &queue, 0, NULL, NULL, &device_in,
                                    &device_out, NULL);
    }
    err = clFinish(queue);
    timeval end;
    gettimeofday(&end, NULL);
    double diff_sec = (end.tv_sec+end.tv_usec/1e6) - 
                      (start.tv_sec+start.tv_usec/1e6);
    return iterations/diff_sec;
}

std::vector<std::complex<float> > OpenCLBenchmark::value() {
    cl_int err;
    err = clfftEnqueueTransform(planHandle, CLFFT_FORWARD, 1,
                                &queue, 0, NULL, NULL, &device_in,
                                &device_out, NULL);
    clFinish(queue);
    err = clEnqueueReadBuffer(queue, device_out, CL_TRUE, 0, 
                              FFTW_SIZE*2*sizeof(*host_out), host_out, 0,
                              NULL, NULL);
    vector<complex<float> > result(FFTW_SIZE);
    for(int i=0; i < result.size(); i++) {
        result[i] = complex<float>(host_out[2*i], host_out[2*i+1]);
    }
    return result;
}

OpenCLBenchmark::~OpenCLBenchmark() {
    clReleaseMemObject(device_in);
    clReleaseMemObject(device_out);
    free(host_in);
    if(host_out != NULL)
        free(host_out);
    cl_int err = clfftDestroyPlan(&planHandle);
    clfftTeardown();
    clReleaseCommandQueue( queue );
    clReleaseContext( ctx );
}