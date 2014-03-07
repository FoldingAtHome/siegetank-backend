#include "OpenCLBenchmark.h"
#include <iostream>
#include <sys/time.h>
#include <stdexcept>
#include <iostream>

#include <unistd.h>

OpenCLBenchmark::OpenCLBenchmark(cl_device_id deviceId,
                                 cl_platform_id platformId) {
    cl_int err;
    cl_context_properties props[3] = { CL_CONTEXT_PLATFORM, 0, 0 };





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
        host_out = 0;
        if(i % 2 == 0)
            host_in[i] = 1;
        else
            host_in[i] = 2;
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

    std::cout << "FOO" << std::endl;

    /* Create a default plan for a complex FFT. */
    err = clfftCreateDefaultPlan(&planHandle, ctx, dim, clLengths);

    std::cout << "BAR" << std::endl;

    /* Set plan parameters. */
    err = clfftSetPlanPrecision(planHandle, CLFFT_SINGLE);
    err = clfftSetLayout(planHandle, CLFFT_COMPLEX_INTERLEAVED,
                         CLFFT_COMPLEX_INTERLEAVED);
    err = clfftSetResultLocation(planHandle, CLFFT_OUTOFPLACE);

    std::cout << "VAR" << std::endl;


    /* Bake the plan. */
    err = clfftBakePlan(planHandle, 1, &queue, NULL, NULL);

    std::cout << "ZAR" << std::endl;


}

double OpenCLBenchmark::speed() {
    cl_int err;
    timeval start;
    gettimeofday(&start, NULL);
    const int iterations = 7000;
    for(int i=0; i < iterations; i++) {
        err = clfftEnqueueTransform(planHandle, CLFFT_FORWARD, 1, &queue, 0,
                                    NULL, NULL, &device_in, &device_out, NULL);
    }
    err = clFinish(queue);
    timeval end;
    gettimeofday(&end, NULL);
    double diff_sec = (end.tv_sec+end.tv_usec/1e6) - 
                      (start.tv_sec+start.tv_usec/1e6);
    return iterations/diff_sec;
}

OpenCLBenchmark::~OpenCLBenchmark() {
    clReleaseMemObject(device_in);
    clReleaseMemObject(device_out);

    free(host_in);
    free(host_out);

    cl_int err = clfftDestroyPlan(&planHandle);

    clfftTeardown();

    clReleaseCommandQueue( queue );
    clReleaseContext( ctx );
}

int main() {
    cl_platform_id platform = 0;
    cl_device_id device = 0;
    cl_int err = 0;
    err = clGetPlatformIDs( 1, &platform, NULL );
    err = clGetDeviceIDs( platform, CL_DEVICE_TYPE_GPU, 1, &device, NULL );


    std::cout << platform << " " << device << std::endl;



    std::cout << "speed: " << OpenCLBenchmark(device, platform).speed() << std::endl;
}