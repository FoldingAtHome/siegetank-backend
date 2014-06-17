#include "gpuinfo.h"
#include <map>
#include <string>
#include <iostream>

using namespace std;

#ifdef OPENMM_OPENCL
#ifdef __APPLE__
#include <OpenCL/cl.h>
#else
#include <CL/cl.h>
#endif
void Util::listOpenCLDevices() {
    cl_platform_id platforms[100];
    cl_uint platforms_n = 0;
    clGetPlatformIDs(100, platforms, &platforms_n);
    if (platforms_n == 0) {
        cout << "No OpenCL Compatible Devices Found" << endl;
        return;
    }
    cout << "OpenCL compatible devices: " << endl;
    for(int j=0; j<platforms_n; j++) {
        cl_device_id devices[100];
        cl_uint devices_n = 0;
        clGetDeviceIDs(platforms[j], CL_DEVICE_TYPE_ALL, 100, devices, &devices_n);
        for (int i=0; i<devices_n; i++) {
            char buffer[10240];
            clGetDeviceInfo(devices[i], CL_DEVICE_NAME, sizeof(buffer), buffer, NULL);
            string deviceName(buffer);
            cout << "name: " << deviceName << " | platformId: " << j << " deviceId: " << i << endl;
        }
    }
}
#elif OPENMM_CUDA
#include <cuda.h>
void Util::listCUDADevices() {
    CUresult errorMsg = cuInit(0);
    if(errorMsg != CUDA_SUCCESS) {
        cout << "CUDA ERROR: cannot initialize CUDA." << endl;
        return;
    }
    int numDevices = 0;
    errorMsg = cuDeviceGetCount(&numDevices);
    if(errorMsg != CUDA_SUCCESS) {
        cout << "CUDA ERROR: cannot get number of devices." << endl;
        return;
    }
    if(numDevices == 0) {
        cout << "No CUDA Compatible Devices Found" << endl;
        return;
    }
    map<string, int> deviceMap;
    for(int i=0; i<numDevices;i++) {
        CUdevice device;
        cuDeviceGet(&device, i);
        char name[500];
        cuDeviceGetName(name, 500, device);
        cout << "name: " << name << " | deviceId: " << i << endl;
    }
}
#endif
