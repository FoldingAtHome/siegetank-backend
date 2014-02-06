// OS X SSL instructions:
// ./Configure darwin64-x86_64-cc -noshared
// g++ -I ~/poco/include Core.cpp ~/poco/lib/libPoco* /usr/local/ssl/lib/lib*
// Linux:
// g++ -I/home/yutong/openmm_install/include -I/usr/local/ssl/include -I/home/yutong/poco152_install/include -L/home/yutong/poco152_install/lib -L/usr/local/ssl/lib/ Core.cpp -lpthread -lPocoNetSSL -lPocoCrypto -lssl -lcrypto -lPocoUtil -lPocoJSON -ldl -lPocoXML -lPocoNet -lPocoFoundation -L/home/yutong/openmm_install/lib -L/home/yutong/openmm_install/lib/plugins -lOpenMMOpenCL_static /usr/lib/nvidia-current/libOpenCL.so -lOpenMMCUDA_static /usr/lib/nvidia-current/libcuda.so /usr/local/cuda/lib64/libcufft.so -lOpenMMCPU_static -lOpenMMPME_static -L/home/yutong/fftw_install/lib/ -lfftw3f -lfftw3f_threads -lOpenMM_static; ./a.out 

// Linux:
// g++ -I/Users/yutongzhao/openmm_install/include -I/usr/local/ssl/include -I/users/yutongzhao/poco152_install/include -L/users/yutongzhao/poco152_install/lib -L/usr/local/ssl/lib/ Core.cpp -lpthread -lPocoNetSSL -lPocoCrypto -lssl -lcrypto -lPocoUtil -lPocoJSON -ldl -lPocoXML -lPocoNet -lPocoFoundation -L/Users/yutongzhao/openmm_install/lib -lOpenMMCPU_static -L/Users/yutongzhao/openmm_install/lib/plugins -lOpenMM_static; ./a.out 

//  ./configure --static --prefix=/home/yutong/poco152_install --omit=Data/MySQL,Data/ODBC

#include <iostream>

#include <OpenMM.h>
#include "XTCWriter.h"
#include "OpenMMCore.h"
#include "kbhit.h"

using namespace std;
using namespace Poco;

extern "C" void registerSerializationProxies();
extern "C" void registerCpuPlatform();
extern "C" void registerOpenCLPlatform();

OpenMMCore::OpenMMCore(int frame_send_interval, int checkpoint_send_interval):
    _core_context(NULL),
    _ref_context(NULL),
    Core(frame_send_interval, checkpoint_send_interval, "openmm", "6.0") {

}

void OpenMMCore::main() {
    Poco::URI uri("https://127.0.0.1:8980/core/assign");
    string stream_id;
    string target_id;
    map<string, string> target_files;
    map<string, string> stream_files;

    try {
        start_stream(uri, stream_id, target_id, target_files, stream_files);
        // step();
        for(int i=1; i < 100000; i++) {
            if(exit()) {
                // send checkpoint
                break;
            }
            if(kbhit()) {
                // handle keyboard events
                // c sends the previous checkpoints 
                // n sends the next available checkpoint
                cout << "keyboard event: " << char(getchar()) << endl;
            }
            if(i % _frame_write_interval == 0) {
                // write frame
            }
            if(i % _frame_send_interval == 0) {
                // get state
                // write checkpoint
                // send frame
            }
            if(i % _checkpoint_send_interval == 0) {

            }
        }

    } catch(exception &e) {
        cout << e.what() << endl;
    }
    stop_stream();
}