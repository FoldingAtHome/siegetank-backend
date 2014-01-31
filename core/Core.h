#include <string>
#include <Poco/Util/Application.h>
#include <OpenMM.h>

class Core: public Application {

public:

    Core();
    ~Core();

    /* return a JSON encoded string containing all the files needed
    for the job. The gzipped data is encoded in the fields "stream_files" and 
    "target_files". */
    string get_job();

    /* send a single frame and the associated files to the WS */
    virtual void send_frame();

    /* send a checkpoint and associated files to the WS */
    virtual void send_checkpoint();

    /* send a heartbeat */
    void heartbeat();

    /* stop the stream */
    void stop_stream();

    /* main io_loop */
    void main();

protected:

    int _steps_per_frame;

    Poco::Net::HTTPSClientSession* _cc_session;
    Poco::Net::HTTPSClientSession* _ws_session;

    OpenMM::Context* _ref_context;
    OpenMM::Context* _core_context;

private:

    /* returns True if we should checkpoint */  
    bool checkpoint();

    /* returns True if the app should exit. Typically, a checkpoint is sent
    and the program exits. */
    bool exit();

    /* returns True if we should write a frame */
    bool frame();


};

/* General usage:

Core::main() {

    for(int i=0; i < 1500; i++) {

        if(frame()) {
            Core->send_frame();
        }

        if(exit()) {
            Core->send_checkpoint();
            return 0;
        } else if(checkpoint()) {
            Core->send_checkpoint();
        }


        Core->step();
    }

}
*/