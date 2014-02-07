#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Util/Application.h>
#include <Poco/URI.h>

#include <string>
#include <map>
#include <ostream>

class Core {

public:

    // frame_send_interval is in number of frames written
    // int checkpoint_send_interval is in number of times per day (user config)
    Core(int checkpoint_send_interval,
         std::string engine,
         std::string engine_version);

    ~Core();

    /* Main MD loop */
    virtual void main();

    /* Start the stream and fetch files. If the files end in .gz or .gz.b64
    then the suffixes and stripped, and the contents are processed for you. The
    method also initializes the _frame_write_interval.
    
    The workserver will automatically gunzip and decode files as needed.
    */
    void start_stream(const Poco::URI &cc_uri,
                      std::string &stream_id, std::string &target_id,
                      std::map<std::string, std::string> &target_files,
                      std::map<std::string, std::string> &stream_files);

    /* Send frame files to the WS. This method automatically base64 encodes
       the file */
    void send_frame_files(const std::map<std::string, std::string> &files, int frame_count = 1, bool gzip=false) const;

    /* Send checkpoint files to the WS. This method automatically base64
       encodes the files, and adds '.b64' to the suffix. If gzip is true, the 
       files will first be gzipped, with a '.gz' suffix appendde, and then b64
       encoded. Note that the workserver does not automatically gunzip files
       on the WS side, but it will automatically decode base64.

       ex: if gzip == true:
            'state.xml' -> 'state.xml.gz.b64'
           else:
            'state.xml' -> 'state.xml.b64'
    */
    void send_checkpoint_files(const std::map<std::string, std::string> &files, bool gzip=false) const;

    /* Disengage the core from the stream and destroys the session */
    void stop_stream(std::string error_msg = "");

    /* Send a heartbeat */
    void send_heartbeat() const;

    /* Returns true if the core should exit */
    bool exit() const;

    int get_frame_send_interval() const;

    int get_frame_write_interval() const;

    int get_checkpoint_send_interval() const;

    /* return true if current time > _next_checkpoint_time, and sets
    _next_checkpoint_time = current_time + _check_point_send_interval
    */
    bool should_checkpoint();

protected:

    /* how often we send frames in steps */
    int _frame_send_interval;

    /* number of steps we take before writing out a frame */
    int _frame_write_interval;

    /* how often we send checkpoints in seconds */
    const int _checkpoint_send_interval;

    int _next_checkpoint_time;

    std::ostream& _logstream;

private:

    /* Get an assignment from the command center, and initializes _session so
    we can start the stream the from the workserver */
    void _initialize_session(const Poco::URI &cc_uri);

    Poco::Net::HTTPSClientSession* _session;

    void _send_files_to_uri(const std::string &path, 
        const std::map<std::string, std::string> &files, bool gzip) const;

    Poco::URI _ws_uri;

    /* every request must be validated by the authorization token */
    std::string _auth_token;

    const std::string _engine;
    const std::string _engine_version;

};

/* Example implementation of main():

void OpenMMCore::main() {
    core.start_stream(&files);
    int steps = 0;
    integrator->step();
    steps++;
    while(true) {
        if(exit()) {
            send_checkpoint();
            break;
        }
        if(user_checkpoint()) {
            send_checkpoint();
        }
        if(steps % _checkpoint_send_interval == 0) {
            send_checkpoint(); 
        }
        if(steps % _frame_write_interval == 0) {
            // write frame
        }
        if(steps % _frame_send_interval == 0) {
            send_frames();
        }


        
    }
    core.stop_stream();
}

int main() {
    OpenMMCore core(15,25);
    while(true && !core.exit()) {
        core.main();
    }
}

*/
