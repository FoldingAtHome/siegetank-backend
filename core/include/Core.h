#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Util/Application.h>
#include <Poco/URI.h>

#include <string>
#include <map>

class Core {

public:

    // frame_send_interval is in number of frames written
    // int checkpoint_send_interval is in number of times per day (user config)
    Core(int frame_send_interval, int checkpoint_send_interval);

    ~Core();

    /* Get an assignment from the command center, and initializes _session so
    we can start the stream the from the workserver */
    void initialize_session(Poco::URI &cc_uri);

    /* Start the stream and fetch files. If the files end in .gz or .gz.b64
    then the suffixes and stripped, and the contents are processed for you. */
    void start_stream(std::string &stream_id, std::string &target_id,
                      std::map<std::string, std::string> &target_files,
                      std::map<std::string, std::string> &stream_files) const;

    /* Send frame files to the WS. This method automatically base64 encodes
       the file */
    void send_frame_files(const std::map<std::string, std::string> &files, bool gzip=false) const;

    /* Send checkpoint files to the WS. This method automatically base64
       encodes the file */
    void send_checkpoint_files(const std::map<std::string, std::string> &files, bool gzip=false) const;

    /* Disengage the core from the stream */
    void stop_stream() const;

    /* Send a heartbeat */
    void send_heartbeat(string error_msg = "");

    /* Main MD loop */
    virtual void main();

protected:

    Poco::Net::HTTPSClientSession* _session;

private:

    void _send_files_to_uri(const std::string &path, 
        const std::map<std::string, std::string> &files, bool gzip) const;

    Poco::URI _ws_uri;

    // every request must be validated by the authorization token
    std::string _auth_token;

    const int _frame_send_interval;
    const int _checkpoint_send_interval;

    // number of steps we take before writing out a frame
    int _frame_write_interval;

    /* returns True if we should checkpoint */  
    bool checkpoint() const;

    /* returns True if the app should exit. Typically one of two things happen
    in the case of a checkpoint:

    1. An older checkpoint is used
    2. If the frame_write_interval is small enough, then the next frame write
       is used. */
    bool exit() const;

    /* returns True if we should write a frame */
    bool frame() const;

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