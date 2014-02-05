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

    /* Main MD loop */
    virtual void main();

    /* Start the stream and fetch files. If the files end in .gz or .gz.b64
    then the suffixes and stripped, and the contents are processed for you. */
    void start_stream(const Poco::URI &cc_uri,
                      std::string &stream_id, std::string &target_id,
                      std::map<std::string, std::string> &target_files,
                      std::map<std::string, std::string> &stream_files);

    /* Send frame files to the WS. This method automatically base64 encodes
       the file */
    void send_frame_files(const std::map<std::string, std::string> &files, bool gzip=false) const;

    /* Send checkpoint files to the WS. This method automatically base64
       encodes the file */
    void send_checkpoint_files(const std::map<std::string, std::string> &files, bool gzip=false) const;

    /* Disengage the core from the stream and destroys the session */
    void stop_stream(std::string error_msg = "");

    /* Send a heartbeat */
    void send_heartbeat() const;

protected:

    /* how often we send frames in steps */
    const int _frame_send_interval;

    /* how often we send checkpoints in steps */
    const int _checkpoint_send_interval;

    // number of steps we take before writing out a frame
    int _frame_write_interval;

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

};

/* General usage:

void OpenMMCore::main() {
    core.start_stream();
    
    while(true && !core.exit()) {
        
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
