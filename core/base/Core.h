#ifndef CORE_H_
#define CORE_H_

#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>

#include <string>
#include <map>
#include <ostream>
#include <sstream>

class Core {
public:

    // checkpoint_send_interval is in number of times per day (user config)
    Core(std::string engine, std::string core_token,
         int checkpoint_send_interval = 500);

    ~Core();

    /* Main MD loop */
    virtual void main();

    /* Start the stream and fetch files. options is a JSON string. */
    void startStream(const std::string &cc_uri,
                     std::map<std::string, std::string> &files,
                     std::string &options,
                     std::string &description);

    /* Disengage the core from the stream and destroy the session */
    void stopStream(std::string error_msg = "");

    /* Send frame files to the WS. This method automatically base64 encodes
       the file */
    void sendFrame(const std::map<std::string, std::string> &files,
                   int frame_count=1, bool gzip=false) const;

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
    void sendCheckpoint(const std::map<std::string, std::string> &files,
                        bool gzip=false) const;

    /* Send a heartbeat */
    void sendHeartbeat() const;

    /* Returns true if the core should exit */
    bool exit() const;

    /* return true if we should send checkpoint, it is assumed that the user
       will immediately checkpoint if True */
    bool shouldSendCheckpoint();

    /* return true if we should send heartbeat, it is assumed that the user
       will immediately checkpoint if True */
    bool shouldHeartbeat();

    /* set the donor_token */
    std::string donor_token;

    /* target_id */
    std::string target_id;

protected:

    /* how often we send frames in steps */
    int _frame_send_interval;

    /* number of steps we take before writing out a frame */
    int _frame_write_interval;

    std::ostringstream _logstring;

    /* where the log data is being piped to */
    std::ostream& _logstream;

    /* time the job started in seconds since epoch */
    const int _start_time;
    
    std::string _stream_id;

private:

    const string core_token;

    /* how often we send checkpoints in seconds */
    const int _checkpoint_send_interval;

    /* how often we send heartbeats in seconds */
    const int _heartbeat_interval;

    /* used by should_send_checkpoint() to see if we need to checkpoint */
    int _next_checkpoint_time;

    /* used by should_heartbeat() to see when we should send a heartbeat */
    int _next_heartbeat_time;

    /* Get an assignment from the command center, and initializes _session so
    we can start the stream the from the workserver */
    void initializeSession(const Poco::URI &cc_uri);

    Poco::Net::HTTPSClientSession* _session;

    Poco::URI _ws_uri;

    /* every request must be validated by the Authorization token */
    std::string _auth_token;

    const std::string _engine;
    const std::string _engine_version;

};

#endif