#ifndef CORE_H_
#define CORE_H_

#include <Poco/Net/HTTPSClientSession.h>

#include <string>
#include <map>
#include <ostream>
#include <sstream>


/**
 * A Core provides the basic interface for talking to the Siegetank Backend.
 *
 * The core contains basic functionality such as starting a stream, stopping a
 * stream, sending frames, checkpoints, and heartbeats.
 *
 */

class Core {
public:
    // checkpoint_send_interval is in number of times per day (user config)
    Core(std::string engine, std::string core_key = "");

    ~Core();

    /* Main MD loop */
    virtual void main();

    /* Start the stream and fetch files. options is a JSON string. */
    void startStream(const std::string &cc_uri,
                     const std::string &donor_token,
                     std::map<std::string, std::string> &files,
                     std::string &options);

    /* Disengage the core from the stream and destroy the session */
    void stopStream(std::string error_msg = "");

    /* Send frame files to the WS. This method automatically base64 encodes
       the file. */
    void sendFrame(const std::map<std::string, std::string> &files,
                   int frame_count=1, bool gzip=false) const;

    /* Send checkpoint files to the WS. This method automatically base64
       encodes the files, and adds '.b64' to the suffix. If 'gzip' is true, the 
       files will first be gzipped, with a '.gz' suffix appended, and then b64
       encoded.

       ex: if gzip == true:
            'state.xml' -> 'state.xml.gz.b64'
           else:
            'state.xml' -> 'state.xml.b64'
    */
    void sendCheckpoint(const std::map<std::string, std::string> &files,
                        bool gzip=false) const;

    /* Send a heartbeat */
    void sendHeartbeat() const;

    /* Get the target id */
    std::string getTargetId() const;

    /* Get the stream id */
    std::string getStreamId() const;

private:
    std::string target_id_;
    std::string stream_id_;
    std::string core_token_;
    Poco::Net::HTTPSClientSession* session_;
    const std::string engine_;
    const std::string core_key_;
    void assign(const std::string &cc_host, const std::string &donor_token);

};

#endif