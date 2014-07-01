// Authors: Yutong Zhao <proteneer@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

#ifndef CORE_H_
#define CORE_H_

#include <Poco/Net/HTTPSClientSession.h>

#include <string>
#include <map>
#include <ostream>
#include <sstream>
#include <iostream>

#include "picojson.h"

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
    Core(std::string core_key, std::ostream& log = std::cout);

    ~Core();

    /* Main MD loop */
    virtual void main();
  
    std::ostream &logStream;

protected:
    /* Start the stream and fetch files. options is a JSON string. */
    virtual void startStream(const std::string &cc_uri,
                             const std::string &donor_token = "",
                             const std::string &target_id = "",
                             const std::string &proxy_string = "");

    /* Disengage the core from the stream and destroy the session */
    virtual void stopStream(std::string error_msg = "");

    /* Send frame files to the WS.  This method automatically base64
       encodes the files, and adds '.b64' to the suffix. If 'gzip' is true, the 
       files will first be gzipped, with a '.gz' suffix appended, and then b64
       encoded.
    */
    void sendFrame(const std::map<std::string, std::string> &files,
                   int frame_count=1, bool gzip=false) const;

    /* Send checkpoint files to the WS. This method automatically base64
       encodes the files, and adds '.b64' to the suffix. If 'gzip' is true, the 
       files will first be gzipped, with a '.gz' suffix appended, and then b64
       encoded.
    */
    void sendCheckpoint(const std::map<std::string, std::string> &files, double frames,
                        bool gzip=false) const;

    /* Send a heartbeat */
    void sendHeartbeat() const;

    /* get a specific option */
    template<typename T>
    T getOption(const std::string &key) const {
        std::stringstream ss(options_);
        picojson::value value; ss >> value;
        picojson::value::object &object = value.get<picojson::object>();
        return object[key].get<T>();
    }

    std::map<std::string, std::string> files_;
    std::string target_id_;
    std::string stream_id_;

private:
    std::string core_token_;
    std::string options_;

    Poco::Net::HTTPSClientSession* session_;
    const std::string core_key_;
    void assign(const std::string &cc_host,
                const std::string &donor_token,
                const std::string &target_id,
                const std::string &proxy = "");

};

#endif