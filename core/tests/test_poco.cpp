#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/StreamCopier.h>

#include <iostream>

using namespace std;


int main() {

    try {
        cout << "A" << endl;
        Poco::URI uri("https://proteneer.stanford.edu");
        cout << uri.getHost() << " " << uri.getPort() << endl;
        Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort());
        cout << "b" << endl;
        Poco::Net::HTTPRequest request("GET", "/scvs/status");
        cout << "c" << endl;
        session.sendRequest(request);
        cout << "d" << endl;
        Poco::Net::HTTPResponse response;
        istream &content_stream = session.receiveResponse(response);
        cout << response.getStatus() << endl;
    } catch(exception &e) {
        cout << "FATAL:" << e.what() << endl;
    }
}