#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/StreamCopier.h>
#include <Poco/Exception.h>
#include <Poco/Net/SSLManager.h>

#include <iostream>

using namespace std;


int main() {
    try {
        Poco::Net::Context::Ptr ctxt = new Poco::Net::Context(
            Poco::Net::Context::CLIENT_USE, "", Poco::Net::Context::VERIFY_STRICT,
            9, true);
        Poco::URI uri("https://www.google.com");
        Poco::Net::HTTPSClientSession session(uri.getHost(),
                                              uri.getPort(), ctxt);
        Poco::Net::HTTPRequest request("GET", "/");
        session.sendRequest(request);
        Poco::Net::HTTPResponse response;
        istream &content_stream = session.receiveResponse(response);
    } catch(Poco::Exception &e) {
        cout << "Exception caught" << endl;
        cout << e.message() << endl;
        throw;
    }
}