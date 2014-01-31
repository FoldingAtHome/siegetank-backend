#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SSLException.h>
#include <Poco/Net/X509Certificate.h>
#include <Poco/UnicodeConverter.h>
#include <Poco/Util/Application.h>
#include <Poco/StreamCopier.h>
#include <Poco/Dynamic/Var.h>

#include <Poco/Base64Decoder.h>
#include <Poco/Base64Encoder.h>
#include <Poco/InflatingStream.h>
#include <Poco/DeflatingStream.h>

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/x509.h>

#include <fstream>
#include <string>
#include <streambuf>
#include <sstream>
#include <stdexcept>
#include <algorithm>

#include <OpenMM.h>

#include "XTCWriter.h"



