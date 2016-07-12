/* -*-mode:c++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <unistd.h>

#include "socket.hh"
#include "secure_socket.hh"
#include "exception.hh"
#include "http_request.hh"
#include "http_response_parser.hh"
#include "lambda_request.hh"
#include "getenv.hh"

using namespace std;

void launchser(int nlaunch);

int main(int argc, char **argv)
{
    int nlaunch = 0;
    if (argc > 1) {
        nlaunch = atoi(argv[1]);
    }
    if (nlaunch < 1) {
        nlaunch = 1;
    }

    try {
        launchser(nlaunch);
    } catch ( const exception & e ) {
        print_exception( e );
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

void launchser(int nlaunch) {
    /* open connection to server */
    TCPSocket sock;
    Address server {"lambda.us-east-1.amazonaws.com", "https"};
    cerr << "Connecting to " << server.str() << "... ";
    sock.connect( server );
    cerr << "done.\n";

    SSLContext ctx;
    cerr << "Setting up SSL connection... ";
    SecureSocket www = ctx.new_secure_socket(move(sock));
    www.connect();
    cerr << "done.\n";

    /* prepare requests */
    string fn_name = safe_getenv("LAMBDA_FUNCTION");
    string secret = safe_getenv("AWS_SECRET_ACCESS_KEY");
    string akid = safe_getenv("AWS_ACCESS_KEY_ID");
    string ipaddr = safe_getenv("UDP_PING_ADDR");
    string ipport = safe_getenv("UDP_PING_PORT");

    /* send nlaunch requests */
    HTTPResponseParser parser;
    for (int i = 0; i < nlaunch; i++) {
        string payload = "{\"id\":\"" + to_string(i) + "\",\"addr\":\"" + ipaddr + "\",\"port\":\"" + ipport + "\"}";
        LambdaInvocation ll(secret, akid, fn_name, payload, "", LambdaInvocation::InvocationType::Event);
        HTTPRequest request = ll.to_http_request();
        www.write(request.str());

        parser.new_request_arrived(request);
        while (! www.eof()) {
            parser.parse( www.read() );
            if ( not parser.empty() ) {
                cerr << "Got reply.\n";
                const HTTPResponse & response = parser.front();
                for ( unsigned int i = 0; i < response.headers().size(); i++ ) {
                    cout << "header # " << i << ": key={" << response.headers().at( i ).key()
                        << "}, value={" << response.headers().at( i ).value() << "}\n";
                }

                cout << "Body (" << response.body().size() << " bytes) follows:\n";
                cout << response.body();
                cout << "\n[end of body]\n";

                // done with this one
                parser.pop();
                break;
            }
        }
    }
    cerr << "done sending requests.\n";

    www.close();
}
