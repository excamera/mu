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
    // prepare requests
    cerr << "Building requests... ";
    string fn_name = safe_getenv("LAMBDA_FUNCTION");
    string secret = safe_getenv("AWS_SECRET_ACCESS_KEY");
    string akid = safe_getenv("AWS_ACCESS_KEY_ID");
    string ipaddr = safe_getenv("UDP_PING_ADDR");
    string ipport = safe_getenv("UDP_PING_PORT");
    vector<HTTPRequest> request;
    for (int i = 0; i < nlaunch; i++) {
        string payload = "{\"id\":\"" + to_string(i) + "\",\"addr\":\"" + ipaddr + "\",\"port\":\"" + ipport + "\"}";
        LambdaInvocation ll(secret, akid, fn_name, payload, "", LambdaInvocation::InvocationType::Event);
        request.emplace_back(move(ll.to_http_request()));
    }
    cerr << "done.\n";

    // open connections to server
    cerr << "Opening sockets... ";
    vector<SecureSocket> www;
    Address server {"lambda.us-east-1.amazonaws.com", "https"};
    SSLContext ctx;
    for (int i = 0; i < nlaunch; i++) {
        TCPSocket sock;
        sock.set_blocking(false);
        try {
            sock.connect( server );
        } catch (const exception &e) {
            // it's OK, we expected this
        }
        www.emplace_back(move(ctx.new_secure_socket(move(sock))));
    }

    // let all the connections happen
    // yes this is ugly
    {
        bool all_done = false;
        vector<bool> sfin (nlaunch, false);
        while (! all_done) {
            bool found = false;
            for (int i = 0; i < nlaunch; i++) {
                if (! sfin[i]) {
                    try {
                        www[i].connect();
                    } catch (const exception &e) {
                        found = true;
                        continue;
                    }
                    sfin[i] = true;
                }
            }

            if (!found) {
                all_done = true;
            } else {
                usleep(10000);
            }
        }
    }
    cerr << "done.\n";

    // send requests
    cerr << "Sending requests... ";
    for (int i = 0; i < nlaunch; i++) {
        www[i].set_blocking(true);
        www[i].write(request[i].str());
    }
    cerr << "done.\n";

    // parse responses
    HTTPResponseParser parser;
    for (int i = 0; i < nlaunch; i++) {
        parser.new_request_arrived(request[i]);

        while (! www[i].eof()) {
            parser.parse( www[i].read() );

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
                www[i].close();
                break;
            }
        }
    }
}
