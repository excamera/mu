/* -*-mode:c++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <time.h>
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

static const vector<string> lambda_regions = { {"us-west-2"}        // Oregon
                                             , {"us-east-1"}        // Virginia
                                             , {"eu-west-1"}        // Ireland
                                             , {"eu-central-1"}     // Frankfurt
                                             , {"ap-northeast-1"}   // Tokyo
                                             , {"ap-southeast-2"}   // Sydney
                                             };

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
    vector<vector<HTTPRequest>> request;

    for (unsigned j = 0; j < lambda_regions.size(); j++) {
        request.emplace_back(vector<HTTPRequest>());
        for (int i = 0; i < nlaunch; i++) {
            string payload = "{\"id\":\"" + to_string(100000*j + i) + "\",\"addr\":\"" + ipaddr + "\",\"port\":\"" + ipport + "\"}";
            LambdaInvocation ll(secret, akid, fn_name, payload, "", LambdaInvocation::InvocationType::Event, lambda_regions[j]);
            request[j].emplace_back(move(ll.to_http_request()));
        }
    }
    cerr << "done.\n";

    // open connections to server
    cerr << "Opening sockets...";
    vector<vector<SecureSocket>> www;
    vector<vector<bool>> sfins;
    SSLContext ctx;
    for (unsigned j = 0; j < lambda_regions.size(); j++) {
        string servername = "lambda." + lambda_regions[j] + ".amazonaws.com";
        Address server {servername, "https"};
        www.emplace_back(vector<SecureSocket>());
        sfins.emplace_back(vector<bool> (nlaunch, false));
        for (int i = 0; i < nlaunch; i++) {
            TCPSocket sock;
            sock.set_blocking(false);
            try {
                sock.connect( server );
            } catch (const exception &e) {
                // it's OK, we expected this
            }
            SecureSocket this_www = ctx.new_secure_socket(move(sock));
            this_www.set_hostname(servername);
            www[j].emplace_back(move(this_www));
        }
    }

    // let all the connections happen
    // yes this is ugly
    {
        bool all_done = false;
        while (! all_done) {
            bool found = false;
            for (unsigned j = 0; j < lambda_regions.size(); j++) {
                for (int i = 0; i < nlaunch; i++) {
                    if (! sfins[j][i]) {
                        try {
                            www[j][i].connect();
                        } catch (const exception &e) {
                            found = true;
                            continue;
                        }
                        sfins[j][i] = true;
                    }
                }
            }

            if (!found) {
                all_done = true;
            } else {
                usleep(10000);
                cerr << ".";
            }
        }
    }
    cerr << "done.\n";

    // send requests
    {
        struct timespec start_time, stop_time;
        cerr << "Sending requests... ";
        clock_gettime(CLOCK_REALTIME, &start_time);
        for (int i = 0; i < nlaunch; i++) {
            for (unsigned j = 0; j < lambda_regions.size(); j++) {
                www[j][i].set_blocking(true);
                www[j][i].write(request[j][i].str());
            }
        }
        clock_gettime(CLOCK_REALTIME, &stop_time);
        cerr << "done (";
        cerr << ((double) (1000000000 * (stop_time.tv_sec - start_time.tv_sec) + stop_time.tv_nsec - start_time.tv_nsec)) / 1.0e9 << "s).\n";
    }

    // parse responses
    HTTPResponseParser parser;
    for (int i = 0; i < nlaunch; i++) {
        for (unsigned j = 0; j < lambda_regions.size(); j++) {
            parser.new_request_arrived(request[j][i]);

            while (! www[j][i].eof()) {
                parser.parse( www[j][i].read() );

                if ( not parser.empty() ) {
                    /*
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
                    */
                    parser.pop();
                    www[j][i].close();
                    break;
                }
            }
        }
    }
}
