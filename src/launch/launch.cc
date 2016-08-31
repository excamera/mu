/* -*-mode:c++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <string>
#include <time.h>
#include <unistd.h>

#include "http_request.hh"
#include "http_response_parser.hh"
#include "lambda_request.hh"
#include "socket.hh"
#include "secure_socket.hh"

#include "launch.hh"

using namespace std;

static SecureSocket new_connection(const Address &addr, const string &name, SSLContext &ctx);

void launchpar(int nlaunch, string fn_name, string akid, string secret, string payload, vector<string> lambda_regions) {
    // prepare requests
    cerr << "Building requests... ";

    vector<vector<HTTPRequest>> request;
    for (unsigned j = 0; j < lambda_regions.size(); j++) {
        request.emplace_back(vector<HTTPRequest>());
        for (int i = 0; i < nlaunch; i++) {
            // replace ##ID## with launch number if it's in the string
            string local_payload = payload;
            int id_idx = local_payload.find("##ID##");
            if (id_idx >= 0) {
                local_payload.replace(id_idx, 6, to_string(j*nlaunch + i));
            }

            LambdaInvocation ll(secret, akid, fn_name, local_payload, "", LambdaInvocation::InvocationType::Event, lambda_regions[j]);
            request[j].emplace_back(move(ll.to_http_request()));
        }
    }
    cerr << "done.\n";

    // open connections to server
    vector<vector<SecureSocket>> www;
    cerr << "Opening sockets...";
    {
        vector<vector<bool>> sfins;
        vector<string> servername;
        vector<Address> server;
        SSLContext ctx;
        for (int i = 0; i < nlaunch; i++) {
            for (unsigned j = 0; j < lambda_regions.size(); j++) {
                if (i == 0) {
                    servername.emplace_back(string("lambda." + lambda_regions[j] + ".amazonaws.com"));
                    server.emplace_back(Address(servername[j], "https"));
                    www.emplace_back(vector<SecureSocket>());
                    sfins.emplace_back(vector<bool> (nlaunch, false));
                }

                www[j].emplace_back(move(new_connection(server[j], servername[j], ctx)));
            }
        }

        // let all the connections happen
        // yes this is ugly
        bool all_done = false;
        for (int l = 0; ! all_done; l++) {
            bool found = false;
            for (unsigned j = 0; j < lambda_regions.size(); j++) {
                for (int i = 0; i < nlaunch; i++) {
                    if (! sfins[j][i]) {
                        int ret = 0;
                        www[j][i].getsockopt(SOL_SOCKET, SO_ERROR, ret);
                        if (ret) {
                            cerr << '!';
                            found = true;
                            continue;
                        } else {
                            try {
                                www[j][i].connect();
                            } catch (...) {
                                found = true;
                                continue;
                            }
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
                    parser.pop();
                    www[j][i].close();
                    break;
                }
            }
        }
    }
}

SecureSocket new_connection(const Address &addr, const string &name, SSLContext &ctx) {
    TCPSocket sock;
    sock.set_blocking(false);
    try {
        sock.connect(addr);
    } catch (const exception &e) {
        // it's OK, we expected this
    }
    SecureSocket this_www = ctx.new_secure_socket(move(sock));
    this_www.set_hostname(name);

    return this_www;
}
