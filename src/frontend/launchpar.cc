/* -*-mode:c++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
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

void launchpar(int nlaunch);
SecureSocket new_connection(const Address &addr, const string &name, SSLContext &ctx);
void json_from_env(const char *varname, stringstream &strm, char pre_char, bool throw_if_empty, const char *dflt);
void read_cert_file(const char *varname, stringstream &strm);

static const vector<string> lambda_regions = { {"us-east-1"}        // Oregon
                                             /*
                                             , {"us-west-2"}        // Virginia
                                             , {"eu-west-1"}        // Ireland
                                             , {"eu-central-1"}     // Frankfurt
                                             , {"ap-northeast-1"}   // Tokyo
                                             , {"ap-southeast-2"}   // Sydney
                                             */
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
        launchpar(nlaunch);
    } catch ( const exception & e ) {
        print_exception( e );
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

void json_from_env(const char *varname, stringstream &strm, char prechar, bool throw_if_empty, const char *dflt) {
    string envvarname("EVENT_");
    envvarname += varname;
    char *value = getenv(envvarname.c_str());

    if (!value) {
        if (throw_if_empty) {
            string err = "missing required environment variable: EVENT_";
            err += varname;
            throw runtime_error(err);
        } else if (dflt == NULL) {
            return;
        }

        value = (char *)dflt;
    }

    strm << prechar << '"' << varname << "\":\"" << value << '"';
}

void read_cert_file(const char *varname, stringstream &strm) {
    string envvarname("EVENT_");
    envvarname += varname;
    char *filename = getenv(envvarname.c_str());

    if (!filename) {
        return;
    }

    ifstream cert(filename);
    if (!cert.good()) {
        return;
    }

    string tmp;
    strm << ',' << '"' << varname << "\":\"";

    bool started = false;
    while (true) {
        getline(cert, tmp);
        if (tmp.find("-----BEGIN ") == 0) {
            started = true;
            continue;
        } else if (tmp.find("-----END ") == 0 || tmp == "") {
            break;
        }

        if (started) {
            strm << tmp;
        }
    }

    strm << '"';
}

void launchpar(int nlaunch) {
    // prepare requests
    cerr << "Building requests... ";
    string fn_name = safe_getenv("LAMBDA_FUNCTION");
    string secret = safe_getenv("AWS_SECRET_ACCESS_KEY");
    string akid = safe_getenv("AWS_ACCESS_KEY_ID");

    string payload;
    {
        stringstream pstream;

        // required  parts
        json_from_env("addr", pstream, '{', true, NULL);
        json_from_env("port", pstream, ',', true, NULL);

        // optional parts
        read_cert_file("cacert", pstream);
        read_cert_file("srvcrt", pstream);
        read_cert_file("srvkey", pstream);
        json_from_env("mode", pstream, ',', false, "1");

        json_from_env("bucket", pstream, ',', false, NULL);
        json_from_env("nonblock", pstream, ',', false, NULL);
        json_from_env("expect_statefile", pstream, ',', false, NULL);
        json_from_env("rm_tmpdir", pstream, ',', false, NULL);

        pstream << '}';

        payload = pstream.str();
    }

    vector<vector<HTTPRequest>> request;

    for (unsigned j = 0; j < lambda_regions.size(); j++) {
        request.emplace_back(vector<HTTPRequest>());
        for (int i = 0; i < nlaunch; i++) {
            LambdaInvocation ll(secret, akid, fn_name, payload, "", LambdaInvocation::InvocationType::Event, lambda_regions[j]);
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
