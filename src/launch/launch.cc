/* -*-mode:c++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <string>
#include <time.h>
#include <unistd.h>
#include <mutex>
#include <deque>
#include <condition_variable>
#include <thread>
#include <chrono>

#include <grpc++/grpc++.h>

#include "http_request.hh"
#include "http_response_parser.hh"
#include "lambda_request.hh"
#include "socket.hh"
#include "secure_socket.hh"

#include "launch.hh"
#include "launch.grpc.pb.h"
#include "blockingconcurrentqueue.h"


using namespace std;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using launch::LaunchParRequest;
using launch::LaunchParReply;
using launch::Launch;

static SecureSocket new_connection(const Address &addr, const string &name, SSLContext &ctx);

void launchpar(int nlaunch, string fn_name, string akid, string secret, string payload, vector<string> lambda_regions) {
    // prepare requests
//    cerr << "void launchpar(nlaunch: " << nlaunch << ", fn_name: " << fn_name <<", akid: "<<akid << ", secret: "<<secret<< ", payload: " << payload << ", lambda_regions: ";
//    for (vector<string>::const_iterator i = lambda_regions.begin(); i != lambda_regions.end(); ++i) cerr << *i << ' ';
   // cerr << endl;
//    cerr << "Building requests... ";
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
//    cerr << "done.\n";

    // open connections to server
    vector<vector<SecureSocket>> www;
//    cerr << "Opening sockets...";
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
//        cerr << "Sending requests... ";
        clock_gettime(CLOCK_REALTIME, &start_time);
        for (int i = 0; i < nlaunch; i++) {
            for (unsigned j = 0; j < lambda_regions.size(); j++) {
                www[j][i].set_blocking(true);
                www[j][i].write(request[j][i].str());
            }
        }
        clock_gettime(CLOCK_REALTIME, &stop_time);
//        cerr << "done (";
//        cerr << ((double) (1000000000 * (stop_time.tv_sec - start_time.tv_sec) + stop_time.tv_nsec - start_time.tv_nsec)) / 1.0e9 << "s).\n";
    }

    // parse responses
    HTTPResponseParser parser;
    for (int i = 0; i < nlaunch; i++) {
        for (unsigned j = 0; j < lambda_regions.size(); j++) {
            parser.new_request_arrived(request[j][i]);

            while (! www[j][i].eof()) {
                parser.parse( www[j][i].read() );

                if ( not parser.empty() ) {
                    if (parser.front().str().find("202 Accepted") == std::string::npos) {
                        cerr << "FORREST: request " << i << " errant response: \n" << parser.front().str() << "\n";
                        exit(-1);
                    }
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

class LaunchServiceImpl final : public Launch::Service {
    struct LaunchParInvocation {
        int nlaunch;
        std::string fn_name;
        std::string akid;
        std::string secret;
        std::string payload;
        std::vector<std::string> lambda_regions;
        std::chrono::steady_clock::time_point enqueue_time_monolith;
        clock_t enqueue_time_processor;
    };

private:
    moodycamel::BlockingConcurrentQueue<struct LaunchParInvocation> invocation_queue;

public:
    Status LaunchPar(ServerContext*, const LaunchParRequest* request, LaunchParReply* response) override {
        struct LaunchParInvocation invoc;
        vector<string> regions(request->lambda_regions().begin(), request->lambda_regions().end());
        invoc = {request->nlaunch(), request->fn_name(), request->akid(), request->secret(), request->payload(),
            regions, std::chrono::steady_clock::now(), std::clock()};
        this->invocation_queue.enqueue(invoc);
        response->set_success(true);
        return Status::OK;
    }

    void StartConsumer() {
        std::thread consumer([this]()
        {
            while (true) {
                struct LaunchParInvocation invoc, current;
                int req_count = 0;
                while (true) {
                    bool found = this->invocation_queue.try_dequeue(current);
                    if (!found) break;
                    if (0 == req_count) {
                        invoc = current;
                    }
                    req_count += current.nlaunch;
                }
                if (req_count == 0) {
                    usleep(10000);
                    continue;
                }
                auto dequeue_ts = std::chrono::steady_clock::now();
                auto dequeue_ts_p = std::clock();
                std::cerr << "the first invoc stayed in queue for: " << (std::chrono::duration<double>
                    (dequeue_ts - invoc.enqueue_time_monolith)).count() << " seconds, " <<
                    ((float)(dequeue_ts_p - invoc.enqueue_time_processor))/CLOCKS_PER_SEC << " processor seconds" << std::endl;
                launchpar(req_count, invoc.fn_name, invoc.akid, invoc.secret, invoc.payload, invoc.lambda_regions); //TODO: fix multiple fn_name
                std::cerr << "launchpar takes: " << (std::chrono::duration<double>(std::chrono::steady_clock::now()
                    - dequeue_ts)).count() << " seconds to start " << req_count << " batched reqs" << std::endl;
            }
        });
        consumer.detach();
    }
};

void servegrpc(std::string listen_addr) {
    std::cerr << "servegrpc called with " << listen_addr << std::endl;
    std::string server_address(listen_addr);
    LaunchServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cerr << "Server listening on " << server_address << std::endl;

    service.StartConsumer();
    std::cerr << "Consumer thread started" << std::endl;
    server->Wait();
}
