#!/usr/bin/python

import sys
import os
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))

import libmu
import xcenc_server
import png2y4m_server
import lambda_state_server

import test.util

def run_tests(server_module):
    libmu.Defs.debug = True
    test.util.run_enc_server(server_module)
    print "Server exiting."

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] is "0":
        xcenc_server.ServerInfo.num_parts = 2
        xcenc_server.ServerInfo.num_passes = 4
        server = xcenc_server

    elif sys.argv[1] is "1":
        server = png2y4m_server

    elif sys.argv[1] is "2":
        server = lambda_state_server

    else:
        print "Usage: %s <0|1>" % sys.argv[0]
        print "0 : run xcenc server, expecting 2 clients"
        print "1 : run png2y4m server, expecting 1 client"
        print "2: run lambda_state_server"
        sys.exit(1)

    run_tests(server)
