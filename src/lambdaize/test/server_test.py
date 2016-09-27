#!/usr/bin/python

import sys
import os
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))

import libmu
import xcenc_server
import xcenc7_server
import png2y4m_server
import lambda_state_server

import test.util

def run_tests(server_module):
    libmu.Defs.debug = True
    test.util.run_enc_server(server_module)
    print "Server exiting."

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] is "0" or sys.argv[1] is "3":
        if sys.argv[1] is "0":
            server = xcenc_server
            server.ServerInfo.num_passes = (1,6,0,0)
        else:
            server = xcenc7_server
        server.ServerInfo.num_parts = 8
        server.ServerInfo.quality_y=62
        server.ServerInfo.num_offset = 725
        server.ServerInfo.keyframe_distance = 5

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
