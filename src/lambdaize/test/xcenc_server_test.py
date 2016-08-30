#!/usr/bin/python

import sys
import os
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))

import libmu
import xcenc_server

import test.util

def run_tests():
    libmu.Defs.debug = True
    test.util.run_enc_server(xcenc_server.run, 2)
    print "Server exiting."

if __name__ == "__main__":
    run_tests()
