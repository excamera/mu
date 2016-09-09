#!/usr/bin/python

import sys
import os
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))

import test.util as tutil

import vpxenc_server
#import xcenc_server

tests = [ ("vpxenc --quiet --good -o ##OUTFILE## ##INFILE##", 1, vpxenc_server)
        #, ("##INSTATEWAIT## /home/kwantam/git/github.com/alfalfa/src/frontend/xc-enc -s ##QUALITY## -i y4m ##INSTATESWITCH## -O ##TMPDIR##/final.state -o ##OUTFILE## ##INFILE##", 2, xcenc_server)
        ]

def run_tests():
    for test in tests:
        tutil.run_encsrv_test(test)

if __name__ == "__main__":
    run_tests()
