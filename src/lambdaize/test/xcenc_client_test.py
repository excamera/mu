#!/usr/bin/python

import os
import sys
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))

import lambda_function_template
import libmu

import test.defs
import test.util

def run_tests():
    lambda_function_template.cmdstring = "##INSTATEWAIT## /home/kwantam/git/github.com/alfalfa/src/frontend/xc-enc -s ##QUALITY## -i y4m ##INSTATESWITCH## -O ##TMPDIR##/final.state -o ##OUTFILE## ##INFILE##"
    libmu.Defs.debug = True

    event = { 'mode': 2
            , 'port': 13579
            , 'nonblock': 0
            , 'cacert': test.defs.Defs.cacert
            , 'srvcrt': test.defs.Defs.srvcrt
            , 'srvkey': test.defs.Defs.srvkey
            , 'rm_tmpdir': 0
            }

    test.util.run_lambda_function_template(event)

if __name__ == "__main__":
    run_tests()
