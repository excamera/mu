#!/usr/bin/python

import os
import sys
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))

import lambda_function_template
import libmu

import test.defs
import test.util

def run_tests(use_mode):
    libmu.Defs.debug = True

    event = { 'mode': use_mode
            , 'port': 13579
            , 'nonblock': 0
            , 'cacert': test.defs.Defs.cacert
            , 'srvcrt': test.defs.Defs.srvcrt
            , 'srvkey': test.defs.Defs.srvkey
            , 'rm_tmpdir': 0
            }

    test.util.run_lambda_function_template(event)

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] is "0":
        lambda_function_template.cmdstring = "##INSTATEWAIT## /home/kwantam/git/github.com/alfalfa/src/frontend/xc-enc -s ##QUALITY## -i y4m ##INSTATESWITCH## -O ##TMPDIR##/final.state -o ##OUTFILE## ##INFILE##"
        mode = 2

    elif sys.argv[1] is "1":
        lambda_function_template.cmdstring = "/home/kwantam/git/github.com/daala_tools/png2y4m -o ##OUTFILE## ##INFILE##"
        mode = 1

    else:
        print "Usage: %s <0|1>" % sys.argv[0]
        print "0 : run xcenc client"
        print "1 : run png2y4m client"
        sys.exit(1)

    run_tests(mode)
