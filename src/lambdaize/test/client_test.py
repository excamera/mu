#!/usr/bin/python

import os
import sys
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))

import lambda_function_template
import libmu

import test.defs
import test.util

def run_tests(use_mode, use_nonblock, use_silent, use_expect):
    libmu.Defs.debug = True

    event = { 'mode': use_mode
            , 'port': 13579
            , 'nonblock': use_nonblock
            , 'bg_silent': use_silent
            , 'cacert': test.defs.Defs.cacert
            , 'srvcrt': test.defs.Defs.srvcrt
            , 'srvkey': test.defs.Defs.srvkey
            , 'rm_tmpdir': 0
            , 'minimal_recode': 1
            , 'expect_statefile': use_expect
            }

    test.util.run_lambda_function_template(event)

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] is "0":
        lambda_function_template.cmdstring = ""
        mode = 2
        nonblock = 1
        silent = 1
        expect = 1

    elif sys.argv[1] is "1":
        lambda_function_template.cmdstring = "/home/kwantam/git/github.com/daala_tools/png2y4m -o ##OUTFILE## ##INFILE##"
        mode = 1
        nonblock = 0
        silent = 0
        expect = 0

    else:
        print "Usage: %s <0|1>" % sys.argv[0]
        print "0 : run xcenc client"
        print "1 : run png2y4m client"
        sys.exit(1)

    run_tests(mode, nonblock, silent, expect)
