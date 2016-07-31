#!/usr/bin/python

import sys
import os
import os.path
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))
import time
import traceback

from libmu import Defs

from test.defs import Defs as Td

import vpxenc_server
import lambda_function_template



def run_tests():
    executable = "vpxenc --good -o ##OUTFILE## ##INFILE##"
    Defs.debug = True
    pid = os.fork()
    if pid == 0:
        event = { 'mode': 1
                , 'port': 13579
                , 'nonblock': 0
                , 'cacert': Td.cacert
                , 'srvcrt': Td.srvcrt
                , 'srvkey': Td.srvkey
                }

        time.sleep(1)   # XXX race condition w/ server startup
        print "Client starting."

        try:
            lambda_function_template.lambda_handler(event, None)
        except:
            print "Client exception:\n%s" % traceback.format_exc()
            sys.exit(1)

        print "Client exiting."
        sys.exit(0)

    else:
        print "Server starting."

        try:
            chainfile = os.path.abspath(os.path.join(sys.path[0], "data/server_chain.pem"))
            keyfile = os.path.abspath(os.path.join(sys.path[0], "data/server_key.pem"))
            vpxenc_server.main(chainfile, keyfile)
        except:
            print "Server exception:\n%s" % traceback.format_exc()
            sys.exit(1)

        (_, status) = os.waitpid(pid, 0)
        retval = status >> 8
        if retval != 0:
            print "ERROR: client process exited with retval %d" % retval
            sys.exit(1)
        else:
            print "Server exiting."

if __name__ == "__main__":
    run_tests()
