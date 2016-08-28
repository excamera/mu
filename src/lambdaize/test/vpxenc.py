#!/usr/bin/python

import sys
import os
import os.path
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))
import time
import traceback

from libmu import Defs

import test.util
from test.defs import Defs as Td

import vpxenc_server
import lambda_function_template



def run_tests():
    lambda_function_template.cmdstring = "vpxenc --quiet --good -o ##OUTFILE## ##INFILE##"
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

        # NOTE there is a race condition w/ server startup... 1 sec is probably OK
        time.sleep(1)
        print "Client starting."

        try:
            lambda_function_template.lambda_handler(event, None)

        except SystemExit as e:
            if e.code == 0:
                sys.exit(0)
            else:
                print "Client subprocess exited with code %d" % e.code
                sys.exit(e.code)

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
            vpxenc_server.run(1, chainfile, keyfile)
        except:
            print "Server exception:\n%s" % traceback.format_exc()
            sys.exit(1)

        test.util.server_finish_check_retval(pid)

if __name__ == "__main__":
    run_tests()
