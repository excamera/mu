#!/usr/bin/python

import sys
import os
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))
# insert parent directory in search path, since test/ lives alongside libmu

import libmu

import test.util as tutil

def test_server(sock, run_nonblock, args, _):
    do_quit = args[0]

    response = tutil.blocking_recv(sock)
    print "Initial OK:", response

    print "Setting infile variable."
    tutil.blocking_send(sock, "set:cmdinfile:ASDF")
    response = tutil.blocking_recv(sock)
    print "  Got response:", response

    print "Dumping vals."
    tutil.blocking_send(sock, "dump_vals:")
    response = tutil.blocking_recv(sock)
    print "  Got response:", response

    print "Running command."
    tutil.blocking_send(sock, "run:")
    if run_nonblock:
        response = tutil.blocking_recv(sock)
        print "  Got response:", response
    response = tutil.blocking_recv(sock)
    print "  Got response:", response

    # make sure return value of the process we ran was OK
    (ok, retval, _) = response.split(':', 2)
    assert ok == "OK"
    retval = int(retval[7:-1])
    if retval != 0:
        raise Exception("Retval of RUN process was %d" % retval)

    if do_quit:
        print "Telling client to quit."
        tutil.blocking_send(sock, "quit:")

    sock.close()

def run_tests():
    cmdstring = "echo ##INFILE## | md5sum"
    libmu.Defs.debug = True
    tutil.run_one_test(test_server, cmdstring, False, False, False)
    tutil.run_one_test(test_server, cmdstring, False, True, False)
    tutil.run_one_test(test_server, cmdstring, True, False, False)
    tutil.run_one_test(test_server, cmdstring, True, True, False)
    tutil.run_one_test(test_server, cmdstring, False, False, True)
    tutil.run_one_test(test_server, cmdstring, False, True, True)
    tutil.run_one_test(test_server, cmdstring, True, False, True)
    tutil.run_one_test(test_server, cmdstring, True, True, True)

if __name__ == "__main__":
    run_tests()
