#!/usr/bin/python

import os
import select
import sys
import traceback

from OpenSSL import SSL

import libmu
from libmu import util

import lambda_function_template

from test.defs import Defs as Td

# emulate a blocking send on a SocketNB
def blocking_send(sock, msg):
    sock.enqueue(msg)
    while sock.want_write:
        (_, wfds, _) = select.select([], [sock], [], Td.timeout)
        if len(wfds) == 0:
            raise Exception("timeout in blocking_send(%s)" % msg)
        sock.do_write()

# emulate a blocking recv on a SocketNB
def blocking_recv(sock):
    while not sock.want_handle:
        (rfds, _, _) = select.select([sock], [], [], Td.timeout)
        if len(rfds) == 0:
            raise Exception("timeout in blocking_recv()")
        sock.do_read()
    return sock.dequeue()

def blocking_accept(sock):
    (rfds, _, _) = select.select([sock], [], [], Td.timeout)
    if len(rfds) == 0:
        raise Exception("timeout waiting to accept")
    return libmu.util.accept_socket(sock)

def run_one_test(test_server, executable, use_ssl, run_nonblock, *args, **kwargs):
    if use_ssl:
        ls = util.listen_socket('127.0.0.1', 0, Td.cacert, Td.srvcrt, Td.srvkey, 1)
        if not isinstance(ls, SSL.Connection):
            raise Exception("Error creating SSL connection: %s" % str(ls))
    else:
        ls = util.listen_socket('127.0.0.1', 0, None, None, None, 1)

    pid = os.fork()
    if pid == 0:
        (_, port) = ls.getsockname()
        print port
        lambda_function_template.executable = executable
        event = { 'mode': kwargs.get('mode', 1)
                , 'port': port
                , 'nonblock': 1 if run_nonblock else 0
                }
        if use_ssl:
            if Td.cacert is not None:
                event['cacert'] = Td.cacert
            if Td.srvcrt is not None:
                event['srvcrt'] = Td.srvcrt
            if Td.srvkey is not None:
                event['srvkey'] = Td.srvkey

        print "Client starting."
        lambda_function_template.lambda_handler(event, None)
        print "Client exiting."
        sys.exit(0)

    else:
        cs = blocking_accept(ls)
        print "Server starting."

        try:
            test_server(cs, run_nonblock, args, kwargs)
        except:
            print "Server exception:\n%s" % traceback.format_exc()
            sys.exit(1)

        try:
            cs.close()
        except:
            pass

        del cs

        (_, status) = os.waitpid(pid, 0)
        retval = status >> 8
        if retval != 0:
            print "ERROR: client process exited with retval %d" % retval
            sys.exit(1)
        else:
            print "Server exiting."
