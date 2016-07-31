#!/usr/bin/python

import os
import select
import socket

from OpenSSL import SSL

from libmu import SocketNB, Defs, util, handler

###
#  accept new connection from listening socket
###
def accept_new_connection(vals):
    if vals.get('prvsock') is not None:
        util.accept_socket(vals['lsnsock']).close()
    else:
        vals['prvsock'] = util.accept_socket(vals['lsnsock'])

###
#  send state file to nxtsock
###
def send_state_file(fname, vals):
    if vals.get('nxtsock') is None:
        return

    if fname is not None:
        # XXX do something here!!!
        pass

###
#  figure out which sockets need to be selected
###
def get_arwsocks(vals):
    socknames = ['cmdsock', 'lsnsock', 'prvsock', 'nxtsock', 'runsock']
    # asocks is all of the above that are not none
    asocks = [ s for s in [ vals.get(n) for n in socknames ] if s is not None ]
    # rsocks is all objects that we could select upon
    rsocks = [ s for s in asocks
                 if isinstance(s, socket.SocketType)
                 or isinstance(s, SSL.Connection)
                 or (isinstance(s, SocketNB) and s.sock is not None) ]
    # wsocks is all rsocks that indicate they want to be written
    wsocks = [ s for s in asocks if isinstance(s, SocketNB) and (s.ssl_write or s.want_write) ]
    return (asocks, rsocks, wsocks)

###
#  lambda enters here
###
def lambda_handler(event, _):
    Defs.executable = executable

    # get config info from event
    port = int(event.get('port', 13579))
    mode = int(event.get('mode', 0))
    addr = event.get('addr', '127.0.0.1')
    bucket = event.get('bucket', 'excamera-us-east-1')
    region = event.get('region', 'us-east-1')
    cacert = event.get('cacert')
    srvkey = event.get('srvkey')
    srvcrt = event.get('srvcrt')
    nonblock = int(event.get('nonblock', 0))

    # default: just run the command and exit
    if mode == 0:
        return handler.do_run('', {'event': event})

    s = util.connect_socket(addr, port, cacert)
    if not isinstance(s, SocketNB):
        return str(s)

    vals = { 'cmdsock': s
           , 'bucket': bucket
           , 'region': region
           , 'event': event
           , 'cacert': cacert
           , 'srvkey': srvkey
           , 'srvcrt': srvcrt
           , 'nonblock': nonblock
           }

    # in mode 2, we open a listening socket and report the port number to the cmdsock
    if mode == 2:
        handler.do_listen('', vals)
    else:
        vals['cmdsock'].enqueue('OK:HELLO')

    while True:
        (_, rsocks, wsocks) = get_arwsocks(vals)
        if len(rsocks) == 0 and len(wsocks) == 0:
            break

        (rfds, wfds, _) = select.select(rsocks, wsocks, [], Defs.timeout)

        if len(rfds) == 0 and len(wfds) == 0:
            print "TIMEOUT!!!"
            break

        # do all the reads we can
        for r in rfds:
            if vals.get('lsnsock') is not None and r is vals['lsnsock']:
                accept_new_connection(vals)
                continue

            r.do_read()

        # launch any writes we can
        for w in wfds:
            w.do_write()

        # if the command sock is dead, we are dead
        if vals.get('cmdsock') is None:
            break

        ### cmdsock
        # handle commands in the cmdsock queue
        break_outer = False
        while vals['cmdsock'].want_handle and not break_outer:
            nxt = vals['cmdsock'].dequeue()
            break_outer = handler.handle_message(nxt, vals)

        if break_outer:
            break

        ### runsock
        # if we got something from the runsock, handle it (and kill the sock)
        if vals.get('runsock') is not None and vals['runsock'].want_handle:
            (_, status) = os.waitpid(vals['runpid'], 0)
            retval = status >> 8

            (cmdstring, runval) = vals['runsock'].dequeue().split('\0')
            vals['runsock'].close()
            vals['runsock'] = None
            vals['cmdsock'].enqueue('OK:RETVAL(%d):OUTPUT(%s):COMMAND(%s)' % (retval, runval, cmdstring))
            # XXX do something with the result
            #send_state_file(fname, vals)

        if vals.get('prvsock') is not None:
            pass
            # do something
            # handle receiving new state files from previous lambda

        if vals.get('nxtsock') is not None:
            pass
            # this should be a send-only socket unless we decide we need two-way state comms

    (afds, _, _) = get_arwsocks(vals)
    for a in afds:
        # try to be nice... but not too hard
        try:
            a.shutdown(socket.SHUT_RDWR)
        except:
            pass

        try:
            a.shutdown()
        except:
            pass

        try:
            a.close()
        except:
            pass

executable = ''
