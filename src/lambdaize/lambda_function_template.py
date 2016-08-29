#!/usr/bin/python

import base64
import os
import select
import socket
import zlib

from OpenSSL import SSL

from libmu import SocketNB, Defs, util, handler

###
#  accept new connection from listening socket
###
def accept_new_connection(vals):
    if vals.get('prvsock') is not None and vals.get('prvsock').sock is not None:
        util.accept_socket(vals['lsnsock']).close()
    else:
        vals['prvsock'] = util.accept_socket(vals['lsnsock'])

###
#  state passed into the make_cmdstring function
###
class WorkerState(object):
    prev_iter = 0
    # If we were (more) deranged, we'd just use
    #     client_state = type('', (object,), {'prev_iter': None})()
    # Yes, you can construct an anonymous class!

###
#  send state file to nxtsock
###
def send_output_state(vals):
    with open("/tmp/final.state", 'r') as f:
        indata = ("STATE(%d):" % WorkerState.prev_iter) + base64.b64encode(zlib.compress(f.read()))

    vals['nxtsock'].enqueue(indata)
    WorkerState.prev_iter += 1

###
#  get state file from prvsock
###
def get_input_state(vals):
    indata = vals['prvsock'].dequeue()
    (msg, data) = indata.split(':', 1)

    assert msg[:6] == "STATE("
    lind = 6
    rind = msg.find(')')
    statenum = int(msg[lind:rind])

    with open("/tmp/temp.state", 'w') as f:
        f.write(base64.b64decode(zlib.decompress(data)))

    os.rename("/tmp/temp.state", "%d.state" % statenum)

###
#  figure out which sockets need to be selected
###
def get_arwsocks(vals):
    # asocks is all extant sockets
    socknames = ['cmdsock', 'lsnsock', 'prvsock', 'nxtsock']
    asocks = [ s for s in [ vals.get(n) for n in socknames ] if s is not None ] + \
             [ info[1] for info in vals.setdefault('runinfo', []) ]

    # rsocks is all objects that we could select upon
    rsocks = [ s for s in asocks
                 if isinstance(s, socket.SocketType)
                 or isinstance(s, SSL.Connection)
                 or (isinstance(s, SocketNB) and s.sock is not None) ]

    # wsocks is all rsocks that indicate they want to be written
    wsocks = [ s for s in asocks if isinstance(s, SocketNB) and (s.ssl_write or s.want_write) ]

    return (asocks, rsocks, wsocks)

###
#  make command string
###
def make_cmdstring(_, vals):
    command = Defs.cmdstring

    def vals_lookup(name, aslist = False):
        out = vals.get('cmd%s' % name)
        if out is None:
            out = vals['event'].get(name)

        if out is not None and aslist and not isinstance(out, list):
            out = [out]

        return out

    # add environment variables
    usevars = vals_lookup('vars', True)
    if usevars is not None:
        command = ' '.join(usevars) + ' ' + command

    # add arguments
    useargs = vals_lookup('args', True)
    if useargs is not None:
        command += ' ' + ' '.join(useargs)

    usequality = vals_lookup('quality', False)
    if usequality is not None:
        command = command.replace("##QUALITY##", usequality)

    # ##INFILE## and ##OUTFILE## string replacement
    useinfile = vals_lookup('infile', False)
    if useinfile is not None:
        command = command.replace('##INFILE##', useinfile)
    useoutfile = vals_lookup('outfile', False)
    if useoutfile is not None:
        command = command.replace('##OUTFILE##', useoutfile)

    if WorkerState.prev_iter != 0 and vals['expect_statefile']:
        instatefile = "/tmp/%d.state" % (WorkerState.prev_iter - 1)
        instatewait = 'while [ ! -f "%s" ]; do sleep 1; done; ' % instatefile
        instateswitch = "-I " + instatefile
    else:
        instatewait = ""
        instateswitch = ""

    command = command.replace("##INSTATEWAIT##", instatewait)
    command = command.replace("##INSTATESWITCH##", instateswitch)

    return command

###
#  lambda enters here
###
def lambda_handler(event, _):
    Defs.cmdstring = cmdstring
    Defs.make_cmdstring = staticmethod(make_cmdstring)

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
    expect_statefile = int(event.get('nonblock', 0))

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
           , 'expect_statefile': expect_statefile
           }

    # in mode 2, we open a listening socket and report the port number to the cmdsock
    if mode == 2:
        handler.do_listen('', vals)
    else:
        vals['cmdsock'].enqueue('OK:HELLO')

    while True:
        (_, rsocks, wsocks) = get_arwsocks(vals)
        if len(rsocks) == 0 and len(wsocks) == 0:
            if Defs.debug:
                print "***WARNING*** unclean client exit"
            break

        (rfds, wfds, _) = select.select(rsocks, wsocks, [], Defs.timeout)

        if len(rfds) == 0 and len(wfds) == 0 and len(vals.setdefault('runinfo', [])) == 0:
            print "CLIENT TIMEOUT"
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

        ### runsocks
        # if we get something from the runsock, handle it (and kill the sock)
        # iterate in reverse because we want to be able to remove without screwing up iteration
        for i in reversed(range(0, len(vals.setdefault('runinfo', [])))):
            (pid, sock) = vals['runinfo'][i]
            if sock.want_handle:
                del vals['runinfo'][i]

                # we only receive something after the runner is done
                # the return value is already packed into the outmsg, so no need to get it here
                os.waitpid(pid, 0)

                outmsg = sock.dequeue()
                sock.close()
                del sock

                vals['cmdsock'].enqueue(outmsg)

                if outmsg[:12] == "OK:RETVAL(0)" and vals.get('nxtsock') is not None:
                    send_output_state(vals)

        if vals.get('prvsock') is not None and vals['prvsock'].want_handle:
            # handle receiving new state file from previous lambda
            get_input_state(vals)

        #if vals.get('nxtsock') is not None:
        #    pass
        #    # this should be a send-only socket unless we decide we need two-way state comms

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

cmdstring = ''
# cmdstring = "##INSTATEWAIT## ./xc-enc -s ##QUALITY## -i y4m ##INSTATESWITCH## -O /tmp/final.state -o ##OUTFILE## ##INFILE##"
