#!/usr/bin/python

import json
import os
import select
import socket
import sys

from OpenSSL import SSL

import pylaunch
import libmu.defs
import libmu.machine_state

###
#  handle new connection on server listening socket
###
def _handle_server_sock(ls, states, statemap, num_parts, constructor):
    (ns, _) = ls.accept()
    ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ns.setblocking(False)

    actor_number = len(states)
    nstate = constructor(ns, actor_number)
    nstate.do_handshake()

    states.append(nstate)
    statemap[nstate.fileno()] = actor_number

    if len(states) == num_parts:
        # no need to listen any longer, we have all our connections
        try:
            ls.shutdown()
            ls.close()
        except:
            pass

        ls = None

    return ls

###
#  server: launch a bunch of lambda instances using pylaunch
###
def server_launch(server_info, event, akid, secret):
    if event.get('addr') is None:
        # figure out what the IP address of the interface talking to AWS is
        # NOTE if you have different interfaces routing to different regions
        #      this won't work. I'm assuming that's unlikely.
        testsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        testsock.connect(("lambda." + server_info.regions[0] + ".amazonaws.com", 443))
        event['addr'] = testsock.getsockname()[0]
        testsock.close()

    pid = os.fork()
    if pid == 0:
        # pylint: disable=no-member
        # (pylint can't "see" into C modules)
        pylaunch.launchpar(server_info.num_parts, server_info.lambda_function,
                           akid, secret, json.dumps(event), server_info.regions)
        sys.exit(0)

###
#  server mainloop
###
def server_main_loop(states, constructor, num_parts, chainfile=None, keyfile=None, outfile=None):
    # bro, you listening to this?
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(('0.0.0.0', 13579))
    lsock.listen(num_parts + 10) # lol like the kernel listens to me

    sslctx = SSL.Context(SSL.TLSv1_2_METHOD)
    sslctx.set_options(SSL.OP_NO_COMPRESSION)
    sslctx.set_cipher_list(libmu.defs.Defs.cipher_list)
    sslctx.set_verify(SSL.VERIFY_NONE, lambda *_: True)

    # set up server key
    if chainfile is None or keyfile is None:
        sslctx.use_certificate_chain_file(os.environ.get('CERTIFICATE_CHAIN', 'server_chain.pem'))
        sslctx.use_privatekey_file(os.environ.get('PRIVATE_KEY', 'server_key.pem'))
    else:
        sslctx.use_certificate_chain_file(chainfile)
        sslctx.use_privatekey_file(keyfile)
    sslctx.check_privatekey()

    # set up server SSL connection
    lsock = SSL.Connection(sslctx, lsock)
    lsock.set_accept_state()
    lsock.setblocking(False)
    lsock_fd = lsock.fileno()

    def rwsplit(sts, ret):
        diffs = []
        ret += [0] * (len(sts) - len(ret))
        for (st, idx) in zip(sts, range(0, len(sts))):
            val = 0
            if st.sock is not None:
                if not isinstance(st, libmu.machine_state.TerminalState):
                    val = val | select.POLLIN

                if st.ssl_write or st.want_write:
                    val = val | select.POLLOUT

            if val != ret[idx]:
                ret[idx] = val
                diffs.append(idx)

        return diffs

    statemap = {}
    rwflags = []
    poll_obj = select.poll()
    poll_obj.register(lsock_fd, select.POLLIN)
    while True:
        dflags = rwsplit(states, rwflags)

        if all([ v == 0 for v in rwflags ]) and lsock is None:
            break

        for idx in dflags:
            if rwflags[idx] != 0:
                poll_obj.register(states[idx], rwflags[idx])
            else:
                poll_obj.unregister(states[idx])

        if lsock is None and lsock_fd is not None:
            poll_obj.unregister(lsock_fd)
            lsock_fd = None

        pfds = poll_obj.poll(1000 * libmu.defs.Defs.timeout)

        if len(pfds) == 0:
            # len(rfds) == 0 and len(wfds) == 0:
            print "SERVER TIMEOUT"
            break

        # look for readable FDs
        for (fd, ev) in pfds:
            if (ev & select.POLLIN) != 0:
                if lsock is not None and fd == lsock.fileno():
                    lsock = _handle_server_sock(lsock, states, statemap, num_parts, constructor)

                else:
                    actorNum = statemap[fd]
                    r = states[actorNum]
                    rnext = r.do_read()
                    states[actorNum] = rnext

        for (fd, ev) in pfds:
            if (ev & select.POLLOUT) != 0:
                # reading might have caused this state to get updated,
                # so we index into states to be sure we have the freshest version
                actorNum = statemap[fd]
                w = states[actorNum]
                wnext = w.do_write()
                states[actorNum] = wnext

        for rnext in [ st for st in states if not isinstance(st, libmu.machine_state.TerminalState) ]:
            if rnext.want_handle:
                rnext = rnext.do_handle()
            states[rnext.actorNum] = rnext

    fo = None
    error = []
    errvals = []
    if outfile is not None:
        fo = open(outfile, 'w')

    for (state, num) in zip(states, range(0, len(states))):
        state.close()
        if isinstance(state, libmu.machine_state.ErrorState):
            error.append(num)
            errvals.append(repr(state))
        elif fo is not None:
            fo.write("%d:%s" % (state.actorNum, str(state.get_timestamps())))

    if error:
        evals = str(error) + "\n  " + "\n  ".join(errvals)
        raise Exception("ERROR: the following workers terminated abnormally:\n%s" % evals)
