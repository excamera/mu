#!/usr/bin/python

import os
import select
import socket
import sys

from OpenSSL import SSL

from libmu import Defs, ErrorState, TerminalState, CommandListState, MachineState

class FinalState(TerminalState):
    extra = "(finished)"

class VPXEncStateMachine(CommandListState):
    nextState = FinalState
    commandlist = [ ("OK:HELLO", "set:inkey:{0}/{0}{1}.y4m")
                  , "set:targfile:/tmp/{0}{1}.y4m"
                  , "set:cmdinfile:/tmp/{0}{1}.y4m"
                  , "set:cmdoutfile:/tmp/{0}{1}.ivf"
                  , "set:fromfile:/tmp/{0}{1}.ivf"
                  , "set:outkey:{0}/out/{0}{1}.ivf"
                  , "retrieve:"
                  , "run:"
                  , ("OK:RETVAL(0)", "upload:")
                  , "quit:"
                  ]

    def __init__(self, prevState, aNum, vName):
        super(VPXEncStateMachine, self).__init__(prevState, aNum)
        self.commands = [ s.format(vName, "%06d" % aNum) for s in self.commands ]

#### begin script ####

def main(chainfile=None, keyfile=None):
    NUM_PARTS = 1
    if len(sys.argv) > 1:
        NUM_PARTS = int(sys.argv[1])

    # bro, you listening to this?
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(('0.0.0.0', 13579))
    lsock.listen(NUM_PARTS + 10) # lol like the kernel listens to me

    sslctx = SSL.Context(SSL.TLSv1_2_METHOD)
    sslctx.set_options(SSL.OP_NO_COMPRESSION)
    sslctx.set_cipher_list(Defs.cipher_list)
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
    states = []

    def handle_server_sock(ls):
        (ns, _) = ls.accept()
        ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        ns.setblocking(False)

        nstate = VPXEncStateMachine(ns, len(states), "bbb")
        nstate.do_handshake()

        states.append(nstate)

        if len(states) == NUM_PARTS:
            # no need to listen any longer, we have all our connections
            try:
                ls.shutdown()
                ls.close()
            except:
                pass

            ls = None

        return ls

    def rwsplit(sts):
        rs = []
        ws = []
        for st in sts:
            if st.sock is None:
                continue

            if not isinstance(st, TerminalState):
                rs.append(st)

            if st.ssl_write or st.want_write:
                ws.append(st)

        return (rs, ws)

    while True:
        (readSocks, writeSocks) = rwsplit(states)

        if len(readSocks) == 0 and len(writeSocks) == 0 and lsock is None:
            break

        if lsock is not None:
            readSocks += [lsock]

        (rfds, wfds, _) = select.select(readSocks, writeSocks, [], 60)

        if len(rfds) == 0 and len(wfds) == 0:
            print "TIMEOUT!!!"
            break

        for r in rfds:
            if r is lsock:
                lsock = handle_server_sock(lsock)

            else:
                rnext = r.do_read()
                states[rnext.actorNum] = rnext

        for w in wfds:
            # reading might have caused this state to get updated,
            # so we index into states to be sure we have the freshest version
            actorNum = w.actorNum
            wnext = states[actorNum].do_write()
            states[actorNum] = wnext

        for r in readSocks:
            if not isinstance(r, MachineState):
                continue

            rnum = r.actorNum
            rnext = states[rnum]
            if rnext.want_handle:
                rnext = rnext.do_handle()

            states[rnum] = rnext

    error = False
    for state in states:
        state.close()
        print str(state.get_timestamps())
        if isinstance(state, ErrorState):
            error = True

    if error:
        raise Exception("ERROR: worker terminated abnormally.")

if __name__ == "__main__":
    main()
