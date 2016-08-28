#!/usr/bin/python

import os
import select
import socket
import sys

from OpenSSL import SSL

from libmu import Defs, ErrorState, TerminalState, CommandListState, MachineState, OnePassState, IfElseState, SuperpositionState, InfoWatcherState, ForLoopState

class ServerInfo(object):
    states = None
    num_passes = 4

class FinalState(TerminalState):
    extra = "(finished)"

class XCEncUploadState(CommandListState):
    extra = "(uploading result)"
    nextState = FinalState
    commandlist = [ (None, "upload:")
                  , ("OK:UPLOADING", None)
                  , ("OK:UPLOAD(", "quit:")
                  ]

class XCEncRunState(CommandListState):
    extra = "(running xc-enc)"
    commandlist = [ (None, "run:")
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", None)
                  ]

class XCEncLoopState(ForLoopState):
    extra = "(xc-enc looping)"
    loopState = XCEncRunState
    exitState = XCEncUploadState
    iterFin = ServerInfo.num_passes
    # XXX maybe make this evaluated at construction time s.t. we can change it on the fly

# need to do this here to avoid use-before-def
XCEncRunState.nextState = XCEncLoopState

class XCEncDoConnect(CommandListState):
    extra = "(connecting to neighbor)"
    commandList = [ (None, "connect:")
                  , ("OK:CONNECT", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncDoConnect, self).__init__(prevState, aNum)
        # fill in correct command
        self.commands[0] = "connect:%s:%d" % self.info['connecthost']

class XCEncWaitForConnectHost(InfoWatcherState):
    extra = "(waiting for neighbor to be ready to accept a connection)"
    nextState = XCEncDoConnect

    def info_updated(self):
        if self.info.get('connecthost') is not None:
            self.kick()

class XCEncStartConnect(IfElseState):
    extra = "(checking whether neighbor is ready to accept a connection)"
    consequentState = XCEncDoConnect
    alternativeState = XCEncWaitForConnectHost

    def testfn(self):
        return (not self.info.get('do_fwconn')) or (self.info.get('connecthost') is not None)

class XCEncFinishRetrieve(CommandListState):
    extra = "(waiting for retrieval confirmation)"
    commandlist = [ ("OK:RETRIEVING", None)
                  , ("OK:RETRIEVE(", None)
                  ]

class XCSetupConnect(SuperpositionState):
    nextState = XCEncLoopState
    state_constructors = [XCEncFinishRetrieve, XCEncStartConnect]

class XCEncSettingsState(CommandListState):
    extra = "(preparing worker)"
    nextState = XCSetupConnect
    commandlist = [ "set:inkey:{0}/{0}{1}.y4m"
                  , "set:targfile:/tmp/{0}{1}.y4m"
                  , "set:cmdinfile:/tmp/{0}{1}.y4m"
                  , "set:cmdoutfile:/tmp/{0}{1}.ivf"
                  , "set:cmdquality:0.9"
                  , "set:fromfile:/tmp/{0}{1}.ivf"
                  , "set:outkey:{0}/out/{0}{1}.ivf"
                  , "seti:nonblock:1"
                  , "retrieve:"
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncSettingsState, self).__init__(prevState, aNum)
        # set up commands
        aNum = self.actorNum
        vName = self.info['vidName']
        self.commands = [ s.format(vName, "%06d" % aNum) if s is not None else None for s in self.commands ]

class XCEncSetNeighborConnectState(OnePassState):
    extra = "(waiting for lsnport to report to neighbor)"
    expect = "OK:LISTEN"
    nextState = XCEncSettingsState

    def __init__(self, prevState, aNum, vName, do_fwconn):
        super(XCEncSetNeighborConnectState, self).__init__(prevState, aNum)

        # store these for later
        self.info['do_fwconn'] = do_fwconn
        self.info['vidName'] = vName

        # all except actor #0 should expect a statefile from its neighbor
        if aNum is not 0:
            self.command = "seti:expect_statefile:1"
        else:
            # actor #0 doesn't need to listen at all
            self.command = "close_listen:"

    def post_transition(self):
        lsnport = int(self.info.get('lsnport'))
        if lsnport is None:
            raise Exception("Error: got OK:LISTEN but no corresponding INFO for lsnport")

        (lsnip, _) = self.sock.getpeername()
        neighbor = self.actorNum - 1
        ServerInfo.states[neighbor].info['connecthost'] = (lsnip, lsnport)

        # let the neighbor know that its info has been updated
        ServerInfo.states[neighbor].info_updated()

        return self.nextState(self)

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
    ServerInfo.states = states

    def handle_server_sock(ls):
        (ns, _) = ls.accept()
        ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        ns.setblocking(False)

        nstate = XCEncSetNeighborConnectState(ns, len(states), "bbb", len(states) is not NUM_PARTS - 1)
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
