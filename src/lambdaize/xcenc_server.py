#!/usr/bin/python

import socket
import sys

from libmu import server, TerminalState, CommandListState, OnePassState, IfElseState, SuperpositionState, InfoWatcherState, ForLoopState

class ServerInfo(object):
    states = []
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

class XCEncMaybeDoConnect(CommandListState):
    extra = "(connecting to neighbor)"
    commandlist = [ (None, None)
                  , ("OK:CONNECT", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncMaybeDoConnect, self).__init__(prevState, aNum)
        # fill in correct command
        if self.info.get('do_fwconn'):
            self.commands[0] = "connect:%s:%d" % self.info['connecthost']
        else:
            del self.commands[1]
            del self.expects[1]

class XCEncWaitForConnectHost(InfoWatcherState):
    extra = "(waiting for neighbor to be ready to accept a connection)"
    nextState = XCEncMaybeDoConnect

    def info_updated(self):
        if self.info.get('connecthost') is not None:
            self.kick()

class XCEncStartConnect(IfElseState):
    extra = "(checking whether neighbor is ready to accept a connection)"
    consequentState = XCEncMaybeDoConnect
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
                  , "set:targfile:##TMPDIR##/{0}{1}.y4m"
                  , "set:cmdinfile:##TMPDIR##/{0}{1}.y4m"
                  , "set:cmdoutfile:##TMPDIR##/{0}{1}.ivf"
                  , "set:fromfile:##TMPDIR##/{0}{1}.ivf"
                  , "set:cmdquality:0.9"
                  , "set:outkey:{0}/out/{0}{1}.ivf"
                  , "seti:nonblock:1"
                  , "seti:rm_tmpdir:0"
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

def handle_server_sock(ls, states, num_parts, basename):
    (ns, _) = ls.accept()
    ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ns.setblocking(False)

    nstate = XCEncSetNeighborConnectState(ns, len(states), basename, len(states) is not num_parts - 1)
    nstate.do_handshake()

    states.append(nstate)

    if len(states) == num_parts:
        # no need to listen any longer, we have all our connections
        try:
            ls.shutdown()
            ls.close()
        except:
            pass

        ls = None

    return ls

def run(num_parts, basename, chainfile=None, keyfile=None):
    server.server_main_loop(ServerInfo.states, handle_server_sock, num_parts, basename, chainfile, keyfile)

if __name__ == "__main__":
    nparts = 1
    if len(sys.argv) > 1:
        nparts = int(sys.argv[1])

    bname = "6bbb"
    if len(sys.argv) > 2:
        bname = sys.argv[2]

    run(nparts, bname)
