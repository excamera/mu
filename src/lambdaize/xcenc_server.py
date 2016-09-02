#!/usr/bin/python

import sys

import pylaunch
from libmu import server, TerminalState, CommandListState, OnePassState, IfElseState, SuperpositionState, InfoWatcherState, ForLoopState

class ServerInfo(object):
    states = []
    video_name = "6bbb"
    num_passes = 4
    num_parts = 1

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
    commandlist = [ (None, "seti:run_iter:")
                  , "run:"
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncRunState, self).__init__(prevState, aNum)
        self.commands[0] = "seti:run_iter:%d" % (self.info['iter_key'])

class XCEncLoopState(ForLoopState):
    extra = "(xc-enc looping)"
    loopState = XCEncRunState
    exitState = XCEncUploadState

    def __init__(self, prevState, aNum=0):
        super(XCEncLoopState, self).__init__(prevState, aNum)

        # we need at most actorNum + 1 passes
        self.iterFin = min(ServerInfo.num_passes, self.actorNum + 1)

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
        if self.info['do_fwconn']:
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
        return (not self.info['do_fwconn']) or (self.info.get('connecthost') is not None)

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
        vName = ServerInfo.video_name
        self.commands = [ s.format(vName, "%06d" % aNum) if s is not None else None for s in self.commands ]

class XCEncSetNeighborConnectState(OnePassState):
    extra = "(waiting for lsnport to report to neighbor)"
    expect = "OK:LISTEN"
    nextState = XCEncSettingsState

    def __init__(self, prevState, aNum):
        super(XCEncSetNeighborConnectState, self).__init__(prevState, aNum)

        # store these for later
        self.info['do_fwconn'] = aNum != (ServerInfo.num_parts - 1)

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

        if self.actorNum is not 0:
            (lsnip, _) = self.sock.getpeername()
            neighbor = self.actorNum - 1
            ServerInfo.states[neighbor].info['connecthost'] = (lsnip, lsnport)

            # let the neighbor know that its info has been updated
            ServerInfo.states[neighbor].info_updated()

        return self.nextState(self)

def run(chainfile=None, keyfile=None):
    server.server_main_loop(ServerInfo.states, XCEncSetNeighborConnectState, ServerInfo.num_parts, chainfile, keyfile)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ServerInfo.num_parts = int(sys.argv[1])

    if len(sys.argv) > 2:
        ServerInfo.video_name = sys.argv[2]

    if len(sys.argv) > 3:
        ServerInfo.num_passes = int(sys.argv[3])

    run()
