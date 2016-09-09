#!/usr/bin/python

import os

from libmu import server, TerminalState, CommandListState, OnePassState, IfElseState, SuperpositionState, InfoWatcherState, ForLoopState

class ServerInfo(object):
    states = []
    port_number = 13579

    quality_s = 127
    quality_y = 30

    video_name = "sintel-1k-y4m_06"
    num_offset = 0
    num_parts = 1
    num_passes = 7
    lambda_function = "xcenc"
    regions = ["us-east-1"]
    bucket = "excamera-us-east-1"
    out_file = None
    profiling = None

    cacert = None
    srvcrt = None
    srvkey = None

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
    #pipelined = True
    commandlist = [ (None, "seti:run_iter:{0}")
                  , "run:##INSTATEWAIT## ./xc-enc ##QUALITY## -i y4m -O \"##TMPDIR##/final.state\" -o \"##TMPDIR##/output.ivf\" ##INSTATESWITCH## \"##TMPDIR##/input.y4m\" 2>&1"
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", "set:cmdquality:--s-ac-qi {1}")
                  , None
                  ]
    ### commands look like this:
    # ${XC_ENC} --y-ac-qi ${Y_AC_QI} -i y4m -O final.state -o output.ivf                                                          input.y4m 2>&1
    # ${XC_ENC} --s-ac-qi ${S_AC_QI} -i y4m -O final.state -o output.ivf -r -I 0.state           -p prev.ivf                      input.y4m 2>&1
    # ${XC_ENC} --s-ac-qi ${S_AC_QI} -i y4m -O final.state -o output.ivf -r -I $(($j - 1)).state -p prev.ivf -S $(($j - 2)).state input.y4m 2>&1

    def __init__(self, prevState, aNum=0):
        super(XCEncRunState, self).__init__(prevState, aNum)
        self.commands = [ s.format(self.info['iter_key'], ServerInfo.quality_s) if s is not None else None for s in self.commands ]

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
    #pipelined = True
    nextState = XCSetupConnect
    commandlist = [ "set:inkey:{0}/{1}.y4m"
                  , "set:targfile:##TMPDIR##/input.y4m"
                  , "set:fromfile:##TMPDIR##/output.ivf"
                  , "set:cmdquality:--y-ac-qi {2}"
                  , "set:outkey:{0}/out/{1}.ivf"
                  , "retrieve:"
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncSettingsState, self).__init__(prevState, aNum)
        pNum = self.actorNum + ServerInfo.num_offset
        vName = ServerInfo.video_name
        self.commands = [ s.format(vName, "%08d" % pNum, ServerInfo.quality_y) if s is not None else None for s in self.commands ]

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

def run():
    server.server_main_loop(ServerInfo.states, XCEncSetNeighborConnectState, ServerInfo)

def main():
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode": 2
            , "port": ServerInfo.port_number
            , "addr": None  # server_launch will fill this in for us
            , "nonblock": 1
            , "cacert": ServerInfo.cacert
            , "srvcrt": ServerInfo.srvcrt
            , "srvkey": ServerInfo.srvkey
            , "bucket": ServerInfo.bucket
            }
    server.server_launch(ServerInfo, event, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

    # run the server
    run()

if __name__ == "__main__":
    main()
