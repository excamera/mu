#!/usr/bin/python

import os

from libmu import util, server, TerminalState, CommandListState, SuperpositionState, ForLoopState

class ServerInfo(object):
    states = []
    port_number = 13579

    state_srv_addr = '127.0.0.1'
    state_srv_port = 13337

    quality_s = 127
    quality_y = 30

    video_name = "sintel-1k-y4m_06"
    num_offset = 0
    num_parts = 1

    tot_passes = 9
    num_passes = (1, 3, 3, 2)
    min_passes = (1, 0, 1, 2)

    lambda_function = "xcenc"
    regions = ["us-east-1"]
    bucket = "excamera-us-east-1"
    out_file = None
    profiling = None

    cacert = None
    srvcrt = None
    srvkey = None

    client_uniq = None

class FinalState(TerminalState):
    extra = "(finished)"

class XCEncFinishState(CommandListState):
    extra = "(uploading comparison data and quitting command)"
    nextState = FinalState
    commandlist = [ (None, "set:fromfile:##TMPDIR##/comp.txt")
                  , "set:outkey:{0}/comp_txt/{1}.txt"
                  , "upload:"
                  , ("OK:UPLOADING", None)
                  , ("OK:UPLOAD(", "quit:")
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncFinishState, self).__init__(prevState, aNum)
        if self.actorNum > 0:
            pStr = "%08d" % (self.actorNum + ServerInfo.num_offset)
            vName = ServerInfo.video_name
            self.commands = [ s.format(vName, pStr) if s is not None else None for s in self.commands ]
        else:
            # actor #0 has nothing to upload because it never compares
            self.commands[0] = "quit:"
            del self.commands[1:]
            del self.expects[1:]


class XCEncCompareState(CommandListState):
    extra = "(comparing states)"
    nextState = TerminalState
    commandlist = [ (None, "run:test ! -f \"##TMPDIR##/prev.state\" || ./comp-states \"##TMPDIR##/prev.state\" \"##TMPDIR##/final.state\" >> \"##TMPDIR##\"/comp.txt")
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", None)
                  ]

class XCEncUploadState(CommandListState):
    extra = "(uploading result)"
    nextState = TerminalState
    commandlist = [ (None, "upload:")
                  , ("OK:UPLOADING", None)
                  , ("OK:UPLOAD(", None)
                  ]

class XCEncUploadAndCompare(SuperpositionState):
    nextState = XCEncFinishState
    state_constructors = [XCEncUploadState, XCEncCompareState]

class XCEncRunState(CommandListState):
    extra = "(running xc-enc)"
    commandlist = [ (None, "seti:run_iter:{0}")
                  , "set:cmdquality:{1}"
                  , "run:test ! -f \"##TMPDIR##/final.state\" || cp \"##TMPDIR##/final.state\" \"##TMPDIR##/prev.state\""
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", "run:##INSTATEWAIT## ./xc-enc ##QUALITY## -i y4m -O \"##TMPDIR##/final.state\" -o \"##TMPDIR##/output.ivf\" ##INSTATESWITCH## \"##TMPDIR##/input.y4m\" 2>&1")
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", None)
                  ]
    ### commands look like this:
    # PHASE 1 # ${XC_ENC} --y-ac-qi ${Y_AC_QI}             -i y4m -O final.state -o output.ivf                                                          input.y4m 2>&1
    # PHASE 2 # ${XC_ENC}                                  -i y4m -O final.state -o output.ivf -r -I 0.state           -p prev.ivf                      input.y4m 2>&1
    # PHASE 3 # ${XC_ENC} --s-ac-qi ${S_AC_QI}             -i y4m -O final.state -o output.ivf -r -I $(($j - 1)).state -p prev.ivf -S $(($j - 2)).state input.y4m 2>&1
    # PHASE 4 # ${XC_ENC} --s-ac-qi ${S_QC_QI} --refine-sw -i y4m -O final.state -o output.ivf -r -I $(($j - 1)).state -p prev.ivf -S $(($j - 2)).state input.y4m 2>&1

    def __init__(self, prevState, aNum=0):
        super(XCEncRunState, self).__init__(prevState, aNum)

        pass_num = self.info['iter_key']
        if pass_num < ServerInfo.num_passes[0]:
            qstring = "--y-ac-qi %d" % ServerInfo.quality_y
        elif pass_num < sum(ServerInfo.num_passes[:2]):
            qstring = ""
        elif pass_num < sum(ServerInfo.num_passes[:3]):
            qstring = "--s-ac-qi %d" % ServerInfo.quality_s
        else:
            qstring = "--s-ac-qi %d --refine-sw" % ServerInfo.quality_s
        self.commands = [ s.format(self.info['iter_key'], qstring) if s is not None else None for s in self.commands ]

class XCEncLoopState(ForLoopState):
    extra = "(xc-enc looping)"
    loopState = XCEncRunState
    exitState = XCEncUploadAndCompare

    def __init__(self, prevState, aNum=0):
        super(XCEncLoopState, self).__init__(prevState, aNum)

        # we need at most actorNum + 1 passes
        self.iterFin = min(ServerInfo.tot_passes, self.actorNum + 1)

# need to do this here to avoid use-before-def
XCEncRunState.nextState = XCEncLoopState

class XCEncSettingsState(CommandListState):
    extra = "(preparing worker)"
    pipelined = True
    nextState = XCEncLoopState
    commandlist = [ ("OK:HELLO", "set:inkey:{0}/{1}.y4m")
                  , "set:targfile:##TMPDIR##/input.y4m"
                  , "set:fromfile:##TMPDIR##/output.ivf"
                  , "set:outkey:{0}/out/{1}.ivf"
                  , "seti:expect_statefile:{4}"
                  , "seti:send_statefile:{5}"
                  , "connect:{6}:HELLO_STATE:{2}:{1}:{3}"
                  , "retrieve:"
                  , ("OK:RETRIEVING", None)
                  , ("OK:RETRIEVE(", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncSettingsState, self).__init__(prevState, aNum)
        pNum = self.actorNum + ServerInfo.num_offset
        nNum = pNum + 1
        pStr = "%08d" % pNum
        vName = ServerInfo.video_name
        if ServerInfo.client_uniq is None:
            ServerInfo.client_uniq = util.rand_str(16)
        rStr = ServerInfo.client_uniq
        expS = 1 if self.actorNum != 0 else 0
        sndS = 1 if self.actorNum != ServerInfo.num_parts - 1 else 0
        stateAddr = "%s:%d" % (ServerInfo.state_srv_addr, ServerInfo.state_srv_port)
        self.commands = [ s.format(vName, pStr, rStr, nNum, expS, sndS, stateAddr) if s is not None else None for s in self.commands ]

def run():
    server.server_main_loop(ServerInfo.states, XCEncSettingsState, ServerInfo)

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
