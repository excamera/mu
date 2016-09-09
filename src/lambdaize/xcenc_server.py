#!/usr/bin/python

import os

from libmu import util, server, TerminalState, CommandListState, OnePassState, IfElseState, SuperpositionState, InfoWatcherState, ForLoopState

class ServerInfo(object):
    states = []
    port_number = 13579

    state_port_host = '127.0.0.1'
    state_port_number = 13337

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

    client_uniq = None

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

class XCEncSettingsState(CommandListState):
    extra = "(preparing worker)"
    #pipelined = True
    nextState = XCEncLoopState
    commandlist = [ ("OK:HELLO", "set:inkey:{0}/{1}.y4m")
                  , "set:targfile:##TMPDIR##/input.y4m"
                  , "set:fromfile:##TMPDIR##/output.ivf"
                  , "set:cmdquality:--y-ac-qi {2}"
                  , "set:outkey:{0}/out/{1}.ivf"
                  , "seti:expect_statefile:{5}"
                  , "seti:send_statefile:{6}"
                  , "connect:127.0.0.1:13337:HELLO_STATE:{3}:{1}:{4}"
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
        qual = ServerInfo.quality_y
        if ServerInfo.client_uniq is None:
            ServerInfo.client_uniq = util.rand_str(16)
        rStr = ServerInfo.client_uniq
        expS = 1 if self.actorNum != 0 else 0
        sndS = 1 if self.actorNum != ServerInfo.num_parts - 1 else 0
        self.commands = [ s.format(vName, pStr, qual, rStr, nNum, expS, sndS) if s is not None else None for s in self.commands ]

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
