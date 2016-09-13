#!/usr/bin/python

import os

from libmu import util, server, TerminalState, CommandListState, SuperpositionState, ForLoopState, OnePassState, ErrorState

class ServerInfo(object):
    states = []
    port_number = 13579

    state_srv_addr = '127.0.0.1'
    state_srv_port = 13337

    upload_states = False

    quality_s = 127
    quality_y = 30

    video_name = "sintel-1k-y4m_06"
    num_offset = 0
    num_parts = 1
    overprovision = 25

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

    xcenc_invocation = "##INSTATEWAIT## ./xc-enc ##QUALITY## -i y4m -O \"##TMPDIR##/final.state\" -o \"##TMPDIR##/output.ivf\" ##INSTATESWITCH## \"##TMPDIR##/input.y4m\" 2>&1"
    vpxenc_invocation = "./vpxenc -q --codec=vp8 --good --cpu-used=0 --end-usage=cq --min-q=0 --max-q=63 --cq-level=##QUALITY## --buf-initial-sz=10000 --buf-optimal-sz=20000 --buf-sz=40000 --undershoot-pct=100 --passes=2 --auto-alt-ref=1 --threads=1 --token-parts=0 --tune=ssim --target-bitrate=4294967295 -o \"##TMPDIR##/output.ivf\" \"##TMPDIR##/input.y4m\""
    ### commands look like this:
    # PHASE 1 # vpxenc and then xc-dump
    # PHASE 2 # xc-enc                                  -i y4m -O final.state -o output.ivf -r -I 0.state           -p prev.ivf                      input.y4m 2>&1
    # PHASE 3 # xc-enc --s-ac-qi ${S_AC_QI}             -i y4m -O final.state -o output.ivf -r -I $(($j - 1)).state -p prev.ivf -S $(($j - 2)).state input.y4m 2>&1
    # PHASE 4 # xc-enc --s-ac-qi ${S_QC_QI} --refine-sw -i y4m -O final.state -o output.ivf -r -I $(($j - 1)).state -p prev.ivf -S $(($j - 2)).state input.y4m 2>&1

class FinalState(TerminalState):
    extra = "(finished)"

    def __init__(self, prevState, aNum=0):
        super(FinalState, self).__init__(prevState, aNum)
        if not self.info.get('converged', False):
            # we didn't converge. reclass ourselves as an error state.
            self.__class__ = ErrorState
            self.err = "Convergence check failed for state %d" % self.actorNum

class XCEncComputeSSIMState(CommandListState):
    extra = "(computing SSIM)"
    nextState = TerminalState
    commandlist = [ (None, "run:echo {2} > \"##TMPDIR##/ssim.txt\"")
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", "run:./xc-framesize \"##TMPDIR##/output.ivf\" >> \"##TMPDIR##/ssim.txt\"")
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", "run:echo \"##TMPDIR##/output.ivf\" | ./xc-decode-bundle {4} > \"##TMPDIR##/output.y4m\"")
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", "run:./dump_ssim \"##TMPDIR##/input.y4m\" \"##TMPDIR##/output.y4m\" >> \"##TMPDIR##/ssim.txt\"")
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", "run:rm \"##TMPDIR##/output.y4m\"")
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", "set:outkey:{0}/ssim_{3}/{1}.txt")
                  , "set:fromfile:##TMPDIR##/ssim.txt"
                  , "upload:"
                  , ("OK:UPLOADING", None)
                  , ("OK:UPLOAD(", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncComputeSSIMState, self).__init__(prevState, aNum)
        self.nextState = self.info['after_ssim_state']
        if self.nextState == FinalState:
            self.commands[-1] = "quit:"

        vName = ServerInfo.video_name
        pStr = "%08d" % (self.actorNum + ServerInfo.num_offset)
        qStr = self.info['ssim_quality_string']
        qnStr = self.info['ssim_quality_key']
        iState = self.info['decode_input_state']
        self.commands = [ s.format(vName, pStr, qStr, qnStr, iState) if s is not None else None for s in self.commands ]

class XCEncFinishState(CommandListState):
    extra = "(uploading comparison data and quitting command)"
    nextState = FinalState
    commandlist = [ (None, "set:fromfile:##TMPDIR##/comp.txt")
                  , "set:outkey:{0}/comp_txt/{1}.txt"
                  , "upload:"
                  , ("OK:UPLOADING", None)
                  , ("OK:UPLOAD(", "set:fromfile:##TMPDIR##/prev.state")
                  , "set:outkey:{0}/prev_state/{1}.state"
                  , "upload:"
                  , ("OK:UPLOADING", None)
                  , ("OK:UPLOAD(", "set:fromfile:##TMPDIR##/final.state")
                  , "set:outkey:{0}/final_state/{1}.state"
                  , "upload:"
                  , ("OK:UPLOADING", None)
                  , ("OK:UPLOAD(", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncFinishState, self).__init__(prevState, aNum)
        if self.actorNum > 0 and ServerInfo.upload_states:
            pStr = "%08d" % (self.actorNum + ServerInfo.num_offset)
            vName = ServerInfo.video_name
            self.commands = [ s.format(vName, pStr) if s is not None else None for s in self.commands ]
            self.nextState = XCEncComputeSSIMState
            self.info['after_ssim_state'] = FinalState
            self.info['ssim_quality_string'] = "--y-ac-qi=%d --s-ac-qi=%d" % (ServerInfo.quality_y, ServerInfo.quality_s)
            self.info['ssim_quality_key'] = "%d_%d_final" % (ServerInfo.quality_y, ServerInfo.quality_s)
            self.info['decode_input_state'] = "\"##TMPDIR##/prev.state\""
        else:
            # actor #0 has nothing to upload because it never compares
            self.commands[0] = "quit:"
            del self.commands[1:]
            del self.expects[1:]

class XCEncCheckConvergedState(OnePassState):
    extra = "(checking result of comparison)"
    command = None
    expect = "OK:RETVAL("
    nextState = TerminalState

    def post_transition(self):
        last_msg = self.messages[-1]
        self.info['converged'] = self.actorNum < ServerInfo.tot_passes - 1 or last_msg[:12] == "OK:RETVAL(0)"
        return self.nextState(self)

class XCEncCompareState(CommandListState):
    extra = "(comparing states)"
    nextState = XCEncCheckConvergedState
    commandlist = [ (None, "run:test ! -f \"##TMPDIR##/prev.state\" || ./comp-states \"##TMPDIR##/prev.state\" \"##TMPDIR##/final.state\" >> \"##TMPDIR##\"/comp.txt")
                  , ("OK:RUNNING", None)
                  ]

class XCEncUploadState(CommandListState):
    extra = "(uploading result)"
    pipelined = True
    nextState = TerminalState
    keyString = "out"
    thenState = None
    commandlist = [ (None, "set:fromfile:##TMPDIR##/output.ivf")
                  , "set:outkey:{0}/{2}/{1}.ivf"
                  , "upload:"
                  , ("OK:UPLOADING", None)
                  , ("OK:UPLOAD(", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncUploadState, self).__init__(prevState, aNum)
        vName = ServerInfo.video_name
        pStr = "%08d" % (self.actorNum + ServerInfo.num_offset)
        kStr = self.keyString
        self.info[''] = self.thenState
        self.commands = [ s.format(vName, pStr, kStr) if s is not None else None for s in self.commands ]

class XCEncUploadAndCompare(SuperpositionState):
    nextState = XCEncFinishState
    state_constructors = [XCEncUploadState, XCEncCompareState]

class XCEncUploadFirstIVFState(XCEncUploadState):
    extra = "(uploading first IVF output)"
    nextState = XCEncComputeSSIMState
    thenState = TerminalState
    keyString = "first"

    def __init__(self, prevState, aNum=0):
        super(XCEncUploadFirstIVFState, self).__init__(prevState, aNum)
        self.info['after_ssim_state'] = self.thenState
        self.info['ssim_quality_string'] = "--y-ac-qi=%d" % ServerInfo.quality_y
        self.info['ssim_quality_key'] = "%d_%d_first" % (ServerInfo.quality_y, ServerInfo.quality_s)
        self.info['decode_input_state'] = ""

class XCEncDumpState(CommandListState):
    extra = "(running xc-dump to get output state from vpxenc)"
    nextState = TerminalState
    commandlist = [ (None, "run:./xc-dump \"##TMPDIR##/output.ivf\" \"##TMPDIR##/final.state\"")
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncDumpState, self).__init__(prevState, aNum)
        if ServerInfo.upload_states:
            self.nextState = XCEncUploadFirstIVFState

class XCEncRunState(CommandListState):
    extra = "(running xc-enc)"
    commandlist = [ (None, "seti:run_iter:{0}")
                  , "set:cmdquality:{1}"
                  , "run:test ! -f \"##TMPDIR##/final.state\" || cp \"##TMPDIR##/final.state\" \"##TMPDIR##/prev.state\""
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", "run:{2}")
                  , ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncRunState, self).__init__(prevState, aNum)

        pass_num = self.info['iter_key']
        if pass_num == 0:
            self.nextState = XCEncDumpState
            cmdstring = ServerInfo.vpxenc_invocation
        else:
            cmdstring = ServerInfo.xcenc_invocation

        if pass_num < ServerInfo.num_passes[0]:
            qstring = str(ServerInfo.quality_y)
        elif pass_num < sum(ServerInfo.num_passes[:2]):
            qstring = ""
        elif pass_num < sum(ServerInfo.num_passes[:3]):
            qstring = "--s-ac-qi %d" % ServerInfo.quality_s
        else:
            qstring = "--s-ac-qi %d --refine-sw" % ServerInfo.quality_s
            if pass_num == ServerInfo.tot_passes - 1:
                qstring += " --fix-prob-tables"

        self.commands = [ s.format(self.info['iter_key'], qstring, cmdstring) if s is not None else None for s in self.commands ]

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
XCEncDumpState.nextState = XCEncLoopState
XCEncUploadFirstIVFState.thenState = XCEncLoopState

class XCEncSettingsState(CommandListState):
    extra = "(preparing worker)"
    pipelined = True
    nextState = XCEncLoopState
    commandlist = [ ("OK:HELLO", "set:inkey:{0}/{1}.y4m")
                  , "set:targfile:##TMPDIR##/input.y4m"
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
