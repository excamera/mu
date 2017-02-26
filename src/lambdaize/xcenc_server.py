#!/usr/bin/python

import os

from libmu import util, server, TerminalState, CommandListState, ForLoopState, OnePassState, ErrorState

class ServerInfo(object):
    states = []
    host_addr = None
    port_number = 13579

    state_srv_addr = '127.0.0.1'
    state_srv_port = 13337
    state_srv_threads = 1

    upload_states = False

    quality_y = 30
    quality_s = None
    quality_str = "30_x"

    video_name = "sintel-1k-y4m_06"
    num_offset = 0
    num_parts = 1
    overprovision = 25

    tot_passes = 9
    keyframe_distance = None
    num_passes = (1, 0, 3, 2)
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

    xcenc_invocation = "##INSTATEWAIT## ./xc-enc ##QUALITY## -w 0.75 -i y4m -O \"##TMPDIR##/final.state\" -o \"##TMPDIR##/output.ivf\" ##INSTATESWITCH## \"##TMPDIR##/input.y4m\" 2>&1"
    vpxenc_invocation = "./vpxenc --ivf -q --codec=vp8 --good --cpu-used=0 --end-usage=cq --min-q=0 --max-q=63 --cq-level=##QUALITY## --buf-initial-sz=10000 --buf-optimal-sz=20000 --buf-sz=40000 --undershoot-pct=100 --passes=2 --auto-alt-ref=1 --threads=1 --token-parts=0 --tune=ssim --target-bitrate=4294967295 -o \"##TMPDIR##/output.ivf\" \"##TMPDIR##/input.y4m\""

    hashed_names = False
    ### commands look like this:
    # PHASE 1 # vpxenc and then xc-dump
    # PHASE 2 # xc-enc             -i y4m -O final.state -o output.ivf -r -I 0.state           -p prev.ivf                      input.y4m 2>&1
    # PHASE 3 # xc-enc             -i y4m -O final.state -o output.ivf -r -I $(($j - 1)).state -p prev.ivf -S $(($j - 2)).state input.y4m 2>&1
    # PHASE 4 # xc-enc --refine-sw -i y4m -O final.state -o output.ivf -r -I $(($j - 1)).state -p prev.ivf -S $(($j - 2)).state input.y4m 2>&1
    # NOTE the run immediately after the first run in phase 3 adds --reencode-first-frame
    # NOTE final run of phase 4 adds --fix-prob-tables

class FinalState(TerminalState):
    extra = "(done)"

    def __init__(self, prevState, aNum=0):
        super(FinalState, self).__init__(prevState, aNum)
        if not self.info.get('converged', False):
            # we didn't converge. reclass ourselves as an error state.
            self.__class__ = ErrorState
            self.err = "Convergence check failed for state %d" % self.actorNum

class XCEncQuitState(OnePassState):
    extra = "(sending quit)"
    command = "quit:"
    expect = None
    nextState = FinalState

class XCEncFinishState(CommandListState):
    extra = "(u/l states)"
    pipelined = True
    # NOTE it's OK to pipeline this because we'll get three "UPLOAD(" responses in *some* order
    #      if bg_silent were false, we'd have to use a SuperpositionState to run the uploads in parallel
    nextState = XCEncQuitState
    commandlist = [ (None, "upload:{0}/out_{2}/{1}.ivf\0##TMPDIR##/output.ivf")
                  , ("OK:UPLOAD(", "upload:{0}/first_{2}/{1}.ivf\0##TMPDIR##/first.ivf")
                  , ("OK:UPLOAD(", "upload:{0}/final_state_{2}/{1}.state\0##TMPDIR##/final.state")
                  , ("OK:UPLOAD(", "upload:{0}/prev_state_{2}/{1}.state\0##TMPDIR##/prev.state")
                  , ("OK:UPLOAD(", "upload:{0}/comp_txt_{2}/{1}.txt\0##TMPDIR##/comp.txt")
                  , ("OK:UPLOAD(", None)
                  ]

    def __init__(self, prevState, aNum=0):
        if not ServerInfo.upload_states:
            self.commandlist = [ self.commandlist[i] for i in (0, 5) ]
        elif prevState.actorNum == 0 or prevState.info.get('converged', False):
            self.commandlist = [ self.commandlist[i] for i in (0, 1, 2, 5) ]

        super(XCEncFinishState, self).__init__(prevState, aNum)

        pStr = "%08d" % (self.actorNum + ServerInfo.num_offset)
        vName = ServerInfo.video_name
        qStr = ServerInfo.quality_str
        self.commands = [ s.format(vName, pStr, qStr) if s is not None else None for s in self.commands ]

class XCEncCheckConvergedState(OnePassState):
    extra = "(converged?)"
    command = None
    expect = "OK:RETVAL("
    nextState = XCEncFinishState

    def post_transition(self):
        last_msg = self.messages[-1]
        self.info['converged'] = self.actorNum < ServerInfo.tot_passes - 1 or last_msg[:12] == "OK:RETVAL(0)"
        return self.nextState(self)

class XCEncCompareState(OnePassState):
    extra = "(comp-states)"
    expect = None
    command = "run:test ! -f \"##TMPDIR##/prev.state\" || ./comp-states \"##TMPDIR##/prev.state\" \"##TMPDIR##/final.state\" >> \"##TMPDIR##\"/comp.txt"
    nextState = XCEncCheckConvergedState

class XCEncDumpState(CommandListState):
    extra = "(xc-dump)"
    nextState = TerminalState
    pipelined = True
    commandlist = [ (None, "run:./xc-dump \"##TMPDIR##/output.ivf\" \"##TMPDIR##/final.state\"")
                  , ("OK:RETVAL(0)", "run:cp \"##TMPDIR##/output.ivf\" \"##TMPDIR##/first.ivf\"")
                  , ("OK:RETVAL(0)", None)
                  ]

class XCEncRunState(CommandListState):
    extra = "(encode)"
    pipelined = False
    commandlist = [ ("OK:RETVAL(0)", "run:{0}")
                  , ("OK:RETVAL(0)", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncRunState, self).__init__(prevState, aNum)

        pass_num = self.info['iter_key']
        if pass_num == 0:
            if ServerInfo.keyframe_distance == 1 and not ServerInfo.upload_states:
                self.nextState = XCEncFinishState
            else:
                self.nextState = XCEncDumpState

            cmdstring = ServerInfo.vpxenc_invocation
        else:
            cmdstring = ServerInfo.xcenc_invocation

        self.commands = [ s.format(cmdstring) if s is not None else None for s in self.commands ]

    def str_extra(self):
        xstr = super(XCEncRunState, self).str_extra()
        return "%s/%d" % (xstr, self.info['iter_key'])

class XCEncPreRunState(CommandListState):
    extra = "(pre-encode)"
    pipelined = True
    nextState = XCEncRunState
    commandlist = [ (None, "seti:run_iter:{0}")
                  , "set:cmdquality:{1}"
                  , "seti:send_statefile:{2}"
                  , "run:test ! -f \"##TMPDIR##/final.state\" || cp \"##TMPDIR##/final.state\" \"##TMPDIR##/prev.state\""
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEncPreRunState, self).__init__(prevState, aNum)

        qstring = ''
        pass_num = self.info['iter_key']
        if pass_num < ServerInfo.num_passes[0]:
            self.info['need_reencode'] = False
            qstring = str(ServerInfo.quality_y)

        elif pass_num < sum(ServerInfo.num_passes[:2]):
            pass

        elif pass_num < sum(ServerInfo.num_passes[:3]):
            if ServerInfo.quality_s is not None:
                qstring = "--s-ac-qi %d" % ServerInfo.quality_s

        else:
            qstring = "--refine-sw"
            if pass_num == ServerInfo.tot_passes - 1:
                qstring += " --fix-prob-tables"

        if self.info['need_reencode']:
            qstring += " --reencode-first-frame"
            self.info['need_reencode'] = False

        elif pass_num == sum(ServerInfo.num_passes[:2]):
            self.info['need_reencode'] = True

        kfDist = ServerInfo.keyframe_distance
        if kfDist is not None:
            effActNum = self.info['effActNum']
        else:
            effActNum = self.actorNum

        tell_pass_num = pass_num
        if pass_num == 2 and kfDist is not None:
            # the amazing flying goose
            tell_pass_num = effActNum

        send_statefile = 1
        dist_from_end = (kfDist if kfDist is not None else ServerInfo.num_passes) - effActNum
        if dist_from_end == 1 or (kfDist is not None and pass_num == 1 and effActNum != 1):
            send_statefile = 0

        self.commands = [ s.format(tell_pass_num, qstring, send_statefile) if s is not None else None for s in self.commands ]

class XCEncLoopState(ForLoopState):
    extra = "(encode)"
    loopState = XCEncPreRunState
    exitState = XCEncCompareState

    def __init__(self, prevState, aNum=0):
        super(XCEncLoopState, self).__init__(prevState, aNum)

        # we need at most actorNum + 1 passes
        if ServerInfo.keyframe_distance is None:
            self.iterFin = min(ServerInfo.tot_passes, self.actorNum + 1)
        else:
            self.iterFin = min(3, 1 + (self.info['effActNum']))
            self.exitState = XCEncFinishState
            self.info['converged'] = True

# need to do this here to avoid use-before-def
XCEncRunState.nextState = XCEncLoopState
XCEncDumpState.nextState = XCEncLoopState

class XCEncSettingsState(CommandListState):
    extra = "(setup)"
    nextState = XCEncLoopState
    pipelined = True
    commandlist = [ ("OK:HELLO", "connect:{4}:HELLO_STATE:{2}:{1}:{3}")
                  , "retrieve:{0}/{1}.y4m\0##TMPDIR##/input.y4m"
                  , ("OK:RETRIEVE(", None)
                  ]

    def __init__(self, prevState, aNum=0, gNum=0):
        super(XCEncSettingsState, self).__init__(prevState, aNum)
        pNum = self.actorNum + ServerInfo.num_offset
        nNum = pNum + 1
        pStr = "%08d" % pNum
        vName = ServerInfo.video_name
        if ServerInfo.client_uniq is None:
            ServerInfo.client_uniq = util.rand_str(16)
        rStr = ServerInfo.client_uniq

        if ServerInfo.keyframe_distance is not None:
            self.info['effActNum'] = self.actorNum % ServerInfo.keyframe_distance

        port_number = ServerInfo.state_srv_port + (gNum % ServerInfo.state_srv_threads)
        stateAddr = "%s:%d" % (ServerInfo.state_srv_addr, port_number)
        self.commands = [ s.format(vName, pStr, rStr, nNum, stateAddr) if s is not None else None for s in self.commands ]

def run():
    server.server_main_loop(ServerInfo.states, XCEncSettingsState, ServerInfo)

def main():
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode": 1
            , "port": ServerInfo.port_number
            , "addr": ServerInfo.host_addr
            , "nonblock": 1
            , "bg_silent": 1
            , "minimal_recode": 1 if ServerInfo.keyframe_distance is not None else 0
            , "expect_statefile": 1
            , "cacert": ServerInfo.cacert
            , "srvcrt": ServerInfo.srvcrt
            , "srvkey": ServerInfo.srvkey
            , "bucket": ServerInfo.bucket
            , "hash_s3keys": 1 if ServerInfo.hashed_names else 0
            }
    server.server_launch(ServerInfo, event, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

    # run the server
    run()

if __name__ == "__main__":
    main()
