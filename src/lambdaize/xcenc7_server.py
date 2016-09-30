#!/usr/bin/python

import os

from libmu import util, server, TerminalState, CommandListState, OnePassState

class ServerInfo(object):
    states = []
    host_addr = None
    port_number = 13579

    state_srv_addr = '127.0.0.1'
    state_srv_port = 13337
    state_srv_threads = 1

    upload_states = False

    quality_y = 30
    quality_str = "30_x"

    video_name = "sintel-1k-y4m"
    num_offset = 0
    num_parts = 1
    num_frames = 6
    overprovision = 25

    keyframe_distance = 16

    lambda_function = "xcenc7"
    regions = ["us-east-1"]
    bucket = "excamera-us-east-1"
    out_file = None
    profiling = None

    cacert = None
    srvcrt = None
    srvkey = None

    client_uniq = None


class FinalState(TerminalState):
    extra = "(done)"

class XCEnc7QuitState(OnePassState):
    extra = "(sending quit)"
    command = "quit:"
    expect = None
    nextState = FinalState

class XCEnc7FinishState(CommandListState):
    extra = "(u/l states)"
    pipelined = True
    # NOTE it's OK to pipeline this because we'll get three "UPLOAD(" responses in *some* order
    #      if bg_silent were false, we'd have to use a SuperpositionState to run the uploads in parallel
    nextState = XCEnc7QuitState
    commandlist = [ (None, "upload:{0}/out_{2}/{1}.ivf\0##TMPDIR##/output.ivf")
                  , ("OK:UPLOAD(", "upload:{0}/final_state_{2}/{1}.state\0##TMPDIR##/final.state")
                  , ("OK:UPLOAD(", None)
                  ]

    def __init__(self, prevState, aNum=0):
        if not ServerInfo.upload_states or ServerInfo.keyframe_distance < 2:
            self.commandlist = [ self.commandlist[i] for i in (0, 2) ]

        super(XCEnc7FinishState, self).__init__(prevState, aNum)

        pStr = "%08d" % (self.actorNum + ServerInfo.num_offset)
        vName = ServerInfo.video_name + ("_%02d" % ServerInfo.num_frames)
        qStr = ServerInfo.quality_str
        self.commands = [ s.format(vName, pStr, qStr) if s is not None else None for s in self.commands ]

class XCEnc7PreFinishState(OnePassState):
    extra = "(working)"
    expect = "OK:RETVAL(0)"
    command = None
    nextState = XCEnc7FinishState

class XCEnc7RecodeState(CommandListState):
    extra = "(encode/wait)"
    pipelined = True
    nextState = XCEnc7PreFinishState
    commandlist = [ ("OK:RETVAL(0)", "seti:run_iter:{0}")
                  , "seti:send_statefile:{2}"
                  , "run:( while [ ! -f {1} ]; do sleep 0.025; done; echo \"hi\" ) | ./xc-enc -e -w 0.75 -i y4m -O \"##TMPDIR##/final.state\" -o \"##TMPDIR##/output.ivf\" -r -I {1} -p \"##TMPDIR##/prev.ivf\" \"##TMPDIR##/input.y4m\" 2>&1"
                  ]

    def __init__(self, prevState, aNum=0):
        super(XCEnc7RecodeState, self).__init__(prevState, aNum)

        kfDist = ServerInfo.keyframe_distance
        tell_pass_num = self.actorNum % kfDist
        state_num = tell_pass_num - 1
        state_file = "\"##TMPDIR##/%d.state\"" % state_num

        # send statefile unless we have no forward neighbor
        send_statefile = 1
        if kfDist - tell_pass_num == 1 or ServerInfo.num_parts - self.actorNum == 1:
            send_statefile = 0

        self.commands = [ s.format(tell_pass_num, state_file, send_statefile) if s is not None else None for s in self.commands ]

class XCEnc7DumpState(OnePassState):
    extra = "(vpxenc)"
    expect = "OK:RETVAL(0)"
    command = "run:./xc-terminate-chunk \"##TMPDIR##/prev.ivf\" \"##TMPDIR##/output.ivf\" \"##TMPDIR##/final.state\""
    nextState = XCEnc7PreFinishState

class XCEnc7EncodeState(OnePassState):
    extra = "(d/l)"
    expect = "OK:RETRIEV"
    command = "run:./vpxenc --ivf -q --codec=vp8 --good --cpu-used=0 --end-usage=cq --min-q=0 --max-q=63 --cq-level={0} --buf-initial-sz=10000 --buf-optimal-sz=20000 --buf-sz=40000 --undershoot-pct=100 --passes=2 --auto-alt-ref=1 --threads=1 --token-parts=0 --tune=ssim --target-bitrate=4294967295 -o \"##TMPDIR##/{1}\" \"##TMPDIR##/input.y4m\""
    nextState = XCEnc7RecodeState

    def __init__(self, prevState, aNum=0):
        super(XCEnc7EncodeState, self).__init__(prevState, aNum)

        outfile = "prev.ivf"
        if ServerInfo.keyframe_distance < 2:
            self.nextState = XCEnc7PreFinishState
            outfile = "output.ivf"
        elif self.actorNum % ServerInfo.keyframe_distance == 0:
            self.nextState = XCEnc7DumpState

        self.command = self.command.format(str(ServerInfo.quality_y), outfile)

class XCEnc7StartState(CommandListState):
    extra = "(starting)"
    nextState = XCEnc7EncodeState
    pipelined = True
    commandlist = [ ("OK:HELLO", "connect:{4}:HELLO_STATE:{2}:{1}:{3}")
                  , "seti:run_iter:0"
                  , "seti:send_statefile:{5}"
                  , "retrieve:{0}/{1}.y4m\0##TMPDIR##/input.y4m"
                  ]

    def __init__(self, prevState, aNum=0, gNum=0):
        super(XCEnc7StartState, self).__init__(prevState, aNum)
        pNum = self.actorNum + ServerInfo.num_offset
        nNum = pNum + 1
        pStr = "%08d" % pNum

        vName = ServerInfo.video_name
        effActNum = self.actorNum % ServerInfo.keyframe_distance
        if effActNum != 0:
            vName += ("_%02d" % (ServerInfo.num_frames + 1))
        else:
            vName += ("_%02d" % ServerInfo.num_frames)

        if ServerInfo.client_uniq is None:
            ServerInfo.client_uniq = util.rand_str(16)
        rStr = ServerInfo.client_uniq

        port_number = ServerInfo.state_srv_port + (gNum % ServerInfo.state_srv_threads)
        stateAddr = "%s:%d" % (ServerInfo.state_srv_addr, port_number)

        # send statefile unless we have no forward neighbor
        send_statefile = 1
        if ServerInfo.keyframe_distance - effActNum == 1:
            send_statefile = 0

        self.commands = [ s.format(vName, pStr, rStr, nNum, stateAddr, send_statefile) if s is not None else None for s in self.commands ]

def run():
    server.server_main_loop(ServerInfo.states, XCEnc7StartState, ServerInfo)

def main():
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode": 1
            , "port": ServerInfo.port_number
            , "addr": ServerInfo.host_addr
            , "nonblock": 1
            , "bg_silent": 1
            , "minimal_recode": 1
            , "expect_statefile": 1
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
