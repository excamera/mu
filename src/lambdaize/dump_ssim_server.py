#!/usr/bin/python

import os

from libmu import server, TerminalState, CommandListState

class ServerInfo(object):
    port_number = 13579

    video_name = "sintel-1k-y4m_06"
    num_offset = 0
    num_parts = 1
    overprovision = 25

    lambda_function = "vpxenc"
    regions = ["us-east-1"]
    bucket = "excamera-us-east-1"
    out_file = None
    profiling = None

    cacert = None
    srvcrt = None
    srvkey = None

    num_list = None

class FinalState(TerminalState):
    extra = "(finished)"

class DumpSSIMState(CommandListState):
    extra = "(dumping ssim)"
    pipelined = False
    nextState = FinalState
    commandlist = [ ("OK:RETRIEVE(", "run:echo \"##TMPDIR##/vpx.ivf\" | ./xc-decode-bundle > \"##TMPDIR##/out.y4m\"")
                  , ("OK:RETVAL(0)", "run:./xc-framesize \"##TMPDIR##/vpx.ivf\" > \"##TMPDIR##/out.txt\"")
                  , ("OK:RETVAL(0)", "run:./dump_ssim \"##TMPDIR##/out.y4m\" \"##TMPDIR##/orig.y4m\" >> \"##TMPDIR##/out.txt\"")
                  , ("OK:RETVAL(0)", "run:echo \"##TMPDIR##/xc.ivf\" | ./xc-decode-bundle {2} > \"##TMPDIR##/out.y4m\"")
                  , ("OK:RETVAL(0)", "run:./xc-framesize \"##TMPDIR##/xc.ivf\" >> \"##TMPDIR##/out.txt\"")
                  , ("OK:RETVAL(0)", "run:./dump_ssim \"##TMPDIR##/out.y4m\" \"##TMPDIR##/orig.y4m\" >> \"##TMPDIR##/out.txt\"")
                  , ("OK:RETVAL(0)", "upload:{0}/out_ssim/{1}.txt\0##TMPDIR##/out.txt")
                  , ("OK:UPLOAD(", "quit:")
                  ]

    def __init__(self, prevState, aNum=0):
        super(DumpSSIMState, self).__init__(prevState, aNum)
        vName = ServerInfo.video_name
        pStr = "%08d" % (self.actorNum + ServerInfo.num_offset)
        if self.actorNum == 0:
            stStr = ""
        else:
            stStr = "\"##TMPDIR##/final.state\""
        self.commands = [ s.format(vName, pStr, stStr) if s is not None else None for s in self.commands ]

class DumpSSIMRetrieveState(CommandListState):
    extra = "(retrieving data)"
    # keep this state separate from the next one so that we can parallelize downloading with pipelined commands
    pipelined = True
    nextState = DumpSSIMState
    commandlist = [ ("OK:HELLO", "retrieve:{0}/{1}.y4m\0##TMPDIR##/orig.y4m")
                  , "retrieve:{0}/first/{1}.ivf\0##TMPDIR##/vpx.ivf"
                  , "retrieve:{0}/out/{1}.ivf\0##TMPDIR##/xc.ivf"
                  , "retrieve:{0}/prev_state/{1}.state\0##TMPDIR##/final.state"
                  ]

    def __init__(self, prevState, aNum=0):
        super(DumpSSIMRetrieveState, self).__init__(prevState, aNum)
        vName = ServerInfo.video_name
        pStr = "%08d" % (self.actorNum + ServerInfo.num_offset)
        if self.actorNum == 0:
            del self.commands[-1]
            del self.expects[-1]
        self.commands = [ s.format(vName, pStr) if s is not None else None for s in self.commands ]

def run():
    server.server_main_loop([], DumpSSIMRetrieveState, ServerInfo)

def main():
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode": 1
            , "port": ServerInfo.port_number
            , "addr": None  # server_launch will fill this in for us
            , "nonblock": 1
            , "bg_silent": 1
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
