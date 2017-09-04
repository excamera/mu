#!/usr/bin/python

import os

from libmu import server, TerminalState, CommandListState

class ServerInfo(object):
    port_number = 13579

    video_name = "sintel-1k-y4m_06"
    num_parts = 1
    num_offset = 0
    lambda_function = "vpxenc"
    regions = ["us-east-1"]
    bucket = "excamera-us-west-1"
    out_file = None
    profiling = None

    cacert = None
    srvcrt = None
    srvkey = None

class FinalState(TerminalState):
    extra = "(finished)"

class VPXEncStateMachine(CommandListState):
    nextState = FinalState
    commandlist = [ ("OK:HELLO", "set:inkey:{0}/{1}.y4m")
                  , "set:targfile:##TMPDIR##/{1}.y4m"
                  , "set:cmdinfile:##TMPDIR##/{1}.y4m"
                  , "set:cmdoutfile:##TMPDIR##/{1}.ivf"
                  , "set:fromfile:##TMPDIR##/{1}.ivf"
                  , "set:outkey:{0}/out/{1}.ivf"
                  , "retrieve:"
                  , ("OK:RETRIEVE(", "run:")
                  , ("OK:RETVAL(0)", "seti:nonblock:1")
                  , ("OK:SETI", "upload:")
                  , ("OK:UPLOADING", None)
                  , ("OK:UPLOAD(", "quit:")
                  ]

    def __init__(self, prevState, aNum):
        super(VPXEncStateMachine, self).__init__(prevState, aNum)
        aNum = self.actorNum + ServerInfo.num_offset
        vName = ServerInfo.video_name
        self.commands = [ s.format(vName, "%08d" % aNum) if s is not None else None for s in self.commands ]

def run():
    server.server_main_loop([], VPXEncStateMachine, ServerInfo)

def main():
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode": 1
            , "port": ServerInfo.port_number
            , "addr": None  # server_launch will fill this in for us
            , "nonblock": 0
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
