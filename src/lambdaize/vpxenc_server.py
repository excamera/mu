#!/usr/bin/python

import sys

import pylaunch
from libmu import server, TerminalState, CommandListState

class ServerInfo(object):
    video_name = "6bbb"
    num_parts = 1

class FinalState(TerminalState):
    extra = "(finished)"

class VPXEncStateMachine(CommandListState):
    nextState = FinalState
    commandlist = [ ("OK:HELLO", "set:inkey:{0}/{0}{1}.y4m")
                  , "set:targfile:##TMPDIR##/{0}{1}.y4m"
                  , "set:cmdinfile:##TMPDIR##/{0}{1}.y4m"
                  , "set:cmdoutfile:##TMPDIR##/{0}{1}.ivf"
                  , "set:fromfile:##TMPDIR##/{0}{1}.ivf"
                  , "set:outkey:{0}/out/{0}{1}.ivf"
                  , "seti:rm_tmpdir:1"
                  , "retrieve:"
                  , ("OK:RETRIEVE(", "run:")
                  , ("OK:RETVAL(0)", "seti:nonblock:1")
                  , ("OK:SETI", "upload:")
                  , ("OK:UPLOADING", None)
                  , ("OK:UPLOAD(", "quit:")
                  ]

    def __init__(self, prevState, aNum):
        super(VPXEncStateMachine, self).__init__(prevState, aNum)
        vName = ServerInfo.video_name
        self.commands = [ s.format(vName, "%06d" % aNum) if s is not None else None for s in self.commands ]

def run(chainfile=None, keyfile=None):
    server.server_main_loop([], VPXEncStateMachine, ServerInfo.num_parts, chainfile, keyfile)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ServerInfo.num_parts = int(sys.argv[1])

    if len(sys.argv) > 2:
        ServerInfo.video_name = sys.argv[2]

    run()
