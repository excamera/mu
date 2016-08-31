#!/usr/bin/python

import socket
import sys

import pylaunch
from libmu import server, TerminalState, CommandListState

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

    def __init__(self, prevState, aNum, vName):
        super(VPXEncStateMachine, self).__init__(prevState, aNum)
        self.commands = [ s.format(vName, "%06d" % aNum) if s is not None else None for s in self.commands ]

def handle_server_sock(ls, states, num_parts, basename):
    (ns, _) = ls.accept()
    ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ns.setblocking(False)

    nstate = VPXEncStateMachine(ns, len(states), basename)
    nstate.do_handshake()

    states.append(nstate)

    if len(states) == num_parts:
        # no need to listen any longer, we have all our connections
        try:
            ls.shutdown()
            ls.close()
        except:
            pass

        ls = None

    return ls

def run(num_parts, basename, chainfile=None, keyfile=None):
    server.server_main_loop([], handle_server_sock, num_parts, basename, chainfile, keyfile)

if __name__ == "__main__":
    nparts = 1
    if len(sys.argv) > 1:
        nparts = int(sys.argv[1])

    bname = "6bbb"
    if len(sys.argv) > 2:
        bname = sys.argv[2]

    run(nparts, bname)
