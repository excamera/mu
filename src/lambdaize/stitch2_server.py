#!/usr/bin/python

import os

from libmu import server, TerminalState, CommandListState

class ServerInfo(object):
    host_addr = None
    port_number = 13579

    video_name = "sintel-1k"
    num_offset = 0
    num_parts = 1
    num_frames = 6
    overprovision = 25

    lambda_function = "mk7frames"
    regions = ["us-east-1"]
    bucket = "excamera-us-east-1"
    out_file = None
    profiling = None

    cacert = None
    srvcrt = None
    srvkey = None

class FinalState(TerminalState):
    extra = "(finished)"

class Stitch2State(CommandListState):
    extra = "(configuring lambda worker)"
    nextState = FinalState
    pipelined = False
    commandlist = [ ("OK:HELLO", "retrieve:{0}/{1}.y4m\0##TMPDIR##/first.y4m")
                  , (None, "retrieve:{0}/{2}.y4m\0##TMPDIR##/second.y4m")
                  , ("OK:RETRIEV", None)
                  , ("OK:RETRIEV", """run:/usr/bin/perl -e 'use strict; my $fd; open($fd, "<", "##TMPDIR##/second.y4m"); my @lines = <$fd>; close($fd); unlink("##TMPDIR##/second.y4m"); open($fd, ">>", "##TMPDIR##/first.y4m"); shift @lines; print $fd @lines; close($fd);'""")
                  , ("OK:RETVAL(0)", "upload:{3}/{4}.y4m\0##TMPDIR##/first.y4m")
                  , "quit:"
                  ]

    def __init__(self, prevState, actorNum):
        super(Stitch2State, self).__init__(prevState, actorNum)
        inName = "%s-y4m_%02d" % (ServerInfo.video_name, ServerInfo.num_frames)
        thisNum = self.actorNum + ServerInfo.num_offset
        inNum1 = "%08d" % (2 * thisNum)
        inNum2 = "%08d" % (2 * thisNum + 1)

        outName = "%s-y4m_%02d" % (ServerInfo.video_name, 2 * ServerInfo.num_frames)
        outNum = "%08d" % (thisNum)

        self.commands = [ s.format(inName, inNum1, inNum2, outName, outNum) if s is not None else None for s in self.commands ]

def run():
    server.server_main_loop([], Stitch2State, ServerInfo)

def main():
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode": 1
            , "port": ServerInfo.port_number
            , "addr": ServerInfo.host_addr
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
