#!/usr/bin/python

import os
import pprint

from libmu import server, TerminalState, CommandListState

pp = pprint.PrettyPrinter(indent=2)

class ServerInfo(object):
    port_number = 13579

    regions         = ["us-east-1"]

    num_parts = 16
    lambda_function = "linpack_bench_d"

    cacert = None
    srvcrt = None
    srvkey = None

    profiling = None
    out_file = None

class FinalState(TerminalState):
    extra = "(finished)"

    def __init__(self, prevState, actorNum=0):
        super(FinalState, self).__init__(prevState, actorNum)

        aNum = self.actorNum
        if ServerInfo.out_file is not None:
            with open("%s.%d.msgs" % (ServerInfo.out_file, aNum), 'w') as mout:
                mout.write(pp.pformat(self.prevState.messages))

class LinpackRunState(CommandListState):
    extra       = "(running linpack x2)"
    nextState   = FinalState
    commandlist = [ ("OK:HELLO", "run:time ./linpack_bench_d")
                  , ("OK", "run:time ./linpack_bench_d")
                  , ("OK", None)
                  , ("OK", None)
                  , ("OK", None)
                  ]

def run():
    server.server_main_loop([], LinpackRunState, ServerInfo)

def main():
    # set the server info
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode": 1
            , "port": ServerInfo.port_number
            , "addr": None  # server_launch will fill this in for us
            , "nonblock": 1
            , "cacert": ServerInfo.cacert
            , "srvcrt": ServerInfo.srvcrt
            , "srvkey": ServerInfo.srvkey
            }
    server.server_launch(ServerInfo, event, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

    # run the server
    run()

if __name__ == "__main__":
    main()
