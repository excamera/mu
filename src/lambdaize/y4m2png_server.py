#!/usr/bin/python

###
# This server implements the state machine for converting a y4m 
# image to a png image.
#
# State Machine Description :
#  Co-ordinating y4m2png
#   -> Configure the lambda with instance specific-settings
#   -> Retrieve each input y4m from S3
#   -> Run the commands in the command-list on the retrieved files
#   -> Upload the resulting png
#
# State Machine Transitions :
#  Y4M2PNGConfigState
#    -> Y4M2PNGRetrieveLoopState
#    -> Y4M2PNGRetrieveAndRunState
#    -> Y4M2PNGUploadState
#    -> FinalState
###

import os

from libmu import server, TerminalState, CommandListState, ForLoopState

class ServerInfo(object):
    port_number = 13579

    video_name      = "sintel-1k"
    num_frames      = 6
    num_offset      = 0
    num_parts       = 1
    lambda_function = "y4m2png"
    regions         = ["us-east-1"]
    bucket          = "excamera-us-east-1"
    in_format       = "y4m_06"
    out_file        = None
    profiling       = None

    cacert = None
    srvcrt = None
    srvkey = None

class FinalState(TerminalState):
    extra = "(finished)"

class Y4M2PNGUploadState(CommandListState):
    extra       = "(uploading)"
    nextState   = FinalState
    commandlist = [ (None, "upload:")
                  , "quit:"
                  ]

class Y4M2PNGRetrieveAndRunState(CommandListState):
    extra       = "(retrieving Y4M and appending to PNG)"
    commandlist = [ (None, "set:inkey:{0}/{1}.y4m")
                  , "set:targfile:##TMPDIR##/{1}.y4m"
                  , "retrieve:"
                  , "run:./y4m2png -o ##OUTFILE## ##TMPDIR##/{1}.y4m"
                  , ("OK:RETVAL(0)", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(Y4M2PNGRetrieveAndRunState, self).__init__(prevState, aNum)
        # choose which key to retrieve next
        inName        = "%s-%s" % (ServerInfo.video_name, ServerInfo.in_format)
        inNumber      = 1 + ServerInfo.num_frames * (self.actorNum + ServerInfo.num_offset) + self.info['retrieve_iter']
        self.commands = [ s.format(inName, "%08d" % inNumber) if s is not None else None for s in self.commands ]

class Y4M2PNGRetrieveLoopState(ForLoopState):
    extra     = "(retrieve loop)"
    loopState = Y4M2PNGRetrieveAndRunState
    exitState = Y4M2PNGUploadState
    iterKey   = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(Y4M2PNGRetrieveLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        self.iterFin = ServerInfo.num_frames

# need to set this here to avoid use-before-def
Y4M2PNGRetrieveAndRunState.nextState = Y4M2PNGRetrieveLoopState

class Y4M2PNGConfigState(CommandListState):
    extra       = "(configuring lambda worker)"
    nextState   = Y4M2PNGRetrieveLoopState
    commandlist = [ ("OK:HELLO", "set:cmdinfile:##TMPDIR##/%08d.y4m")
                  , "set:cmdoutfile:##TMPDIR##/{1}.png"
                  , "set:fromfile:##TMPDIR##/{1}.png"
                  , "set:outkey:{0}/{1}.png"
                  , "seti:nonblock:0"
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, actorNum):
        super(Y4M2PNGConfigState, self).__init__(prevState, actorNum)
        outName       = "%s-png16_%02d" % (ServerInfo.video_name, ServerInfo.num_frames)
        outNumber     = self.actorNum + ServerInfo.num_offset
        self.commands = [ s.format(outName, "%08d" % outNumber) if s is not None else None for s in self.commands ]

def run():
    # start from Y4M2PNGConfigState - configures lambda worker
    server.server_main_loop([], Y4M2PNGConfigState, ServerInfo)

def main():
    # set the server info
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
