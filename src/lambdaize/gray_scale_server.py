#!/usr/bin/python
###
# This server implements the state machine for grayscaling
# an image
#
# State Machine Description :
#  Co-ordinating grayscale
#   -> Configure the lambda with instance specific-settings
#   -> Retrieve each input.png from S3
#   -> Run the commands in the command-list on the retrieved files
#   -> Upload the resulting grayscaled png
#
# State Machine Transitions :
#  GrayScaleConfigState
#    -> GrayScaleRetrieveLoopState
#    -> GrayScaleRetrieveAndRunState
#    -> GrayScaleQuitState
#    -> FinalState
###

import os

from libmu import server, TerminalState, CommandListState, ForLoopState

class ServerInfo(object):
    port_number = 13579

    video_name      = "video"
    num_frames      = 6
    num_offset      = 0
    num_parts       = 1
    lambda_function = "ffmpeg"
    regions         = ["us-east-1"]
    bucket          = "excamera-us-east-1"
    in_format       = "mp4"
    out_file        = None
    profiling       = None

    cacert = None
    srvcrt = None
    srvkey = None

class FinalState(TerminalState):
    extra = "(finished)"

class GrayScaleQuitState(CommandListState):
    extra       = "(uploading)"
    nextState   = FinalState
    commandlist = [ (None, "quit:")
                  ]

class GrayScaleRetrieveAndRunState(CommandListState):
    extra       = "(retrieving png images, grayscale and upload)"
    commandlist = [ (None, "set:inkey:{0}/{2}.png")
                  , "set:targfile:##TMPDIR##/{2}.png"
                  , "set:cmdinfile:##TMPDIR##/{2}.png"
                  , "set:cmdoutfile:##TMPDIR##/{2}-gs.png"
                  , "set:fromfile:##TMPDIR##/{2}-gs.png"
                  , "set:outkey:{1}/{2}-gs.png"
                  , "retrieve:"
                  , "run:./ffmpeg -i ##TMPDIR##/{2}.png -f image2 -vf hue=s=0 -c:a copy -safe 0 ##TMPDIR##/{2}-gs.png"
                  , ("OK:RETVAL(0)", "upload:")
                  , None
                  ]

    def __init__(self, prevState, aNum=0):
        super(GrayScaleRetrieveAndRunState, self).__init__(prevState, aNum)
        inName        = "%s-%s-%s" % (ServerInfo.video_name, ServerInfo.in_format, "png-split")
        outName       = "%s-%s-%s" % (ServerInfo.video_name, ServerInfo.in_format, "png-split-gs")
        number        = 1 + ServerInfo.num_frames * (self.actorNum + ServerInfo.num_offset) + self.info['retrieve_iter']
        self.commands = [ s.format(inName, outName, "%08d" % number) if s is not None else None for s in self.commands ]

class GrayScaleRetrieveLoopState(ForLoopState):
    extra     = "(retrieve loop)"
    loopState = GrayScaleRetrieveAndRunState
    exitState = GrayScaleQuitState
    iterKey   = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(GrayScaleRetrieveLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        self.iterFin = ServerInfo.num_frames

# need to set this here to avoid use-before-def
GrayScaleRetrieveAndRunState.nextState = GrayScaleRetrieveLoopState

class GrayScaleConfigState(CommandListState):
    extra       = "(configuring lambda worker)"
    nextState   = GrayScaleRetrieveLoopState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, actorNum):
        super(GrayScaleConfigState, self).__init__(prevState, actorNum)

def run():
    # start from GrayScaleConfigState - configures lambda worker
    server.server_main_loop([], GrayScaleConfigState, ServerInfo)

def main():
    # set the server info
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode"    : 1
            , "port"    : ServerInfo.port_number
            , "addr"    : None  # server_launch will fill this in for us
            , "nonblock": 0
            , "cacert"  : ServerInfo.cacert
            , "srvcrt"  : ServerInfo.srvcrt
            , "srvkey"  : ServerInfo.srvkey
            , "bucket"  : ServerInfo.bucket
            }
    server.server_launch(ServerInfo, event, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

    # run the server
    run()

if __name__ == "__main__":
    main()
