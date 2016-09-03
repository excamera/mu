#!/usr/bin/python

import os

from libmu import server, TerminalState, CommandListState, ForLoopState

class ServerInfo(object):
    video_name = "sintel-1k"
    num_frames = 6
    num_offset = 0
    num_parts = 1
    lambda_function = "png2y4m"
    regions = ["us-east-1"]
    bucket = "excamera-us-east-1"
    in_format = "png16"
    out_file = None
    profiling = None

    cacertfile = None
    srvcrtfile = None
    srvkeyfile = None

    cacert = None
    srvcrt = None
    srvkey = None

class FinalState(TerminalState):
    extra = "(finished)"

class PNG2Y4MUploadState(CommandListState):
    extra = "(uploading)"
    nextState = FinalState
    commandlist = [ (None, "upload:")
                  , "quit:"
                  ]

class PNG2Y4MRetrieveAndRunState(CommandListState):
    extra = "(retrieving PNG and appending to Y4M)"
    commandlist = [ (None, "set:inkey:{0}/{1}.png")
                  , "set:targfile:##TMPDIR##/{1}.png"
                  , "retrieve:"
                  , "run:"
                  , ("OK:RETVAL(0)", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(PNG2Y4MRetrieveAndRunState, self).__init__(prevState, aNum)
        # choose which key to retrieve next
        inName = "%s-%s" % (ServerInfo.video_name, ServerInfo.in_format)
        inNumber = 1 + ServerInfo.num_frames * (self.actorNum + ServerInfo.num_offset) + self.info['retrieve_iter']
        self.commands = [ s.format(inName, "%08d" % inNumber) if s is not None else None for s in self.commands ]

class PNG2Y4MRetrieveLoopState(ForLoopState):
    extra = "(retrieve loop)"
    loopState = PNG2Y4MRetrieveAndRunState
    exitState = PNG2Y4MUploadState
    iterKey = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(PNG2Y4MRetrieveLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        self.iterFin = ServerInfo.num_frames

# need to set this here to avoid use-before-def
PNG2Y4MRetrieveAndRunState.nextState = PNG2Y4MRetrieveLoopState

class PNG2Y4MConfigState(CommandListState):
    extra = "(configuring lambda worker)"
    nextState = PNG2Y4MRetrieveLoopState
    commandlist = [ ("OK:HELLO", "set:cmdinfile:##TMPDIR##/%08d.png")
                  , "set:cmdoutfile:##TMPDIR##/{1}.y4m"
                  , "set:fromfile:##TMPDIR##/{1}.y4m"
                  , "set:outkey:{0}/{1}.y4m"
                  , "seti:nonblock:0"
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, actorNum):
        super(PNG2Y4MConfigState, self).__init__(prevState, actorNum)
        outName = "%s-y4m_%02d" % (ServerInfo.video_name, ServerInfo.num_frames)
        outNumber = self.actorNum + ServerInfo.num_offset
        self.commands = [ s.format(outName, "%08d" % outNumber) if s is not None else None for s in self.commands ]

def run(chainfile=None, keyfile=None):
    server.server_main_loop([], PNG2Y4MConfigState, ServerInfo, chainfile, keyfile)

def main():
    server.options(ServerInfo)

    # launch the lambdas
    event = { "mode": 1
            , "port": 13579
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
