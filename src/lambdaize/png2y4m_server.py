#!/usr/bin/python

import sys

import pylaunch
from libmu import server, TerminalState, CommandListState, ForLoopState

class ServerInfo(object):
    video_name = "sintel-1k"        # basename of video to encode
    num_frames = 6                  # number of frames per worker
    num_offset = 0                  # number of chunks (of num_frames each) to skip
    num_parts = 1                   # number of lambdas to be run

class FinalState(TerminalState):
    extra = "(finished)"

class PNG2Y4MConvertAndUploadState(CommandListState):
    extra = "(converting and uploading)"
    nextState = FinalState
    commandlist = [ (None, "run:")
                  , ("OK:RETVAL(0)", "upload:")
                  , "quit:"
                  ]

class PNG2Y4MRetrieveState(CommandListState):
    extra = "(retrieving PNG from S3)"
    commandlist = [ (None, "set:inkey:{0}/{1}.png")
                  , "set:targfile:##TMPDIR##/{1}.png"
                  , "retrieve:"
                  , None
                  ]

    def __init__(self, prevState, aNum=0):
        super(PNG2Y4MRetrieveState, self).__init__(prevState, aNum)
        # choose which key to retrieve next
        inName = "%s-png16" % ServerInfo.video_name
        inNumber = 1 + ServerInfo.num_frames * (self.actorNum + ServerInfo.num_offset) + self.info['retrieve_iter']
        self.commands = [ s.format(inName, "%08d" % inNumber) if s is not None else None for s in self.commands ]

class PNG2Y4MRetrieveLoopState(ForLoopState):
    extra = "(retrieve loop)"
    loopState = PNG2Y4MRetrieveState
    exitState = PNG2Y4MConvertAndUploadState
    iterKey = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(PNG2Y4MRetrieveLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        self.iterFin = ServerInfo.num_frames

# need to set this here to avoid use-before-def
PNG2Y4MRetrieveState.nextState = PNG2Y4MRetrieveLoopState

class PNG2Y4MConfigState(CommandListState):
    extra = "(configuring lambda worker)"
    nextState = PNG2Y4MRetrieveLoopState
    commandlist = [ ("OK:HELLO", "set:cmdinfile:##TMPDIR##/%08d.png")
                  , "set:cmdoutfile:##TMPDIR##/{1}.y4m"
                  , "set:fromfile:##TMPDIR##/{1}.y4m"
                  , "set:outkey:{0}/{1}.y4m"
                  , "seti:nonblock:0"
                  , None
                  ]

    def __init__(self, prevState, actorNum):
        super(PNG2Y4MConfigState, self).__init__(prevState, actorNum)
        outName = "%s-y4m_%02d" % (ServerInfo.video_name, ServerInfo.num_frames)
        outNumber = self.actorNum + ServerInfo.num_offset
        self.commands = [ s.format(outName, "%08d" % outNumber) if s is not None else None for s in self.commands ]

def run(chainfile=None, keyfile=None):
    server.server_main_loop([], PNG2Y4MConfigState, ServerInfo.num_parts, chainfile, keyfile)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ServerInfo.num_parts = int(sys.argv[1])

    if len(sys.argv) > 2:
        ServerInfo.video_name = sys.argv[2]

    if len(sys.argv) > 3:
        ServerInfo.num_frames = int(sys.argv[3])

    if len(sys.argv) > 4:
        ServerInfo.num_offset = int(sys.argv[4])

    run()
