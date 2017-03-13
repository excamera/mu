#!/usr/bin/python

###
# This server implements the state machine for grayscaling
# an image
#
# State Machine Description :
#  Co-ordinating FfmpegVideoSplitter
#   -> Configure the lambda with instance specific-settings
#   -> Retrieve each input.png from S3
#   -> Run the commands in the command-list on the retrieved files
#   -> Upload the resulting FfmpegVideoSplitterd png
#
# State Machine Transitions :
#  FfmpegVideoSplitterConfigState
#    -> FfmpegVideoSplitterRetrieveLoopState
#    -> FfmpegVideoSplitterRetrieveAndRunState
#    -> FfmpegVideoSplitterUploadLoopState
#    -> FfmpegVideoSplitterUploadLoopState
#    -> FfmpegVideoSplitterQuitState
#    -> FinalState
###

import os
import re
import math

import signurl
from libmu import server, TerminalState, CommandListState, ForLoopState
import extract_metadata

class ServerInfo(object):
    port_number = 13579

    video_name       = "video"
    num_frames       = 1
    num_offset       = 0
    num_parts        = 1
    lambda_function  = ""
    regions          = ["us-east-1"]
    bucket           = "excamera-ffmpeg-input"
    video_mp4_name   = "input.mp4"
    in_format        = "mp4"
    out_file         = None
    profiling        = None
    s3_url_formatter = "http://s3-%s.amazonaws.com/%s/%s/%s"
    chunk_duration   = "%s:%s:%s"
    chunk_duration_s = 1
    frame_rate       = num_frames
    lambda_count     = num_parts

    cacert = None
    srvcrt = None
    srvkey = None

class FinalState(TerminalState):
    extra = "(finished)"
    commandlist = [ (None, "quit:")
		  , None
		  ]

class FfmpegVideoSplitterUploadState(CommandListState):
    extra       = "(uploading)"
    commandlist = [ (None, "set:fromfile:##TMPDIR##/{2}.png")
		  , ("OK:SET", "set:cmdoutfile:##TMPDIR##/{2}.png")
		  , ("OK:SET", "set:outkey:{1}/{3}.png")
                  , ("OK:SET", "upload:")
		  , None
                  ]

    def __init__(self, prevState, aNum=0):
      super(FfmpegVideoSplitterUploadState, self).__init__(prevState, aNum)
      inName  = "%s-%s" % (ServerInfo.video_name, ServerInfo.in_format)
      outName = "%s-%s" % (inName, "png-split")
      num     = 1 + prevState.info['retrieve_iter']
      number  = 1 + ServerInfo.frame_rate * ServerInfo.chunk_duration_s * (self.actorNum + ServerInfo.num_offset) + self.info['retrieve_iter']
      self.commands  = [ s.format(inName, outName, num, "%08d" % number) if s is not None else None for s in self.commands ]

class FfmpegVideoSplitterUploadLoopState(ForLoopState):
    extra     = "(upload loop)"
    loopState = FfmpegVideoSplitterUploadState
    exitState = FinalState
    iterKey   = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(FfmpegVideoSplitterUploadLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        self.iterFin  = ServerInfo.frame_rate * ServerInfo.chunk_duration_s
	self.iterInit = 1

FfmpegVideoSplitterUploadState.nextState = FfmpegVideoSplitterUploadLoopState

class FfmpegVideoSplitterRetrieveAndRunState(CommandListState):
    extra       = "(retrieve video chunk, split into images and upload)"
    commandlist = [ (None, "run:./ffmpeg -ss {1} -t {2} -r {4} -i '{3}' -f image2 -c:v png ##TMPDIR##/%d.png")
		  , ("OK:RETVAL(0)", "run:ls ##TMPDIR## | wc -l")
                  , None
                  ]

    def sign(self, bucket, key):
      return signurl.invoke_sign(bucket, key)

    def __init__(self, prevState, aNum=0):
        super(FfmpegVideoSplitterRetrieveAndRunState, self).__init__(prevState, aNum)
        inName         = "%s-%s" % (ServerInfo.video_name, ServerInfo.in_format)
        chunk_point    = ServerInfo.chunk_duration_s * self.actorNum
        chunk_size     = ServerInfo.chunk_duration_s
	signed_url     = self.sign(ServerInfo.bucket, ServerInfo.video_mp4_name)
	frame_rate     = ServerInfo.frame_rate
        self.commands  = [ s.format(inName, chunk_point, chunk_size, signed_url, frame_rate) if s is not None else None for s in self.commands ]
   
class FfmpegVideoSplitterRetrieveLoopState(ForLoopState):
    extra     = "(retrieve loop)"
    loopState = FfmpegVideoSplitterRetrieveAndRunState
    exitState = FfmpegVideoSplitterUploadLoopState
    iterKey   = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(FfmpegVideoSplitterRetrieveLoopState, self).__init__(prevState, aNum)
	# hard-coded to 1 because our input is just 1
        self.iterFin = 1

# need to set this here to avoid use-before-def
FfmpegVideoSplitterRetrieveAndRunState.nextState = FfmpegVideoSplitterRetrieveLoopState

class FfmpegVideoSplitterConfigState(CommandListState):
    extra       = "(configuring lambda worker)"
    nextState   = FfmpegVideoSplitterRetrieveLoopState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, actorNum):
        super(FfmpegVideoSplitterConfigState, self).__init__(prevState, actorNum)

def run():
    # start from FfmpegVideoSplitterConfigState - configures lambda worker
    server.server_main_loop([], FfmpegVideoSplitterConfigState, ServerInfo)

def main():
    # set the server info
    server.options(ServerInfo)

    # extract metadata
    video_length = extract_metadata.set_chunk_point_in_duration(
					ServerInfo.bucket,
					ServerInfo.video_mp4_name,
					ServerInfo.num_parts)
    if ServerInfo.num_parts > video_length:
      ServerInfo.lambda_count = video_length
      ServerInfo.chunk_duration_s = 1
      print ("Requested : %s lambda, Needed : %d lambdas only" % (ServerInfo.num_parts, ServerInfo.lambda_count))
      ServerInfo.num_parts = ServerInfo.lambda_count
    else:
      ServerInfo.chunk_duration_s = int(math.ceil(video_length/float(ServerInfo.num_parts)))
      print ("Forking %d lambdas with %d secs of video/lambda" %(ServerInfo.num_parts, ServerInfo.chunk_duration_s))

    ServerInfo.frame_rate = ServerInfo.num_frames

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
