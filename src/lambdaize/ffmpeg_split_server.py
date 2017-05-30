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
#    -> FfmpegVideoSplitterQuitState
#    -> FinalState
###

import os
import re
import math

import signurl
from libmu import server, TerminalState, CommandListState, ForLoopState
from extract_metadata import MetadataExtraction

class ServerInfo(object):
    port_number = 13579

    video_name       = "video"
    num_frames       = 1
    num_offset       = 0
    num_parts        = 1
    lambda_function  = "ffmpeg"
    regions          = ["us-east-1"]
    bucket           = "excamera-us-east-1"
    video_mp4_name   = "input.mp4"
    in_format        = "mp4"
    out_file         = None
    profiling        = None
    s3_url_formatter = "http://s3-%s.amazonaws.com/%s/%s/%s"
    chunk_duration   = "%s:%s:%s"
    chunk_duration_s = 1

    cacert = None
    srvcrt = None
    srvkey = None

class FinalState(TerminalState):
    extra = "(finished)"

class FfmpegVideoSplitterQuitState(CommandListState):
    extra       = "(uploading)"
    nextState   = FinalState
    commandlist = [ (None, "quit:")
                  ]

class FfmpegVideoSplitterRetrieveAndRunState(CommandListState):
    extra       = "(retrieve video chunk, split into images and upload)"
    commandlist = [ (None, "set:fromfile:##TMPDIR##/{2}.png")
                  , "set:cmdoutfile:##TMPDIR##/{2}.png"
		  , "set:targfile:##TMPDIR##/{2}.png"
                  , "set:outkey:{1}/{2}.png"
		  , "run:./ffmpeg -version"
                  , "run:./ffmpeg -i '{8}' -ss {6} -t {7} -vframes 24 -f image2 -c:v png ##TMPDIR##/%08d.png"
		  , "run:ls ##TMPDIR##"
                  , ("OK:RETVAL(0)", "upload:")
                  , None
                  ]

    def get_video_metadata(self, bucket, key, number, actorNum):
      metadata = MetadataExtraction(bucket, key)
      metadata.invoke_metadata_extraction()
      return metadata

    def get_chunk_point_in_duration(self, metadata, number, actorNum):
      re_exp_for_duration = "(\d{2}):(\d{2}):(\d{2})\.\d+"
      re_length           = re.compile(re_exp_for_duration)
      video_duration      = metadata.get_duration()
      matches             = re_length.search(video_duration)
      if matches:
        video_length      = int(matches.group(1)) * 3600 + \
                            int(matches.group(2)) * 60 + \
                            int(matches.group(3))
	split_count       = int(math.ceil(video_length/float(ServerInfo.chunk_duration_s)))
	split_start       = ServerInfo.chunk_duration_s * actorNum
        return str(split_start)
      else:
	return str(ServerInfo.chunk_duration_s)

    def sign(self, bucket, key):
      signed_url = signurl.invoke_sign(bucket, key)
      return signed_url.replace("https", "http")

    def __init__(self, prevState, aNum=0):
        super(FfmpegVideoSplitterRetrieveAndRunState, self).__init__(prevState, aNum)
        inName         = "%s-%s" % (ServerInfo.video_name, ServerInfo.in_format)
        outName        = "%s-%s" % (inName, "png-split")
	number         = 1 + self.actorNum * 24
        #number         = 1 + ServerInfo.num_frames * (self.actorNum + ServerInfo.num_offset) + self.info['retrieve_iter']
        video_mp4_name = ServerInfo.video_mp4_name
        video_url      = ServerInfo.s3_url_formatter % (ServerInfo.regions[0], ServerInfo.bucket, inName, ServerInfo.video_mp4_name)
        metadata       = self.get_video_metadata(ServerInfo.bucket, ServerInfo.video_mp4_name, number, self.actorNum)
        chunk_point    = self.get_chunk_point_in_duration(metadata, number, self.actorNum)
        chunk_size     = ServerInfo.chunk_duration_s
	signed_url     = self.sign(ServerInfo.bucket, ServerInfo.video_mp4_name)
        self.commands  = [ s.format(inName, outName, "%08d" % number, video_mp4_name, video_url, metadata, chunk_point, chunk_size, signed_url) if s is not None else None for s in self.commands ]
   
class FfmpegVideoSplitterRetrieveLoopState(ForLoopState):
    extra     = "(retrieve loop)"
    loopState = FfmpegVideoSplitterRetrieveAndRunState
    exitState = FfmpegVideoSplitterQuitState
    iterKey   = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(FfmpegVideoSplitterRetrieveLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
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
