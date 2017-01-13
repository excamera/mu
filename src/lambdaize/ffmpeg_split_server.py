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

from libmu import server, TerminalState, CommandListState, ForLoopState

class ServerInfo(object):
    port_number = 13579

    video_name       = "sintel-1k"
    num_frames       = 6
    num_offset       = 0
    num_parts        = 1
    lambda_function  = "ffmpeg"
    regions          = ["us-east-1"]
    bucket           = "excamera-us-east-1"
    video_mp4_name   = "input.mp4"
    in_format        = "png16"
    out_file         = None
    profiling        = None
    s3_url_formatter = "http://s3-%s.amazonaws.com/%s/%s/%s"

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
    commandlist = [ (None, "set:inkey:{3}")
                  , "set:targfile:##TMPDIR##/{2}.png"
                  , "set:cmdoutfile:##TMPDIR##/{2}.png"
                  , "set:outkey:{1}/{2}.png"
                  , "retrieve:"
                  , "run:./ffmpeg -i {4} -ss {6} -vframes {7} -f image2 image%03d.png"
                  , ("OK:RETVAL(0)", "upload:")
                  , None
                  ]

    def __init__(self, prevState, aNum=0):
        super(FfmpegVideoSplitterRetrieveAndRunState, self).__init__(prevState, aNum)
        inName         = "%s-%s" % (ServerInfo.video_name, ServerInfo.in_format)
        outName        = "%s-%s" % (ServerInfo.video_mp4_name, "png-split")
        number         = 1 + ServerInfo.num_frames * (self.actorNum + ServerInfo.num_offset) + self.info['retrieve_iter']
        video_mp4_name = ServerInfo.video_mp4_name
        video_url      = ServerInfo.s3_url_formatter % (ServerInfo.regions[0], ServerInfo.bucket, ServerInfo.inName, ServerInfo.video_mp4_name)
        metadata       = get_video_metadata(ServerInfo.bucket, ServerInfo.video_mp4_name)
        chunk_point    = json_metadata.get_chunk_point_in_duration(json_metadata, number, self.actorNum)
        frames         = ServerInfo.num_frames
        self.commands  = [ s.format(inName, outName, "%08d" % number) if s is not None else None for s in self.commands ]
   
    def get_video_metadata(self, bucket, key, number, actorNum):
      metadata = MetadataExtraction()
      metadata.invoke_metadata_extraction(bucket, key)
      return metadata

    def get_chunk_point_in_duration(self, json_metadata, number, actorNum):
      video_duration = json_metadata.get_duration()
      return video_duration

class FfmpegVideoSplitterRetrieveLoopState(ForLoopState):
    extra     = "(retrieve loop)"
    loopState = FfmpegVideoSplitterRetrieveAndRunState
    exitState = FfmpegVideoSplitterQuitState
    iterKey   = "retrieve_iter"

    def __init__(self, prevState, aNum=0):
        super(FfmpegVideoSplitterRetrieveLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        self.iterFin = ServerInfo.num_frames

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
