#!/usr/bin/python
###
# This server implements the state machine for grayscaling
# an image
#
# State Machine Description :
#  Co-ordinating MergeMp4
#   -> Configure the lambda with instance specific-settings
#   -> Retrieve each input.png from S3
#   -> Run the commands in the command-list on the retrieved files
#   -> Upload the resulting MergeMp4d png
#
# State Machine Transitions :
#  MergeMp4ConfigState
#    -> MergeMp4RetrieveLoopState
#    -> MergeMp4RetrieveRunAndUploadState
#    -> MergeMp4QuitState
#    -> FinalState
###

import os

from libmu import server, TerminalState, CommandListState, ForLoopState

class ServerInfo(object):
    port_number = 14579

    video_name      	= "output"
    num_frames      	= 1
    num_offset      	= 0
    num_parts       	= 1
    lambda_function 	= "ffmpeg"
    regions         	= ["us-east-1"]
    bucket          	= "lixiang-lambda-test"
    in_format       	= "output"
    out_file        	= None
    profiling       	= None

    cacert = None
    srvcrt = None
    srvkey = None

class FinalState(TerminalState):
    extra = "(finished)"

class MergeMp4QuitState(CommandListState):
    extra       = "(uploading)"
    nextState   = FinalState
    commandlist = [ (None, "quit:")
                  ]

class MergeMp4RetrieveRunAndUploadState(CommandListState):
    extra       = "(retrieving mp4, MP4Box and upload m4s)"
    # set the variables for downloading the first mp4
    commandlist = [ (None, "set:inkey:{0}/{2}.mp4")
                  , "set:targfile:##TMPDIR##/{2}.mp4"
                  , "set:cmdinfile:##TMPDIR##/{2}.mp4"
		  , "retrieve:"
    # set the variables for downloading the second mp4
		  , ("OK:RETVAL(0)", "set:inkey:{0}/{3}.mp4")
		  , "set:targfile:##TMPDIR##/{3}.mp4"
		  , "set:cmdinfile:##TMPDIR##/{3}.mp4"
		  , "retrieve:"
    # concatenate the two mp4 files
		  , ("OK:RETVAL(0)", "./ffmpeg -i 'concat:##TMPDIR##/{2}.mp4|##TMPDIR##/{3}.mp4' -codec copy ##TMPDIR##/{4}.mp4")
    # run MP4Box to create segments
		  , ("OK:RETVAL(0)", "./MP4Box -dash 1000 -rap -segment-name ##TMPDIR##/seg_{2}_{3}_ ##TMPDIR##/{4}.mp4")
    # upload the first segment
                  , "set:cmdoutfile:##TMPDIR##/seg_{2}_{3}_1.m4s"
                  , "set:fromfile:##TMPDIR##/seg_{2}_{3}_1.m4s"
                  , "set:outkey:{1}/seg_{5}_1.m4s"
                  , ("OK:RETVAL(0)", "upload:")
    # upload the second segment
		  , ("OK:UPLOAD(0)", "set:cmdoutfile:##TMPDIR##/seg_{2}_{3}_2.m4s")
                  , "set:fromfile:##TMPDIR##/seg_{2}_{3}_2.m4s"
                  , "set:outkey:{1}/seg_{5}_2.m4s"
                  , ("OK:RETVAL(0)", "upload:")
                  , ("OK:UPLOAD(0)", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(MergeMp4RetrieveAndRunState, self).__init__(prevState, aNum)
	###
	#  inName	: input folder name 
	#  outName	: output folder name
	#  first_mp4	: the first mp4 file
	#  second_mp4	: the second mp4 file
	#  merged_mp4	: the merged mp4 file
	#  output_m4s	: the prefix for the m4s segments
	###
        inName          = "%s" % (ServerInfo.in_format)
        outName         = "%s-%s" % (ServerInfo.in_format, "m4s")
        first_mp4       = "%08d" % (self.actorNum)
        second_mp4      = "%08d" % (self.actorNum + 1)
	merged_mp4	= "%s_%s" % (first_mp4, second_mp4)
	output_m4s	= "%08d_%s" % (self.actorNum, self.info['mp4_iter'])
        self.commands   = [ s.format(inName, first_mp4, second_mp4) if s is not None else None for s in self.commands ]

class MergeMp4RetrieveLoopState(ForLoopState):
    extra     = "(retrieve loop)"
    loopState = MergeMp4RetrieveRunAndUploadState
    exitState = MergeMp4QuitState
    iterKey   = "mp4_iter"

    def __init__(self, prevState, aNum=0):
        super(MergeMp4RetrieveLoopState, self).__init__(prevState, aNum)
        # number of frames to retrieve is stored in ServerInfo object
        self.iterFin = ServerInfo.num_frames

# need to set this here to avoid use-before-def
MergeMp4RetrieveRunAndUploadState.nextState = MergeMp4RetrieveLoopState

class MergeMp4ConfigState(CommandListState):
    extra       = "(configuring lambda worker)"
    nextState   = MergeMp4RetrieveLoopState
    commandlist = [ ("OK:HELLO", "seti:nonblock:0")
                  , "run:rm -rf /tmp/*"
                  , "run:mkdir -p ##TMPDIR##"
                  , None
                  ]

    def __init__(self, prevState, actorNum):
        super(MergeMp4ConfigState, self).__init__(prevState, actorNum)

def run():
    # start from MergeMp4ConfigState - configures lambda worker
    server.server_main_loop([], MergeMp4ConfigState, ServerInfo)

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
