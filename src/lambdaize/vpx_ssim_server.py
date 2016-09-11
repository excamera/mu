#!/usr/bin/python

import os

from libmu import server, TerminalState, CommandListState, ForLoopState

class ServerInfo(object):
    quality_values = [ 0, 8, 16 ]
    quality_valstring = "0_8_16"

    run_xcenc = False
    vpx_cmdstring = "./vpxenc -y --codec=vp8 --ivf --min-q=##QUALITY## --max-q=##QUALITY## -o ##OUTFILE##_##QUALITY## ##INFILE##"
    xc_cmdstring = "./xc-enc --y-ac-qi ##QUALITY## -o ##OUTFILE##_##QUALITY## -i y4m ##INFILE##"

    port_number = 13579

    video_name = "sintel-1k-y4m_06"
    num_parts = 1
    num_offset = 0
    lambda_function = "vpxenc"
    regions = ["us-east-1"]
    bucket = "excamera-us-east-1"
    out_file = None
    profiling = None

    cacert = None
    srvcrt = None
    srvkey = None

    num_list = None

class FinalState(TerminalState):
    extra = "(finished)"

class VPXSsimUpload(CommandListState):
    extra = "(uploading)"
    nextState = FinalState
    commandlist = [ (None, "upload:")
                  , ("OK:UPLOAD(", "quit:")
                  ]

class VPXSsimRun(CommandListState):
    extra = "(running encoder and analyzing output)"
    pipelined = True
    commandlist = [ (None, "set:cmdquality:{2}")
                  , ("OK:SET", "run:{4}")
                  , ("OK:RETVAL(0)", "run:echo QUALITY:##QUALITY## >> ##TMPDIR##/{1}.txt")
                  , ("OK:RETVAL(0)", "run:./xc-framesize ##OUTFILE##_##QUALITY## >> ##TMPDIR##/{1}.txt")
                  , ("OK:RETVAL(0)", "run:./vpxdec --codec=vp8 -o ##INFILE##_dec_##QUALITY## ##OUTFILE##_##QUALITY##")
                  , ("OK:RETVAL(0)", "run:./dump_ssim ##INFILE## ##INFILE##_dec_##QUALITY## >> ##TMPDIR##/{1}.txt")
                  , ("OK:RETVAL(0)", "run:rm ##INFILE##_dec_##QUALITY## ##OUTFILE##_##QUALITY##")
                  , ("OK:RETVAL(0)", None)
                  ]

    def __init__(self, prevState, aNum=0):
        super(VPXSsimRun, self).__init__(prevState, aNum)
        vName = ServerInfo.video_name
        qStr = ServerInfo.quality_valstring

        if ServerInfo.num_list is None:
            pNum = ServerInfo.num_offset + self.actorNum
        else:
            pNum = ServerInfo.num_list[self.actorNum] # pylint: disable=unsubscriptable-object

        if self.info.get('quality_iter') is not None:
            quality = ServerInfo.quality_values[self.info['quality_iter']]
        else:
            quality = 0

        if ServerInfo.run_xcenc:
            cmdStr = ServerInfo.xc_cmdstring
        else:
            cmdStr = ServerInfo.vpx_cmdstring

        self.commands = [ s.format(vName, "%08d" % pNum, quality, qStr, cmdStr) if s is not None else None for s in self.commands ]

class VPXSsimLoop(ForLoopState):
    extra = "(looping)"
    loopState = VPXSsimRun
    exitState = VPXSsimUpload
    iterKey = "quality_iter"

    def __init__(self, prevState, aNum=0):
        super(VPXSsimLoop, self).__init__(prevState, aNum)
        self.iterFin = len(ServerInfo.quality_values)

VPXSsimRun.nextState = VPXSsimLoop

class VPXSsimSettings(VPXSsimRun):
    extra = "(setting up worker and retrieving)"
    pipelined = True
    nextState = VPXSsimLoop
    commandlist = [ ("OK:HELLO", "set:inkey:{0}/{1}.y4m")
                  , "set:targfile:##TMPDIR##/{1}.y4m"
                  , "set:cmdinfile:##TMPDIR##/{1}.y4m"
                  , "set:cmdoutfile:##TMPDIR##/{1}.ivf"
                  , "set:fromfile:##TMPDIR##/{1}.txt"
                  , "set:outkey:{0}/ssim_txt_{3}/{1}.txt"
                  , "run:rm -rf /tmp/*"
                  , ("OK:RETVAL(0)", "run:mkdir -p ##TMPDIR##")
                  , ("OK:RETVAL(0)", "retrieve:")
                  , ("OK:RETRIEVE(", None)
                  ]

def run():
    server.server_main_loop([], VPXSsimSettings, ServerInfo)

def main():
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
