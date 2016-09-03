#!/usr/bin/python

import cProfile
import getopt
import os
import sys

from libmu import server, util, Defs, TerminalState, CommandListState, ForLoopState

class ServerInfo(object):
    video_name = "sintel-1k"        # basename of video to encode
    num_frames = 6                  # number of frames per worker
    num_offset = 0                  # number of chunks (of num_frames each) to skip
    num_parts = 1                   # number of lambdas to be run
    lambda_function = "png2y4m"     # lambda function name
    regions = ["us-east-1"]         # regions in which to launch
    bucket = "excamera-us-east-1"
    in_format = "png16"
    out_file = None
    profiling = None
defaults = ServerInfo()

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
    server.server_main_loop([], PNG2Y4MConfigState, ServerInfo.num_parts, chainfile, keyfile)

def usage():
    oFileStr = "'%s'" % defaults.out_file if defaults.out_file is not None else "None"
    pFileStr = "'%s'" % defaults.profiling if defaults.profiling is not None else "None"
    print "Usage: %s [-h] [-D] [-O oFile] [-P pFile]" % sys.argv[0]
    print "       [-n nParts] [-f nFrames] [-o nOffset]"
    print "       [-v vidName] [-b bucket] [-i inFormat]"
    print "       [-l fnName] [-r region1,region2,...]"
    print "       [-c caCert] [-s srvCert] [-k srvKey]"
    print
    print "You must also set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY envvars."
    print
    print "  switch         description                                     default"
    print "  --             --                                              --"
    print "  -h:            show this message"
    print "  -D:            enable debug                                    (disabled)"
    print "  -O oFile:      state machine times output file                 (%s)" % oFileStr
    print "  -P pFile:      profiling data output file                      (%s)" % pFileStr
    print
    print "  -n nParts:     launch nParts lambdas                           (%d)" % defaults.num_parts
    print "  -f nFrames:    number of frames to process in each chunk       (%d)" % defaults.num_frames
    print "  -o nOffset:    skip this many input chunks when processing     (%d)" % defaults.num_offset
    print
    print "  -v vidName:    video name                                      ('%s')" % defaults.video_name
    print "  -b bucket:     S3 bucket in which videos are stored            ('%s')" % defaults.bucket
    print "  -i inFormat:   PNG format ('png' or 'png16', probably)         ('%s')" % defaults.in_format
    print "Input files are 's3://<bucket>/<vidname>-<in_format>/%08d.png'"
    print
    print "  -l fnName:     lambda function name                            ('%s')" % defaults.lambda_function
    print "  -r r1,r2,...:  comma-separated list of regions                 ('%s')" % ','.join(defaults.regions)
    print
    print "  -c caCert:     CA certificate file                             (None)"
    print "  -s srvCert:    server certificate file                         (None)"
    print "  -k srvKey:     server key file                                 (None)"
    print "(hint: you can generate new keys with <mu>/bin/genkeys.sh)"

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "o:f:n:v:l:r:Dc:s:k:i:b:hO:P:")
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(1)

    if len(args) > 0:
        print "ERROR: Extraneous arguments '%s'" % ' '.join(args)
        print
        usage()
        sys.exit(1)

    sslfiles = [os.environ.get('CA_CERT'), os.environ.get('SRV_CERT'), os.environ.get('SRV_KEY')]
    for (opt, arg) in opts:
        if opt == "-o":
            ServerInfo.num_offset = int(arg)
        elif opt == "-f":
            ServerInfo.num_frames = int(arg)
        elif opt == "-n":
            ServerInfo.num_parts = int(arg)
        elif opt == "-v":
            ServerInfo.video_name = arg
        elif opt == "-l":
            ServerInfo.lambda_function = arg
        elif opt == "-r":
            ServerInfo.regions = arg.split(',')
        elif opt == "-D":
            Defs.debug = True
        elif opt == "-c":
            sslfiles[0] = arg
        elif opt == "-s":
            sslfiles[1] = arg
        elif opt == "-k":
            sslfiles[2] = arg
        elif opt == "-b":
            ServerInfo.bucket = arg
        elif opt == "-i":
            ServerInfo.in_format = arg
        elif opt == "-h":
            usage()
            sys.exit(1)
        elif opt == "-O":
            ServerInfo.out_file = arg
        elif opt == "-P":
            ServerInfo.profiling = arg
        else:
            assert False, "logic error: got unexpected option %s from getopt" % opt

    if len(ServerInfo.regions) == 0:
        print "ERROR: region list cannot be empty"
        print
        usage()
        sys.exit(1)

    if os.environ.get("AWS_ACCESS_KEY_ID") is None or os.environ.get("AWS_SECRET_ACCESS_KEY") is None:
        print "ERROR: You must set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY envvars"
        print
        usage()
        sys.exit(1)

    for f in sslfiles:
        try:
            os.stat(str(f))
        except:
            print "ERROR: Cannot open SSL cert or key file '%s'" % str(f)
            print
            usage()
            sys.exit(1)

    # launch the lambdas
    event = { "mode": 1
            , "port": 13579
            , "addr": None  # server_launch will fill this in for us
            , "nonblock": 0
            , "cacert": util.read_pem(sslfiles[0])
            , "srvcrt": util.read_pem(sslfiles[1])
            , "srvkey": util.read_pem(sslfiles[2])
            }
    server.server_launch(ServerInfo, event, os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])

    # run the server
    if ServerInfo.profiling:
        pr = cProfile.Profile()
        pr.enable()

    run(sslfiles[1], sslfiles[2])

    if ServerInfo.profiling:
        pr.disable()
        pr.dump_stats(ServerInfo.profiling)

if __name__ == "__main__":
    main()
