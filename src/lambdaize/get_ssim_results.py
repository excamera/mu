#!/usr/bin/python

import getopt
import md5
import multiprocessing as mp
import sys

import boto3

class ConfigInfo(object):
    video_name = "sintel"
    num_offset = 0
    num_parts = 1
    num_frames = 6

    keyframe_distance = 16

    quality_y = 30
    quality_str = None

    bucket = None
    region = "us-east-1"

    hashed_names = False

    download_threads = 8

def show_usage():
    print "Usage: get_ssim_results.py [options]"
    print
    print " -Y quality_y"
    print " -K keyframe_distance"
    print " -v video_name"
    print " -f frames_per_chunk"
    print " -n num_chunks"
    print " -o num_chunks_offset"
    print " -r region_name          (will autocompute bucketname if you leave out -b)"
    print " -b bucket_name"
    print " -N download_threads"
    print " -M                      (enable hashed names)"
    print

def get_options():
    oStr = "Y:K:v:f:o:n:b:r:N:M"

    try:
        (opts, args) = getopt.getopt(sys.argv[1:], oStr)
    except getopt.GetoptError as err:
        print str(err)
        print
        show_usage()
        sys.exit(1)

    if len(args) > 0:
        print "ERROR: Extraneous arguments '%s'" % ' '.join(args)
        print
        show_usage()
        sys.exit(1)

    for (opt, arg) in opts:
        if opt == "-o":
            ConfigInfo.num_offset = int(arg)
        elif opt == "-n":
            ConfigInfo.num_parts = int(arg)
        elif opt == "-b":
            ConfigInfo.bucket = arg
        elif opt == "-r":
            ConfigInfo.region = arg
        elif opt == "-f":
            ConfigInfo.num_frames = int(arg)
        elif opt == "-v":
            ConfigInfo.video_name = arg
        elif opt == "-K":
            ConfigInfo.keyframe_distance = int(arg)
        elif opt == "-Y":
            ConfigInfo.quality_y = int(arg)
        elif opt == "-N":
            ConfigInfo.download_threads = int(arg)
        elif opt == "-M":
            ConfigInfo.hashed_names = True
        else:
            assert False, "logic error: got unexpected option %s from getopt" % opt

    # construct quality_str
    ConfigInfo.quality_str = "%d_x_k%d" % (ConfigInfo.quality_y, ConfigInfo.keyframe_distance)

    if ConfigInfo.bucket is None:
        ConfigInfo.bucket = "excamera-%s" % ConfigInfo.region


s3_client = boto3.client('s3', region_name=ConfigInfo.region)

def do_download((key, filename)):
    print "Downloading {}...".format(key)
    s3_client.download_file(ConfigInfo.bucket, key, filename)

def run():
    download_thread_pool = mp.Pool(processes=ConfigInfo.download_threads)

    basekey = "%s-4k-y4m_%02d/out_ssim_%s/" % (ConfigInfo.video_name, ConfigInfo.num_frames, ConfigInfo.quality_str)
    targets = []
    for vNum in range(ConfigInfo.num_offset, ConfigInfo.num_offset + ConfigInfo.num_parts):
        filename = "%08d.txt" % vNum
        prehash = md5.md5(filename).hexdigest()[0:4]
        key = "%s-%s%s" % (prehash, basekey, filename)
        targets += [(key, filename)]

    download_thread_pool.map(do_download, targets)

if __name__ == "__main__":
    get_options()
    run()
