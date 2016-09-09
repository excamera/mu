#!/usr/bin/python

import cProfile
import getopt
import json
import os
import select
import socket
import sys

from OpenSSL import SSL

import pylaunch
import libmu.defs
import libmu.machine_state
import libmu.util

###
#  handle new connection on server listening socket
###
def _handle_server_sock(ls, states, statemap, num_parts, constructor):
    (ns, _) = ls.accept()
    ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ns.setblocking(False)

    actor_number = len(states)
    nstate = constructor(ns, actor_number)
    nstate.do_handshake()

    states.append(nstate)
    statemap[nstate.fileno()] = actor_number

    if len(states) == num_parts:
        # no need to listen any longer, we have all our connections
        try:
            ls.shutdown()
            ls.close()
        except:
            pass

        ls = None

    return ls

###
#  server: launch a bunch of lambda instances using pylaunch
###
def server_launch(server_info, event, akid, secret):
    if event.get('addr') is None:
        # figure out what the IP address of the interface talking to AWS is
        # NOTE if you have different interfaces routing to different regions
        #      this won't work. I'm assuming that's unlikely.
        testsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        testsock.connect(("lambda." + server_info.regions[0] + ".amazonaws.com", 443))
        event['addr'] = testsock.getsockname()[0]
        testsock.close()

    pid = os.fork()
    if pid == 0:
        # pylint: disable=no-member
        # (pylint can't "see" into C modules)
        pylaunch.launchpar(server_info.num_parts, server_info.lambda_function,
                           akid, secret, json.dumps(event), server_info.regions)
        sys.exit(0)

###
#  set up server listen sock
###
def setup_server_listen(server_info, chainfile=None, keyfile=None):
    # bro, you listening to this?
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(('0.0.0.0', server_info.port_number))
    lsock.listen(server_info.num_parts + 10) # lol like the kernel listens to me

    sslctx = SSL.Context(SSL.TLSv1_2_METHOD)
    sslctx.set_verify_depth(9)
    sslctx.set_options(SSL.OP_NO_COMPRESSION)
    sslctx.set_cipher_list(libmu.defs.Defs.cipher_list)
    sslctx.set_verify(SSL.VERIFY_NONE, lambda *_: True)

    # set up server key
    if chainfile is not None and keyfile is not None:
        sslctx.use_certificate_chain_file(chainfile)
        sslctx.use_privatekey_file(keyfile)
    elif server_info.srvcrtfile is not None and server_info.srvkeyfile is not None:
        sslctx.use_certificate_chain_file(server_info.srvcrtfile)
        sslctx.use_privatekey_file(server_info.srvkeyfile)
    else:
        raise Exception("ERROR: you must supply a server cert and key!")
    sslctx.check_privatekey()

    # set up server SSL connection
    lsock = SSL.Connection(sslctx, lsock)
    lsock.set_accept_state()
    lsock.setblocking(False)

    return lsock

###
#  server mainloop
###
def server_main_loop(states, constructor, server_info, chainfile=None, keyfile=None):
    # handle profiling if specified
    if server_info.profiling:
        pr = cProfile.Profile()
        pr.enable()

    lsock = setup_server_listen(server_info, chainfile, keyfile)
    lsock_fd = lsock.fileno()

    def rwsplit(sts, ret):
        diffs = []
        ret += [0] * (len(sts) - len(ret))
        for (st, idx) in zip(sts, range(0, len(sts))):
            val = 0
            if st.sock is not None:
                if not isinstance(st, libmu.machine_state.TerminalState):
                    val = val | select.POLLIN

                if st.ssl_write or st.want_write:
                    val = val | select.POLLOUT

            if val != ret[idx]:
                ret[idx] = val
                diffs.append(idx)

        return diffs

    statemap = {}
    rwflags = []
    poll_obj = select.poll()
    poll_obj.register(lsock_fd, select.POLLIN)
    npasses_out = 0

    def show_status():
        actStates = len([ v for v in rwflags if v != 0 ])
        errStates = len([ 1 for s in states if isinstance(s, libmu.machine_state.ErrorState) ])
        doneStates = len([ 1 for s in states if isinstance(s, libmu.machine_state.TerminalState) ]) - errStates
        waitStates = server_info.num_parts - len(states)
        print "SERVER status: active=%d, done=%d, prelaunch=%d, error=%d" % (actStates, doneStates, waitStates, errStates)

    while True:
        dflags = rwsplit(states, rwflags)

        if all([ v == 0 for v in rwflags ]) and lsock is None:
            break

        for idx in dflags:
            if rwflags[idx] != 0:
                poll_obj.register(states[idx], rwflags[idx])
            else:
                poll_obj.unregister(states[idx])

        if lsock is None and lsock_fd is not None:
            poll_obj.unregister(lsock_fd)
            lsock_fd = None

        if npasses_out == 100:
            npasses_out = 0
            show_status()

        pfds = poll_obj.poll(1000)
        if len(pfds) == 0:
            if npasses_out != 0:
                show_status()
            pfds = poll_obj.poll(1000 * libmu.defs.Defs.timeout)

        npasses_out += 1

        if len(pfds) == 0:
            # len(rfds) == 0 and len(wfds) == 0:
            print "SERVER TIMEOUT"
            break

        # look for readable FDs
        for (fd, ev) in pfds:
            if (ev & select.POLLIN) != 0:
                if lsock is not None and fd == lsock_fd:
                    lsock = _handle_server_sock(lsock, states, statemap, server_info.num_parts, constructor)

                else:
                    actorNum = statemap[fd]
                    r = states[actorNum]
                    rnext = r.do_read()
                    states[actorNum] = rnext

        for (fd, ev) in pfds:
            if (ev & select.POLLOUT) != 0:
                # reading might have caused this state to get updated,
                # so we index into states to be sure we have the freshest version
                actorNum = statemap[fd]
                w = states[actorNum]
                wnext = w.do_write()
                states[actorNum] = wnext

        for rnext in [ st for st in states if not isinstance(st, libmu.machine_state.TerminalState) ]:
            if rnext.want_handle:
                rnext = rnext.do_handle()
            states[rnext.actorNum] = rnext

    fo = None
    error = []
    errvals = []
    if server_info.out_file is not None:
        fo = open(server_info.out_file, 'w')

    for (state, num) in zip(states, range(0, len(states))):
        state.close()
        if isinstance(state, libmu.machine_state.ErrorState) or not isinstance(state, libmu.machine_state.TerminalState):
            error.append(num)
            errvals.append(repr(state))
        elif fo is not None:
            fo.write("%d:%s\n" % (state.actorNum, str(state.get_timestamps())))

    if server_info.profiling:
        pr.disable()
        pr.dump_stats(server_info.profiling)

    if error:
        evals = str(error) + "\n  " + "\n  ".join(errvals)
        if fo is not None:
            fo.write("ERR:%s\n" % str(error))
            fo.close() # we'll never get to the close below
        raise Exception("ERROR: the following workers terminated abnormally:\n%s" % evals)

    if fo is not None:
        fo.close()

###
#  server usage message
###
def usage(defaults):
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
    if hasattr(defaults, 'num_list'):
        print "  -N a,b,c,...   run clients numbered exactly a, b, c, ...       (None)"
    print "  -n nParts:     launch nParts lambdas                           (%d)" % defaults.num_parts
    if hasattr(defaults, 'num_frames'):
        print "  -f nFrames:    number of frames to process in each chunk       (%d)" % defaults.num_frames
    print "  -o nOffset:    skip this many input chunks when processing     (%d)" % defaults.num_offset
    if hasattr(defaults, 'num_passes'):
        print "  -p nPasses:    number of xcenc passes                          (%d)" % defaults.num_passes
    if hasattr(defaults, 'quality_run'):
        print "  -q qRun:       use quality values in run #qRun                 (%d)" % defaults.quality_run
    if hasattr(defaults, 'quality_s'):
        print "  -S s_ac_qi:    use s_ac_qi for S quantizer                     (%d)" % defaults.quality_s
    if hasattr(defaults, 'quality_y'):
        print "  -Y y_ac_qi:    use y_ac_qi for Y quantizer                     (%d)" % defaults.quality_y
    print
    print "  -v vidName:    video name                                      ('%s')" % defaults.video_name
    print "  -b bucket:     S3 bucket in which videos are stored            ('%s')" % defaults.bucket
    if hasattr(defaults, 'in_format'):
        print "  -i inFormat:   input format ('png16', 'y4m_06', etc)           ('%s')" % defaults.in_format
    print
    print "  -t portNum:    listen on portNum                               (%d)" % defaults.port_number
    print
    print "  -l fnName:     lambda function name                            ('%s')" % defaults.lambda_function
    print "  -r r1,r2,...:  comma-separated list of regions                 ('%s')" % ','.join(defaults.regions)
    print
    print "  -c caCert:     CA certificate file                             (None)"
    print "  -s srvCert:    server certificate file                         (None)"
    print "  -k srvKey:     server key file                                 (None)"
    print "(hint: you can generate new keys with <mu>/bin/genkeys.sh)"

def options(server_info):
    defaults = server_info()

    try:
        opts, args = getopt.getopt(sys.argv[1:], "o:f:n:v:l:r:Dc:s:k:i:b:hO:P:p:N:t:q:S:Y:")
    except getopt.GetoptError as err:
        print str(err)
        usage(defaults)
        sys.exit(1)

    if len(args) > 0:
        print "ERROR: Extraneous arguments '%s'" % ' '.join(args)
        print
        usage(defaults)
        sys.exit(1)

    server_info.cacertfile = os.environ.get('CA_CERT')
    server_info.srvcrtfile = os.environ.get('SRV_CERT')
    server_info.srvkeyfile = os.environ.get('SRV_KEY')

    for (opt, arg) in opts:
        if opt == "-o":
            if hasattr(server_info, 'num_list'):
                assert server_info.num_list is None, "You cannot specify both -N and -o!!!"
            server_info.num_offset = int(arg)
        elif opt == "-f":
            server_info.num_frames = int(arg)
        elif opt == "-n":
            if hasattr(server_info, 'num_list'):
                assert server_info.num_list is None, "You cannot specify both -N and -n!!!"
            server_info.num_parts = int(arg)
        elif opt == "-v":
            server_info.video_name = arg
        elif opt == "-l":
            server_info.lambda_function = arg
        elif opt == "-r":
            server_info.regions = arg.split(',')
        elif opt == "-D":
            libmu.defs.Defs.debug = True
        elif opt == "-c":
            server_info.cacertfile = arg
        elif opt == "-s":
            server_info.srvcrtfile = arg
        elif opt == "-k":
            server_info.srvkeyfile = arg
        elif opt == "-b":
            server_info.bucket = arg
        elif opt == "-i":
            server_info.in_format = arg
        elif opt == "-h":
            usage(defaults)
            sys.exit(1)
        elif opt == "-O":
            server_info.out_file = arg
        elif opt == "-P":
            server_info.profiling = arg
        elif opt == "-p":
            server_info.num_passes = int(arg)
        elif opt == "-N":
            vals = arg.replace(' ', '').split(',')
            server_info.num_list = []
            for val in vals:
                if len(val) > 0:
                    server_info.num_list.append(int(val))
            server_info.num_parts = len(server_info.num_list)
        elif opt == "-t":
            server_info.port_number = int(arg)
        elif opt == "-q":
            server_info.quality_run = int(arg)
        elif opt == "-S":
            server_info.quality_s = int(arg)
        elif opt == "-Y":
            server_info.quality_y = int(arg)
        else:
            assert False, "logic error: got unexpected option %s from getopt" % opt

    if len(server_info.regions) == 0:
        print "ERROR: region list cannot be empty"
        print
        usage(defaults)
        sys.exit(1)

    if os.environ.get("AWS_ACCESS_KEY_ID") is None or os.environ.get("AWS_SECRET_ACCESS_KEY") is None:
        print "ERROR: You must set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY envvars"
        print
        usage(defaults)
        sys.exit(1)

    for f in [server_info.cacertfile, server_info.srvcrtfile, server_info.srvkeyfile]:
        try:
            os.stat(str(f))
        except:
            print "ERROR: Cannot open SSL cert or key file '%s'" % str(f)
            print
            usage(defaults)
            sys.exit(1)

    server_info.cacert = libmu.util.read_pem(server_info.cacertfile)
    server_info.srvcrt = libmu.util.read_pem(server_info.srvcrtfile)
    server_info.srvkey = libmu.util.read_pem(server_info.srvkeyfile)
