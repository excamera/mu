#!/usr/bin/python

import cProfile
import datetime
import fcntl
import getopt
import json
import os
import select
import socket
import struct
import sys
import termios
import time

import pylaunch
import libmu.defs
import libmu.machine_state
import libmu.util

###
#  handle new connection on server listening socket
###
def _handle_server_sock(ls, states, state_fd_map, state_actNum_map, server_info, constructor):
    (ns, _) = ls.accept()
    ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ns.setblocking(False)

    this_actor = len(states)
    if getattr(server_info, 'keyframe_distance', None) is not None:
        (actor_number, group_number, _) = _compute_actor_number(this_actor, server_info.keyframe_distance, server_info.num_parts)
    else:
        actor_number = this_actor
        group_number = None

    nstate = constructor(ns, actor_number)
    nstate.do_handshake()
    if hasattr(nstate, 'info') and group_number is not None:
        nstate.info['actor_group_number'] = group_number

    states.append(nstate)
    state_fd_map[nstate.fileno()] = this_actor
    state_actNum_map[actor_number] = this_actor

    if len(states) == server_info.num_parts:
        # no need to listen any longer, we have all our connections
        try:
            ls.shutdown()
            ls.close()
        except:
            pass

        ls = None

    return ls

###
#  rotating goose: compute where this actor goes
###
def _compute_actor_number(thisAct, kfDist, numParts):
    rem = numParts % kfDist
    numGroups = numParts // kfDist + (1 if rem != 0 else 0)

    if rem == 0 or thisAct < numGroups * rem:
        thisGroup = thisAct % numGroups
        thisPlace = thisAct // numGroups
    else:
        effAct = thisAct - rem * numGroups
        effGroups = numGroups - 1
        thisGroup = effAct % effGroups
        thisPlace = rem + effAct // effGroups

    actorNum = thisGroup * kfDist + thisPlace
    return (actorNum, thisGroup, thisPlace)

###
#  test the above function
###
def _test_compute(kfDist, numParts):
    testvec = ['_' * kfDist] * (numParts // kfDist)
    if numParts % kfDist != 0:
        testvec += ['_' * (numParts % kfDist)]
    print str(testvec)

    for i in range(0, numParts):
        (actorNum, thisGroup, thisPlace) = _compute_actor_number(i, kfDist, numParts)
        this_string = testvec[thisGroup]
        this_string = this_string[:thisPlace] + 'x' + this_string[thisPlace + 1:]
        testvec[thisGroup] = this_string
        print str(testvec), i, actorNum, thisGroup, thisPlace

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
        total_parts = server_info.num_parts + getattr(server_info, 'overprovision', 0)
        pylaunch.launchpar(total_parts, server_info.lambda_function, akid, secret, json.dumps(event), server_info.regions)
        sys.exit(0)

###
#  set up server listen sock
###
def setup_server_listen(server_info):
    return libmu.util.listen_socket('0.0.0.0', server_info.port_number, server_info.cacert, server_info.srvcrt, server_info.srvkey, server_info.num_parts + 10)

###
#  server mainloop
###
def server_main_loop(states, constructor, server_info):
    server_info.start_time = time.time()
    # handle profiling if specified
    if server_info.profiling:
        pr = cProfile.Profile()
        pr.enable()

    lsock = setup_server_listen(server_info)
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

            else:
                ret[idx] = 0
                diffs.append(idx)
                if not isinstance(st, libmu.machine_state.TerminalState):
                    sts[idx] = libmu.machine_state.ErrorState(sts[idx], "sock closed in %s" % str(sts[idx]))

        return diffs

    state_fd_map = {}
    state_actNum_map = {}
    rwflags = []
    poll_obj = select.poll()
    poll_obj.register(lsock_fd, select.POLLIN)
    npasses_out = 0
    start_time = time.time()

    try:
        (screen_height, screen_width, _, _) = struct.unpack("HHHH", fcntl.ioctl(0, termios.TIOCGWINSZ, struct.pack("HHHH", 0, 0, 0, 0)))
    except:
        screen_width = 80
        screen_height = 50
    n_per_line = 1 + server_info.num_parts // screen_height
    n_chars_maybe = max(screen_width // n_per_line, 24)
    n_across = max(screen_width // n_chars_maybe, 1)
    n_chars = screen_width // n_across

    def show_status():
        actStates = len([ 1 for v in rwflags if v != 0 ])
        errStates = len([ 1 for s in states if isinstance(s, libmu.machine_state.ErrorState) ])
        doneStates = len([ 1 for s in states if isinstance(s, libmu.machine_state.TerminalState) ]) - errStates
        waitStates = server_info.num_parts - len(states)
        runTime = str(datetime.timedelta(seconds=time.time() - start_time))

        # enhanced output in debugging mode
        if errStates == 0 and not libmu.defs.Defs.debug:
            # make output pretty as long as there aren't errors
            sys.stdout.write("\033[3J\033[H\033[2J")
            sys.stdout.flush()
        n_printed = 0
        for s in states:
            s_str = str(s)
            sys.stdout.write(s_str[:n_chars])
            sys.stdout.write(' ' * (n_chars - min(len(s_str), n_chars)))
            n_printed += 1
            if n_printed == n_across:
                sys.stdout.write('\n')
                n_printed = 0
            else:
                sys.stdout.write(' ')
        if n_printed != 0:
            sys.stdout.write("\n")
        sys.stdout.write("SERVER status (%s): active=%d, done=%d, prelaunch=%d, error=%d\n" % (runTime, actStates, doneStates, waitStates, errStates))
        sys.stdout.flush()

    while True:
        dflags = rwsplit(states, rwflags)

        if all([ v == 0 for v in rwflags ]) and lsock is None:
            break

        for idx in dflags:
            if rwflags[idx] != 0:
                poll_obj.register(states[idx], rwflags[idx])
            else:
                try:
                    poll_obj.unregister(states[idx])
                except:
                    pass

        if lsock is None and lsock_fd is not None:
            poll_obj.unregister(lsock_fd)
            lsock_fd = None

        if npasses_out == 100:
            npasses_out = 0
            show_status()

        pfds = poll_obj.poll(2000)
        npasses_out += 1

        if len(pfds) == 0:
            show_status()
            continue

        # look for readable FDs
        for (fd, ev) in pfds:
            if (ev & select.POLLIN) != 0:
                if lsock is not None and fd == lsock_fd:
                    lsock = _handle_server_sock(lsock, states, state_fd_map, state_actNum_map, server_info, constructor)

                else:
                    stateIdx = state_fd_map[fd]
                    r = states[stateIdx]
                    rnext = r.do_read()
                    states[stateIdx] = rnext

        for (fd, ev) in pfds:
            if (ev & select.POLLOUT) != 0:
                stateIdx = state_fd_map[fd]
                w = states[stateIdx]
                wnext = w.do_write()
                states[stateIdx] = wnext

        for rnext in [ st for st in states if not isinstance(st, libmu.machine_state.TerminalState) ]:
            if rnext.want_handle:
                rnext = rnext.do_handle()
            stateIdx = state_actNum_map[rnext.actorNum]
            states[stateIdx] = rnext

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
            timestamps = [ ts - server_info.start_time for ts in state.get_timestamps() ]
            fo.write("%d:%s\n" % (state.actorNum, str(timestamps)))

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
#  generate server usage message and optstring from ServerInfo object
###
def usage_str(defaults):
    oStr = ""
    uStr = "Usage: %s [args ...]\n\n" % sys.argv[0]

    if hasattr(defaults, 'lambda_function'):
        uStr += "You must also set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY envvars.\n\n"

    uStr += "  switch         description                                     default\n"
    uStr += "  --             --                                              --\n"
    uStr += "  -U:            show this message\n"
    uStr += "  -D:            enable debug                                    (disabled)\n"
    oStr += "UD"

    if hasattr(defaults, 'out_file'):
        oFileStr = "'%s'" % defaults.out_file if defaults.out_file is not None else "None"
        uStr += "  -O oFile:      state machine times output file                 (%s)\n" % oFileStr
        oStr += "O:"

    if hasattr(defaults, 'profiling'):
        pFileStr = "'%s'" % defaults.profiling if defaults.profiling is not None else "None"
        uStr += "  -P pFile:      profiling data output file                      (%s)\n" % pFileStr
        oStr += "P:"

    uStr += "\n  -n nParts:     launch nParts lambdas                           (%d)\n" % defaults.num_parts
    oStr += "n:"

    if hasattr(defaults, 'overprovision'):
        uStr += "  -X nExtra:     overprovision lambda invocations by nExtra      (%d)\n" % defaults.overprovision
        oStr += "X:"

    if hasattr(defaults, 'num_list'):
        uStr += "  -N a,b,c,...   run clients numbered exactly a, b, c, ...       (None)\n"
        oStr += "N:"

    if hasattr(defaults, 'num_frames'):
        uStr += "  -f nFrames:    number of frames to process in each chunk       (%d)\n" % defaults.num_frames
        oStr += "f:"

    if hasattr(defaults, 'num_offset'):
        uStr += "  -o nOffset:    skip this many input chunks when processing     (%d)\n" % defaults.num_offset
        oStr += "o:"

    if hasattr(defaults, 'quality_values'):
        qvals_str = ','.join([ str(x) for x in defaults.quality_values ])
        uStr += "\n  -q qvals:      use qvals as the quality values                 (%s)\n" % qvals_str
        oStr += "q:"

    if hasattr(defaults, 'run_xcenc'):
        uStr += "  -x:            run xc-enc                                      (run vpxenc)\n"
        oStr += "x"

    if hasattr(defaults, 'quality_y'):
        uStr += "  -Y y_ac_qi:    use y_ac_qi for Y quantizer                     (%d)\n" % defaults.quality_y
        oStr += "Y:"

    if hasattr(defaults, 'quality_s'):
        uStr += "  -S s_ac_qi:    use s_ac_qi for Y quantizer                     (%s)\n" % str(defaults.quality_s)
        oStr += "S:"

    if hasattr(defaults, 'keyframe_distance'):
        uStr += "  -K kfDist:     force keyframe every kfDist chunks              (%s)\n" % str(defaults.keyframe_distance)
        oStr += "K:"

    if hasattr(defaults, 'upload_states'):
        uStr += "  -u:            upload prev.state and final.state               (%s)\n" % str(defaults.upload_states)
        oStr += "u"

    if hasattr(defaults, 'num_passes'):
        num_pass_str = ','.join([ str(x) for x in defaults.num_passes ])
        min_pass_str = ','.join([ str(x) for x in defaults.min_passes ])
        uStr += "  -p w,x,y,z:    ph1,ph2,ph3,ph4 num passes                      (%s)\n" % num_pass_str
        uStr += "                 min for each phase is %s\n" % min_pass_str
        oStr += "p:"

    if hasattr(defaults, 'video_name'):
        uStr += "\n  -v vidName:    video name                                      ('%s')\n" % defaults.video_name
        oStr += "v:"

    if hasattr(defaults, 'bucket'):
        uStr += "  -b bucket:     S3 bucket in which videos are stored            ('%s')\n" % defaults.bucket
        oStr += "b:"

    if hasattr(defaults, 'in_format'):
        uStr += "  -i inFormat:   input format ('png16', 'y4m_06', etc)           ('%s')\n" % defaults.in_format
        oStr += "i:"

    if hasattr(defaults, 'host_addr'):
        uStr += "  -h hostAddr:   this server's address                           (auto)\n"
        oStr += "h:"

    uStr += "\n  -t portNum:    listen on portNum                               (%d)\n" % defaults.port_number
    oStr += "t:"

    if hasattr(defaults, 'state_srv_addr'):
        uStr += "  -H stHostAddr: hostname or IP for nat punching host            (%s)\n" % defaults.state_srv_addr
        oStr += "H:"

    if hasattr(defaults, 'state_srv_port'):
        uStr += "  -T stHostPort: port number for nat punching host               (%s)\n" % defaults.state_srv_port
        oStr += "T:"

    if hasattr(defaults, 'lambda_function'):
        uStr += "  -l fnName:     lambda function name                            ('%s')\n" % defaults.lambda_function
        oStr += "l:"

    if hasattr(defaults, 'regions'):
        uStr += "  -r r1,r2,...:  comma-separated list of regions                 ('%s')\n" % ','.join(defaults.regions)
        oStr += "r:"

    uStr += "\n  -c caCert:     CA certificate file                             (None)\n"
    uStr += "  -s srvCert:    server certificate file                         (None)\n"
    uStr += "  -k srvKey:     server key file                                 (None)\n"
    uStr += "     (hint: you can generate new keys with <mu>/bin/genkeys.sh)\n"
    uStr += "     (hint: you can use CA_CERT, SRV_CERT, SRV_KEY envvars instead)\n"
    oStr += "c:s:k:"

    return (uStr, oStr)

def to_numlist(arg, outlist):
    vals = arg.replace(' ', '').split(',')
    del outlist[:]
    for val in vals:
        if len(val) > 0:
            outlist.append(int(val))

def options(server_info):
    (uStr, oStr) = usage_str(server_info)

    try:
        opts, args = getopt.getopt(sys.argv[1:], oStr)
    except getopt.GetoptError as err:
        print str(err)
        print uStr
        sys.exit(1)

    if len(args) > 0:
        print "ERROR: Extraneous arguments '%s'" % ' '.join(args)
        print
        print uStr
        sys.exit(1)

    cacertfile = os.environ.get('CA_CERT')
    srvcrtfile = os.environ.get('SRV_CERT')
    srvkeyfile = os.environ.get('SRV_KEY')

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
            cacertfile = arg
        elif opt == "-s":
            srvcrtfile = arg
        elif opt == "-k":
            srvkeyfile = arg
        elif opt == "-b":
            server_info.bucket = arg
        elif opt == "-i":
            server_info.in_format = arg
        elif opt == "-U":
            print uStr
            sys.exit(1)
        elif opt == "-O":
            server_info.out_file = arg
        elif opt == "-P":
            server_info.profiling = arg
        elif opt == "-p":
            try:
                (p1, p2, p3, p4) = arg.replace(' ', '').split(',')
                p1 = int(p1)
                p2 = int(p2)
                p3 = int(p3)
                p4 = int(p4)
                assert p1 >= server_info.min_passes[0] and \
                       p2 >= server_info.min_passes[1] and \
                       p3 >= server_info.min_passes[2] and \
                       p4 >= server_info.min_passes[3]
                server_info.num_passes = (p1, p2, p3, p4)
                server_info.tot_passes = sum(server_info.num_passes)
            except AssertionError:
                print "ERROR: Invalid number of passes specified."
                print uStr
                sys.exit(1)
            except:
                print "ERROR: Invalid argument to -p: '%s'" % arg
                print uStr
                sys.exit(1)
        elif opt == "-N" or opt == "-q":
            to_numlist(arg, server_info.num_list)
            server_info.num_parts = len(server_info.num_list)
            assert len(server_info.num_list) > 0
        elif opt == "-t":
            server_info.port_number = int(arg)
        elif opt == "-h":
            server_info.host_addr = arg
        elif opt == "-q":
            to_numlist(arg, server_info.quality_values)
            server_info.quality_valstring = '_'.join([ str(x) for x in server_info.quality_values ])
            assert len(server_info.quality_values) > 0
        elif opt == "-Y":
            server_info.quality_y = int(arg)
        elif opt == "-H":
            server_info.state_srv_addr = arg
        elif opt == "-T":
            server_info.state_srv_port = int(arg)
        elif opt == "-x":
            server_info.run_xcenc = True
        elif opt == "-u":
            server_info.upload_states = True
        elif opt == "-K":
            server_info.keyframe_distance = int(arg)
        elif opt == "-X":
            server_info.overprovision = int(arg)
        elif opt == "-S":
            server_info.quality_s = int(arg)
        else:
            assert False, "logic error: got unexpected option %s from getopt" % opt

    if hasattr(server_info, 'quality_y') and hasattr(server_info, 'quality_str'):
        qs = getattr(server_info, 'quality_s', None)
        qStr = str(qs) if qs is not None else 'x'
        ks = getattr(server_info, 'keyframe_distance', None)
        kStr = ('_k%d' % ks) if ks is not None else ''
        server_info.quality_str = "%d_%s%s" % (server_info.quality_y, qStr, kStr)

    if hasattr(server_info, 'regions') and len(server_info.regions) == 0:
        print "ERROR: region list cannot be empty"
        print
        print uStr
        sys.exit(1)

    if hasattr(server_info, 'lambda_function') and (os.environ.get("AWS_ACCESS_KEY_ID") is None or os.environ.get("AWS_SECRET_ACCESS_KEY") is None):
        print "ERROR: You must set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY envvars"
        print
        print uStr
        sys.exit(1)

    if hasattr(server_info, 'num_passes'):
        if getattr(server_info, 'keyframe_distance', None):
            server_info.num_passes = (1, server_info.keyframe_distance, 0, 0)
        else:
            assert server_info.num_passes[1] == 0, "In swframe mode, phase two is not supported."

    for f in [cacertfile, srvcrtfile, srvkeyfile]:
        try:
            os.stat(str(f))
        except:
            print "ERROR: Cannot open SSL cert or key file '%s'" % str(f)
            print
            print uStr
            sys.exit(1)

    server_info.cacert = libmu.util.read_pem(cacertfile)
    server_info.srvcrt = libmu.util.read_pem(srvcrtfile)
    server_info.srvkey = libmu.util.read_pem(srvkeyfile)
