#!/usr/bin/python
import json
import md5
import os
import select
import shutil
import socket
import tempfile
import zlib
import time

from OpenSSL import SSL

from libmu import SocketNB, Defs, util, handler

###
#  send state file to stsock
###
def finished_run(msg, vals):
    # didn't run xc-enc, so nothing to do
    if msg.find('xc-enc') == -1 and msg.find('xc-dump') == -1 and msg.find('xc-terminate-chunk') == -1:
        return

    # we can delete $(($j - 2)).state now
    if vals['run_iter'] > 1:
        try:
            os.unlink("%s/%d.state" % (vals['_tmpdir'], vals['run_iter'] - 2))
        except:
            # if the file didn't exist (because of flying goose) that's OK
            pass

    # preserve previous encoding result
    shutil.copy(vals['_tmpdir'] + "/output.ivf", vals['_tmpdir'] + "/prev.ivf")

    # if we don't have a next neighbor, we're done
    if not vals.get('send_statefile') or vals.get('stsock') is None:
        return

    # send output state to next worker
    with open(vals['_tmpdir'] + "/final.state", 'r') as f:
        indata = ("STATE(%d):" % vals['run_iter']) + zlib.compress(f.read())

    vals['stsock'].enqueue(indata)

###
#  get state file from stsock
###
def get_input_state(vals):
    indata = vals['stsock'].dequeue()
    (msg, data) = indata.split(':', 1)

    if Defs.debug:
        print "CLIENT received from neighbor: %s... (%d)" % (msg, len(data))

    assert msg[:6] == "STATE("
    lind = 6
    rind = msg.find(')')
    statenum = int(msg[lind:rind])

    with open(vals['_tmpdir'] + "/temp.state", 'w') as f:
        f.write(zlib.decompress(data))

    # NOTE we write to a tmpfile and rename because renaming is atomic!
    os.rename(vals['_tmpdir'] + "/temp.state", vals['_tmpdir'] + "/%d.state" % statenum)

###
#  figure out which sockets need to be selected
###
def get_arwsocks(vals):
    # asocks is all extant sockets
    socknames = ['cmdsock', 'stsock']
    asocks = [ s for s in [ vals.get(n) for n in socknames ] if s is not None ] + \
             [ info[1] for info in vals.setdefault('runinfo', []) ]

    # rsocks is all objects that we could select upon
    rsocks = [ s for s in asocks
                 if isinstance(s, socket.SocketType)
                 or isinstance(s, SSL.Connection)
                 or (isinstance(s, SocketNB) and s.sock is not None) ]

    # wsocks is all rsocks that indicate they want to be written
    wsocks = [ s for s in asocks if isinstance(s, SocketNB) and (s.ssl_write or s.want_write) ]

    return (asocks, rsocks, wsocks)

###
#  make command string
###
def make_cmdstring(msg, vals):
    if msg is not None and len(msg) > 0:
        command = msg
    else:
        command = Defs.cmdstring

    def vals_lookup(name, aslist = False):
        out = vals.get('cmd%s' % name)
        if out is None:
            out = vals['event'].get(name)

        if out is not None and aslist and not isinstance(out, list):
            out = [out]

        return out

    # environment variables
    usevars = vals_lookup('vars', True)
    if usevars is not None:
        command = ' '.join(usevars) + ' ' + command

    # arguments
    useargs = vals_lookup('args', True)
    if useargs is not None:
        command += ' ' + ' '.join(useargs)

    # quality setting
    usequality = vals_lookup('quality', False)
    if usequality is not None:
        command = command.replace("##QUALITY##", usequality)

    # infile
    useinfile = vals_lookup('infile', False)
    if useinfile is not None:
        command = command.replace('##INFILE##', useinfile)

    # outfile
    useoutfile = vals_lookup('outfile', False)
    if useoutfile is not None:
        command = command.replace('##OUTFILE##', useoutfile)

    # statefile
    if vals['run_iter'] > 0 and vals['expect_statefile']:
        instatefile = "##TMPDIR##/%d.state" % (vals['run_iter'] - 1)
        instatewait = '( while [ ! -f "%s" ]; do sleep 0.025; done; echo "hi" ) | ' % instatefile
        instateswitch = '-r -I "%s" -p "##TMPDIR##/prev.ivf"' % instatefile

        if vals['run_iter'] > 1:
            # goose
            if vals.get('minimal_recode'):
                instateswitch += ' -S "##TMPDIR##/0.state"'
            else:
                instateswitch += ' -S "##TMPDIR##/%d.state"' % (vals['run_iter'] - 2)
    else:
        instatewait = ""
        instateswitch = ""
    command = command.replace("##INSTATEWAIT##", instatewait)
    command = command.replace("##INSTATESWITCH##", instateswitch)

    # local tempdir
    # NOTE this replacement must come last because other replacements might refer to ##TMPDIR##
    command = command.replace("##TMPDIR##", vals['_tmpdir'])

    if Defs.debug:
        print "CLIENT running '%s'" % command

    return command

###
#  process strings for s3 commands before uploading
###
def make_urstring(msg, vals, keyk, filek):
    bucket = vals.get('bucket')
    if msg != "":
        cmd_data = msg.split('\0', 1)
        key = filename = None
        if len(cmd_data) == 2:
            key = cmd_data[0]
            filename = cmd_data[1]
    else:
        key = vals.get(keyk)
        filename = vals.get(filek)

    success = bucket is not None and key is not None and filename is not None

    if success:
        filename = filename.replace("##TMPDIR##", vals['_tmpdir'])

    if vals.get('hash_s3keys'):
        hashval = md5.md5(key.split('/')[-1]).hexdigest()[0:4]
        key = "%s-%s" % (hashval, key)

    return (success, bucket, key, filename)

make_uploadstring = lambda m, v: make_urstring(m, v, 'outkey', 'fromfile')
make_retrievestring = lambda m, v: make_urstring(m, v, 'inkey', 'targfile')

###
#  lambda enters here
###
def lambda_handler(event, _):
    lambda_start_ts = time.time()

    Defs.cmdstring = cmdstring
    Defs.make_cmdstring = staticmethod(make_cmdstring)
    Defs.make_retrievestring = staticmethod(make_retrievestring)
    Defs.make_uploadstring = staticmethod(make_uploadstring)

    # get config info from event
    port = int(event.get('port', 13579))
    mode = int(event.get('mode', 0))
    addr = event.get('addr', '127.0.0.1')
    bucket = event.get('bucket', 'excamera-us-west-1')
    region = event.get('region', 'us-east-1')
    cacert = event.get('cacert')
    srvkey = event.get('srvkey')
    srvcrt = event.get('srvcrt')
    nonblock = int(event.get('nonblock', 0))
    expect_statefile = int(event.get('expect_statefile', 0))
    send_statefile = int(event.get('send_statefile', 0))
    rm_tmpdir = int(event.get('rm_tmpdir', 1))
    bg_silent = int(event.get('bg_silent', 0))
    minimal_recode = int(event.get('minimal_recode', 0))
    hash_s3keys = int(event.get('hash_s3keys', 0))

    if rm_tmpdir:
        os.system("rm -rf /tmp/*")

    vals = { 'bucket': bucket
           , 'region': region
           , 'event': event
           , 'cacert': cacert
           , 'srvkey': srvkey
           , 'srvcrt': srvcrt
           , 'nonblock': nonblock
           , 'expect_statefile': expect_statefile
           , 'send_statefile': send_statefile
           , 'rm_tmpdir': rm_tmpdir
           , 'bg_silent': bg_silent
           , 'minimal_recode': minimal_recode
           , 'run_iter': 0
           , 'hash_s3keys': hash_s3keys
           , '_tmpdir': tempfile.mkdtemp(prefix="lambda_", dir="/tmp")
           }
    # default: just run the command and exit
    if mode == 0:
        return handler.do_run('', {'event': event})

    s = util.connect_socket(addr, port, cacert, srvcrt, srvkey)
    s.enqueue(json.dumps({'lambda_function': event.get('lambda_function'), 'lambda_start_ts': lambda_start_ts}))  # send init msg

    if not isinstance(s, SocketNB):
        return str(s)
    vals['cmdsock'] = s
    vals['cmdsock'].enqueue('OK:HELLO')

    while True:
        (_, rsocks, wsocks) = get_arwsocks(vals)
        if len(rsocks) == 0 and len(wsocks) == 0:
            if Defs.debug:
                print "***WARNING*** unclean client exit"
            break
        try:
            (rfds, wfds, _) = select.select(rsocks, wsocks, [], Defs.timeout)
        except Exception as e:
            print "error:", e, "rsocks:", rsocks, "wsocks", wsocks
            raise e

        if len(rfds) == 0 and len(wfds) == 0 and len(vals.setdefault('runinfo', [])) == 0:
            print "CLIENT TIMEOUT"
            break

        # do all the reads we can
        for r in rfds:
            r.do_read()

        # launch any writes we can
        for w in wfds:
            w.do_write()

        # if the command sock is dead, we are dead
        if vals.get('cmdsock') is None:
            break

        ### cmdsock
        # handle commands in the cmdsock queue
        break_outer = False
        while vals['cmdsock'].want_handle and not break_outer:
            nxt = vals['cmdsock'].dequeue()
            break_outer = handler.handle_message(nxt, vals)

        if break_outer:
            break

        ### runsocks
        # if we get something from the runsock, handle it (and kill the sock)
        # (iterate in reverse because we want to be able to remove entries)
        for i in reversed(range(0, len(vals.setdefault('runinfo', [])))):
            (pid, sock) = vals['runinfo'][i]
            if sock.want_handle:
                del vals['runinfo'][i]

                # we only receive something after the runner is done
                # the return value is already packed into the outmsg, so no need to get it here
                os.waitpid(pid, 0)

                outmsg = sock.dequeue()
                sock.close()
                del sock

                vals['cmdsock'].enqueue(outmsg)

                if outmsg[:12] == "OK:RETVAL(0)":
                    finished_run(outmsg, vals)

        if vals.get('stsock') is not None and vals['stsock'].want_handle:
            # handle receiving new state file from previous lambda
            get_input_state(vals)

    (afds, _, _) = get_arwsocks(vals)
    for a in afds:
        # try to be nice... but not too hard
        try:
            a.shutdown(socket.SHUT_RDWR)
        except:
            pass

        try:
            a.shutdown()
        except:
            pass

        try:
            a.close()
        except:
            pass

    if vals.get('rm_tmpdir') and vals.get('_tmpdir') is not None:
        shutil.rmtree(vals.get('_tmpdir'))

cmdstring = ''
