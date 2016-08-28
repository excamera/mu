#!/usr/bin/python

import os
import subprocess
import sys
import traceback

import boto3

from libmu.defs import Defs
from libmu.fd_wrapper import FDWrapper
from libmu.socket_nb import SocketNB
import libmu.util

s3_client = boto3.client('s3')

###
#  set a value
###
def _do_set(msg, vals, to_int):
    res = msg.split(':', 1)

    if len(res) != 2 or len(res[0]) < 1:
        vals['cmdsock'].enqueue('FAIL(invalid syntax for SET)')
        return False

    retval = 'OK:SET(%s)'
    if to_int:
        retval = 'OK:SETI(%s)'
        try:
            res[1] = int(res[1])
        except ValueError:
            vals['cmdsock'].enqueue('FAIL(could not interpret %s as an integer)' % res[1])
            return False

    vals[res[0]] = res[1]
    vals['cmdsock'].enqueue(retval % res[0])

    return False

do_set = lambda m, v: _do_set(m, v, False)
do_seti = lambda m, v: _do_set(m, v, True)

###
#  get a value
###
def _do_get(msg, vals, get_info):
    if vals.get(msg) is None:
        vals['cmdsock'].enqueue('FAIL(no such variable %s)' % msg)
        return False

    if get_info:
        vals['cmdsock'].enqueue('INFO:%s:%s' % (msg, vals[msg]))
        vals['cmdsock'].enqueue('OK:GETI(%s)' % (msg))
    else:
        vals['cmdsock'].enqueue('OK:GET(%s)' % vals[msg])

    return False

do_get = lambda m, v: _do_get(m, v, False)
do_geti = lambda m, v: _do_get(m, v, True)

###
#  dump entire vals dict
###
def do_dump_vals(_, vals):
    vals['cmdsock'].enqueue('OK:DUMP_VALS:%s' % str(vals))
    return False

###
#  run something in the background
###
def _background(runner, vals, queuemsg):
    sock = None
    if vals['nonblock']:
        (r, w) = os.pipe()
        pid = os.fork()

        if pid != 0:
            os.close(w)
            sock = FDWrapper(r)
            sock.set_blocking(False)

            info = vals.setdefault('runinfo', [])
            info.append((pid, SocketNB(sock)))

            vals['cmdsock'].enqueue(queuemsg)
            return False

        else:
            os.close(r)
            sock = FDWrapper(w)

    (donemsg, retval) = runner()

    if sock is None:
        if vals.get('cmdsock') is not None:
            vals['cmdsock'].enqueue(donemsg)
            return False

        else:
            # for mode 0 where we don't connect to a command server

            print donemsg
            return donemsg

    else:
        msg = SocketNB.format_message(donemsg)
        sock.send(msg)
        sock.close()
        sys.exit(retval)

###
#  tell the client to retrieve a segment from S3
###
def do_retrieve(_, vals):
    if 'inkey' not in vals or 'targfile' not in vals or 'bucket' not in vals:
        vals['cmdsock'].enqueue('FAIL(bucket, inkey, or targfile not set)')
        return False

    infile = vals['inkey']
    outfile = vals['targfile']
    bucket = vals['bucket']

    def ret_helper():
        donemsg = 'OK:RETRIEVE(%s/%s)' % (bucket, infile)
        retval = 0
        try:
            s3_client.download_file(bucket, infile, outfile)
        except:
            donemsg = 'FAIL(retrieving from s3:\n%s)' % traceback.format_exc()
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:RETRIEVING(%s/%s)' % (bucket, infile))

###
#  tell the client to upload a segment to s3
###
def do_upload(_, vals):
    if 'outkey' not in vals or 'fromfile' not in vals or 'bucket' not in vals:
        vals['cmdsock'].enqueue('FAIL(bucket, outkey, or fromfile not set)')
        return False

    outfile = vals['outkey']
    infile = vals['fromfile']
    bucket = vals['bucket']

    def ret_helper():
        donemsg = 'OK:UPLOAD(%s/%s)' % (bucket, outfile)
        retval = 0
        try:
            s3_client.upload_file(infile, bucket, outfile)
        except:
            donemsg = 'FAIL(uploading to s3:\n%s)' % traceback.format_exc()
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:UPLOADING(%s/%s)' % (bucket, outfile))

###
#  echo msg back to the server
###
def do_echo(msg, vals):
    vals['cmdsock'].enqueue('OK:ECHO(%s)' % msg)
    return False

###
#  we've been told to quit
###
def do_quit(_, vals):
    vals['cmdsock'].close()
    return True

###
#  run the command
###
def do_run(msg, vals):
    if msg is not None and len(msg) > 0:
        cmdstring = msg
    else:
        cmdstring = Defs.make_cmdstring(msg, vals)

    def ret_helper():
        retval = 0
        try:
            output = subprocess.check_output([cmdstring], shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            retval = e.returncode
            output = e.output

        donemsg = 'OK:RETVAL(%d):OUTPUT(%s):COMMAND(%s)' % (retval, output, cmdstring)

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:RUNNING(%s)' % cmdstring)

###
#  listen for peer lambda
###
def do_listen(_, vals):
    if vals.get('lsnsock') is not None:
        vals['cmdsock'].enqueue("FAIL(already listening)")
        return False

    # SSLize it if necessary
    try:
        cacert = vals.get('cacert')
        srvcrt = vals.get('srvcrt')
        srvkey = vals.get('srvkey')
        ls = libmu.util.listen_socket('0.0.0.0', 0, cacert, srvcrt, srvkey)
    except:
        vals['cmdsock'].enqueue('FAIL(could not open listening socket)')

    if not isinstance(ls, SocketNB):
        vals['cmdsock'].enqueue('FAIL(%s)' % str(ls))
        return True

    # record information and send back to master
    (_, vals['lsnport']) = ls.getsockname()
    vals['lsnsock'] = ls

    vals['cmdsock'].enqueue('INFO:lsnport:%d' % vals['lsnport'])
    vals['cmdsock'].enqueue('OK:LISTEN(%d)' % vals['lsnport'])

    return False

###
#  close listening socket
###
def do_close_listen(_, vals):
    ls = vals.get('lsnsock')
    if ls is not None:
        ls.close()
        vals['lsnsock'] = None

    ps = vals.get('prvsock')
    if ps is not None:
        ps.close()
        vals['prvsock'] = None

    if vals.get('lsnport') is not None:
        del vals['lsnport']

    vals['cmdsock'].enqueue("OK:CLOSE_LISTEN")
    return False

###
#  connect to peer lambda
###
def do_connect(msg, vals):
    if vals.get('nxtsock') is None:
        # already connected
        vals['cmdsock'].enqueue("FAIL(already connected)")
        return False

    try:
        (host, port) = msg.split(':', 1)
        port = int(port)
        cs = libmu.util.connect_socket(host, port, vals.get('cacert'))
    except:
        vals['cmdsock'].enqueue('FAIL(could not parse command)')

    if not isinstance(cs, SocketNB):
        vals['cmdsock'].enqueue('FAIL(%s)' % str(cs))
        return True
    vals['nxtsock'] = cs

    vals['cmdsock'].enqueue('OK:CONNECT(%d)' % msg)
    return False

###
#  close connection to peer lambda
###
def do_close_connect(_, vals):
    ns = vals.get('nxtsock')
    if ns is not None:
        ns.close()
        vals['nxtsock'] = None

    vals['cmdsock'].enqueue('OK:CLOSE_CONNECT')
    return False

###
#  dispatch to handler functions
###
message_types = { 'set:': do_set
                , 'seti:': do_seti
                , 'get:': do_get
                , 'geti:': do_geti
                , 'dump_vals:': do_dump_vals
                , 'retrieve:': do_retrieve
                , 'upload:': do_upload
                , 'echo:': do_echo
                , 'quit:': do_quit
                , 'run:': do_run
                , 'listen:': do_listen
                , 'close_listen:': do_close_listen
                , 'connect:': do_connect
                , 'close_connect:': do_close_connect
                }
def handle_message(msg, vals):
    if Defs.debug:
        print "CLIENT HANDLING %s" % msg

    for mtype in message_types:
        if msg[:len(mtype)] == mtype:
            return message_types[mtype](msg[len(mtype):], vals)

    # if we got here, we don't recognize the command
    vals['cmdsock'].enqueue("FAIL(no such command '%s')" % msg)
    return False

message_responses = { 'set:': 'OK:SET'
                    , 'seti:': 'OK:SETI'
                    , 'get:': 'OK:GET'
                    , 'geti:': 'OK:GETI'
                    , 'dump_vals:': 'OK:DUMP_VALS'
                    , 'retrieve:': 'OK:RETRIEV'
                    , 'upload:': 'OK:UPLOAD'
                    , 'echo:': 'OK:ECHO'
                    , 'run:': 'OK:R'
                    , 'listen:': 'OK:LISTEN'
                    , 'close_listen:': 'OK:CLOSE_LISTEN'
                    , 'connect:': 'OK:CONNECT'
                    , 'close_connect:': 'OK:CLOSE_CONNECT'
                    }
