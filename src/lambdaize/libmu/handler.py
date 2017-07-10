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

            if not vals.get('bg_silent'):
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
def do_retrieve(msg, vals):
    (success, bucket, key, filename) = Defs.make_retrievestring(msg, vals)
    if not success:
        vals['cmdsock'].enqueue('FAIL(could not compute download params)')
        return False

    def ret_helper():
        donemsg = 'OK:RETRIEVE(%s/%s)' % (bucket, key)
        retval = 0
        try:
            s3_client.download_file(bucket, key, filename)
        except:
            donemsg = 'FAIL(retrieving %s:%s->%s from s3:\n%s)' % (bucket, key, filename, traceback.format_exc())
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:RETRIEVING(%s/%s->%s)' % (bucket, key, filename))

###
#  tell the client to upload a segment to s3
###
def do_upload(msg, vals):
    (success, bucket, key, filename) = Defs.make_uploadstring(msg, vals)
    if not success:
        vals['cmdsock'].enqueue('FAIL(could not compute upload params)')
        return False

    def ret_helper():
        donemsg = 'OK:UPLOAD(%s/%s)' % (bucket, key)
        retval = 0
        try:
            s3_client.upload_file(filename, bucket, key)
        except:
            donemsg = 'FAIL(uploading %s->%s:%s to s3:\n%s)' % (filename, bucket, key, traceback.format_exc())
            retval = 1

        return (donemsg, retval)

    return _background(ret_helper, vals, 'OK:UPLOADING(%s->%s/%s)' % (filename, bucket, key))

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
#  connect to peer lambda
###
def do_connect(msg, vals):
    if vals.get('stsock') is not None:
        # already connected
        vals['cmdsock'].enqueue("FAIL(already connected)")
        return False

    try:
        (host, port, tosend) = msg.split(':', 2)
        port = int(port)
        cs = libmu.util.connect_socket(host, port, vals.get('cacert'), vals.get('srvcrt'), vals.get('srvkey'))
    except Exception as e: # pylint: disable=broad-except
        vals['cmdsock'].enqueue('FAIL(%s)' % str(e))
        return False

    if not isinstance(cs, SocketNB):
        vals['cmdsock'].enqueue('FAIL(%s)' % str(cs))
        return True
    vals['stsock'] = cs

    if len(tosend) > 0:
        vals['stsock'].enqueue(tosend)
    vals['cmdsock'].enqueue('OK:CONNECT(%s)' % msg)
    return False

###
#  close connection to peer lambda
###
def do_close_connect(_, vals):
    ns = vals.get('stsock')
    if ns is not None:
        ns.close()
        vals['stsock'] = None

    vals['cmdsock'].enqueue('OK:CLOSE_CONNECT')
    return False


def do_emit(msg, vals):
    """Emit the whole directory to the intermediate store (including filenames if possible)
    msg := local_dir_to_emit URI
    URI := s3://key (key includes bucket name) | redis://key | file://local_dir (file:// needs a relay server or NAT traverse)
    """

    local_dir = msg.split(' ', 1)[0]
    local_dir = local_dir.replace("##TMPDIR##", vals['_tmpdir'])
    local_dir = local_dir.rstrip('/')

    protocol = msg.split(' ', 1)[1].split('://', 1)[0]
    key = msg.split(' ', 1)[1].split('://', 1)[1]

    filelist = os.listdir(local_dir)

    donemsg = 'OK:EMIT(%s->%s)' % (local_dir, msg.split(' ', 1)[1])

    if protocol == 's3':
        bucket = key.split('/', 1)[0]
        prefix = key.split('/', 1)[1].rstrip('/')
        for f in filelist:
            try:
                s3_client.upload_file(local_dir+'/'+f, bucket, prefix+'/'+f)
            except:
                donemsg = 'FAIL:EMIT(%s->%s\n%s)' % (local_dir, bucket+'/'+prefix+'/'+f, traceback.format_exc())
                break

    elif protocol == 'redis':
        raise Exception('not implemented yet')

    elif protocol == 'file':
        raise Exception('not implemented yet')

    else:
        donemsg = 'FAIL(unknown protocol: %s)' % protocol

    if vals.get('cmdsock') is not None:
        vals['cmdsock'].enqueue(donemsg)
    return False


def do_collect(msg, vals):
    """Collect the whole directory from the intermediate store (including filenames if possible)
    msg := URI local_dir_to_store
    URI := s3://key (key includes bucket name) | redis://key | file://worker_id/local_dir (file:// needs a relay server or NAT traverse)
    """
    local_dir = msg.split(' ', 1)[1]
    local_dir = local_dir.replace("##TMPDIR##", vals['_tmpdir'])
    local_dir = local_dir.rstrip('/')

    protocol = msg.split(' ', 1)[0].split('://', 1)[0]
    key = msg.split(' ', 1)[0].split('://', 1)[1]

    if protocol == 's3':
        bucket = key.split('/', 1)[0]
        prefix = key.split('/', 1)[1].rstrip('/')
        filename = ''

        contents = []
        try:
            contents = s3_client.list_objects(Bucket=bucket, Prefix=prefix)['Contents']
        except KeyError:
            print "collect: no objects found"
            pass

        for o in contents:
            try:
                filename = o['Key'].split('/')[-1]
                s3_client.download_file(bucket, o['Key'], local_dir+'/'+filename)
            except:
                donemsg = 'FAIL:COLLECT(%s->%s\n%s)' % ('s3://'+bucket+'/'+o['Key'], local_dir+'/'+filename, traceback.format_exc())
                break
        else:
            donemsg = 'OK:COLLECT(%s->%s), get %d objects' % (msg.split(' ', 1)[0], local_dir, len(contents))

    elif protocol == 'redis':
        raise Exception('not implemented yet')

    elif protocol == 'file':
        raise Exception('not implemented yet')

    else:
        donemsg = 'FAIL(unknown protocol: %s)' % protocol

    if vals.get('cmdsock') is not None:
        vals['cmdsock'].enqueue(donemsg)
    return False


###
#  dispatch to handler functions
###
message_types = { 'set:': do_set
                , 'seti:': do_seti
                , 'get:': do_get
                , 'geti:': do_geti
                , 'dump_vals:': do_dump_vals
                , 'emit:': do_emit
                , 'collect:': do_collect
                , 'retrieve:': do_retrieve
                , 'upload:': do_upload
                , 'echo:': do_echo
                , 'quit:': do_quit
                , 'run:': do_run
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

message_responses = { 'set': 'OK:SET'
                    , 'seti': 'OK:SETI'
                    , 'get': 'OK:GET'
                    , 'geti': 'OK:GETI'
                    , 'dump_vals': 'OK:DUMP_VALS'
                    , 'retrieve': 'OK:RETRIEV'
                    , 'upload': 'OK:UPLOAD'
                    , 'echo': 'OK:ECHO'
                    , 'run': 'OK:R'
                    , 'listen': 'OK:LISTEN'
                    , 'close_listen': 'OK:CLOSE_LISTEN'
                    , 'connect': 'OK:CONNECT'
                    , 'close_connect': 'OK:CLOSE_CONNECT'
                    }


def expected_response(msg):
    cmd = msg.split(':', 1)[0]
    expected = message_responses.get(cmd, "OK")
    return expected
