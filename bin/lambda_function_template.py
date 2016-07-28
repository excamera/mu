#!/usr/bin/python

import socket
import subprocess
import traceback

import boto3
from OpenSSL import SSL, crypto

s3_client = boto3.client('s3')

###
#  set or get a value
###
def do_set(msg, vals):
    res = msg.split(':', 1)
    if len(res) == 2:
        (var, val) = res
        if len(var) == 0:
            send_response(vals['cmdsock'], 'FAIL(no variable name specified)')
        else:
            vals[var] = val
            send_response(vals['cmdsock'], 'OK')
    elif res[0] in vals:
        send_response(vals['cmdsock'], 'OK:%s' % str(vals[res[0]]))
    else:
        send_response(vals['cmdsock'], 'FAIL(no such variable)')
    return False


###
#  dump entire vals dict
###
def do_dump_vals(_, vals):
    send_response(vals['cmdsock'], 'OK:%s' % str(vals))
    return False


###
#  tell the client to retrieve a segment from S3
###
def do_retrieve(_, vals):
    if 'inkey' not in vals or 'targfile' not in vals:
        send_response(vals['cmdsock'], 'FAIL(inkey or targfile not set)')

    else:
        infile = vals['inkey']
        outfile = vals['targfile']

        try:
            s3_client.download_file(vals['bucket'], infile, outfile)
        except:
            send_response(vals['cmdsock'], 'FAIL(retrieving from s3:\n%s)' % traceback.format_exc())
        else:
            send_response(vals['cmdsock'], 'OK')

    return False


###
#
###
def do_upload(_, vals):
    if 'outkey' not in vals or 'fromfile' not in vals:
        send_response(vals['cmdsock'], 'FAIL(outkey or fromfile not set)')

    else:
        outfile = vals['outkey']
        infile = vals['fromfile']

        try:
            s3_client.upload_file(infile, vals['bucket'], outfile)
        except:
            send_response(vals['cmdsock'], 'FAIL(uploading to s3:\n%s)' % traceback.format_exc())
        else:
            send_response(vals['cmdsock'], 'OK')

    return False


###
#  echo msg back to the server
###
def do_echo(msg, vals):
    send_response(vals['cmdsock'], 'OK:%s' % msg)
    return False


###
#  we've been told to quit
###
def do_quit(_, vals):
    send_response(vals['cmdsock'], 'BYE')
    return True # bye!


###
#  run the command
###
def do_run(_, vals):
    cmdstring = executable

    def vals_lookup(name, aslist = False):
        out = vals.get('cmd%s' % name, None)
        if out is None:
            out = vals['event'].get(name, None)

        if out is not None and aslist and not isinstance(out, list):
            out = [out]

        return out

    # add environment variables
    usevars = vals_lookup('vars', True)
    if usevars is not None:
        for v in usevars:
            cmdstring = v + ' ' + cmdstring

    # add arguments
    useargs = vals_lookup('args', True)
    if useargs is not None:
        for a in useargs:
            cmdstring = cmdstring + ' ' + a

    # ##INFILE## and ##OUTFILE## string replacement
    useinfile = vals_lookup('infile', False)
    if useinfile is not None:
        cmdstring = cmdstring.replace('##INFILE##', useinfile)
    useoutfile = vals_lookup('outfile', False)
    if useoutfile is not None:
        cmdstring = cmdstring.replace('##OUTFILE##', useoutfile)

    # run command
    output = subprocess.check_output([cmdstring], shell=True, stderr=subprocess.STDOUT)

    # we only have a socket in command mode
    cmdsock = vals.get('cmdsock', None)
    if cmdsock is not None:
        send_response(cmdsock, 'OK:OUTPUT(%s)' % output)
        return False
    else:
        print output
        return output


###
#  response formatting
###
def send_response(sock, msg):
    msg = "%04d %s" % (len(msg), msg)
    sock.send(msg)


###
#  listen for peer lambda
###
def do_listen(_, vals):
    # only listen if the current socket is nonexistent
    # XXX this should use SSL XXX
    if vals.get('lsnsock', None) is None:
        ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ls.bind(('0.0.0.0', 0))
        ls.listen(1)
        vals['lsnsock'] = ls

    # record information and send back to master
    (_, vals['lsnport']) = vals['lsnsock'].getsockname()
    send_response(vals['cmdsock'], 'OK:LISTEN(%d)' % vals['lsnport'])
    return False


###
#  connect to peer lambda
###
def do_connect(msg, vals):
    res = msg.split(':', 1)
    if len(res) != 2:
        send_response(vals['cmdsock'], 'FAIL(could not parse connect msg)')
    else:
        (host, port) = res
        try:
            port = int(port)
        except:
            send_response(vals['cmdsock'], 'FAIL(could not parse port number)')
        else:
            cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cs.connect((host, port))
            vals['consock'] = cs
            send_response(vals['cmdsock'], 'OK')
    return False


###
#  dispatch to handler functions
###
message_types = { 'set:': do_set
                , 'dump_vals:': do_dump_vals
                , 'retrieve:': do_retrieve
                , 'upload:': do_upload
                , 'echo:': do_echo
                , 'quit:': do_quit
                , 'run:': do_run
                , 'listen:': do_listen
                , 'connect:': do_connect
                }
def handle_message(msg, vals):
    for mtype in message_types:
        if msg[:len(mtype)] == mtype:
            return message_types[mtype](msg[len(mtype):], vals)

    # if we got here, we don't recognize the command
    send_response(vals['cmdsock'], 'FAIL(no such command)')
    return False


###
#  SSLize a connected socket, requiring a supplied cacert
###
def ssl_connect(sock, cert):
    sslconn = None
    try:
        # general setup: TLSv1.2, no compression, paranoid ciphers
        sslctx = SSL.Context(SSL.TLSv1_2_METHOD)
        sslctx.set_options(SSL.OP_NO_COMPRESSION)
        sslctx.set_cipher_list("ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-SHA256:ECDHE-RSA-RC4-SHA:ECDHE-RSA-AES256-SHA:HIGH:!aNULL:!eNULL:!EXP:!LOW:!MEDIUM:!MD5")

        # require verification
        # only thing that matters is that the cert chain checks out
        sslctx.set_verify(SSL.VERIFY_PEER, lambda _, __, ___, ____, ok: ok)

        # use CA cert provided during lambda invocation
        fmt_cert = "-----BEGIN CERTIFICATE-----\n"
        while len(cert) > 0:
            fmt_cert += cert[:64] + "\n"
            cert = cert[64:]
        fmt_cert += "-----END CERTIFICATE-----\n"
        x509_cert = crypto.load_certificate(crypto.FILETYPE_PEM, fmt_cert)
        sslctx.get_cert_store().add_cert(x509_cert)

        # turn the provided socket into an SSL socket
        sslconn = SSL.Connection(sslctx, sock)
        sslconn.set_connect_state()
        sslconn.do_handshake()
    except:
        return traceback.format_exc()
    else:
        return sslconn


###
#  lambda enters here
###
def lambda_handler(event, _):
    # get config info from event
    port = int(event.get('port', 13579))
    mode = int(event.get('mode', 0))
    addr = event.get('addr', '127.0.0.1')
    bucket = event.get('bucket', 'excamera-us-east-1')
    region = event.get('region', 'us-east-1')
    cacert = event.get('cacert', None)

    # default: just run the command and exit
    if mode == 0:
        return do_run('', {'event': event})

    # connect to the master for orders
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.connect((addr, port))

    # if we have a cacert, this means we should use SSL for this connection
    if cacert is not None:
        s = ssl_connect(s, event['cacert'])
        if not isinstance(s, SSL.Connection):
            return "ERROR could not initialize SSL connection: %s\n" % str(s)

    vals = { 'cmdsock': s
           , 'bucket': bucket
           , 'region': region
           , 'event': event
           }

    # in mode 2, we open a listening socket and report the port number to the cmdsock
    if mode == 2:
        do_listen('', vals)
    else:
        send_response(vals['cmdsock'], 'OK')

    message = ""
    expectlen = None
    while True:
        if expectlen is not None and len(message) >= expectlen:
            # we've got enough of a message to act
            actmessage = message[:expectlen]
            message = message[expectlen:]
            expectlen = None

            if handle_message(actmessage, vals):
                break
        else:
            message += s.recv(1024)

        if expectlen is None and len(message) >= 5:
            # expected length (4 digits) followed by space
            try:
                expectlen = int(message[0:5])
            except:
                send_response(s, 'FAIL(could not interpret expectlen)')
                break
            else:
                message = message[5:]

    s.shutdown()
    s.close()

executable = ''
