#!/usr/bin/python
import json
import os
import random
import socket
import traceback
import logging

import sys
from OpenSSL import SSL, crypto
from OpenSSL._util import lib as _ssl_lib
from multiprocessing import Process

import libmu.socket_nb
import libmu.defs
import pdb
# format the base64 portion of an SSL cert into something libssl can use
def format_pem(ctype, cert):
    fmt_cert = "-----BEGIN %s-----\n" % ctype
    while len(cert) > 0:
        fmt_cert += cert[:64] + "\n"
        cert = cert[64:]
    fmt_cert += "-----END %s-----\n" % ctype

    return fmt_cert

format_ssl_cert = lambda c: format_pem("CERTIFICATE", c)
format_ssl_key = lambda c: format_pem("RSA PRIVATE KEY", c)

def format_ssl_cert_chain(chain):
    res = ""
    for c in chain.split(' '):
        res += format_ssl_cert(c)

    return res

###
#  connect a socket, maybe SSLizing
###
def connect_socket(addr, port, cacert, srvcrt, srvkey):
    # connect to the master for orders
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.connect((addr, port))

    # if we have a cacert, this means we should use SSL for this connection
    if cacert is not None:
        s = sslize(s, cacert, srvcrt, srvkey, True)
        if not isinstance(s, SSL.Connection):
            return "ERROR could not initialize SSL connection: %s\n" % str(s)

    # wrap in non-blocking socket reader/writer class
    s.setblocking(False)
    s = libmu.socket_nb.SocketNB(s)
    s.do_handshake()

    return s

def ssl_context(cacert, srvcrt, srvkey):
    # general setup: TLSv1.2, no compression, paranoid ciphers
    sslctx = SSL.Context(SSL.TLSv1_2_METHOD)
    sslctx.set_verify_depth(9)
    sslctx.set_options(SSL.OP_NO_COMPRESSION)
    sslctx.set_mode(_ssl_lib.SSL_MODE_ENABLE_PARTIAL_WRITE | _ssl_lib.SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER)
    sslctx.set_cipher_list(libmu.defs.Defs.cipher_list)
    sslctx.set_verify(SSL.VERIFY_PEER | SSL.VERIFY_FAIL_IF_NO_PEER_CERT, lambda _, __, ___, ____, ok: ok)

    # use CA cert provided during lambda invocation
    fmt_cert = format_ssl_cert(cacert)
    x509_cert = crypto.load_certificate(crypto.FILETYPE_PEM, fmt_cert)
    sslctx.get_cert_store().add_cert(x509_cert)

    # add my certificate chain
    has_cert = False
    for cert in srvcrt.split(' '):
        x509_cert = crypto.load_certificate(crypto.FILETYPE_PEM, format_ssl_cert(cert))
        if not has_cert:
            sslctx.use_certificate(x509_cert)
            has_cert = True
        else:
            sslctx.add_extra_chain_cert(x509_cert)

    # private key
    sslctx.use_privatekey(crypto.load_privatekey(crypto.FILETYPE_PEM, format_ssl_key(srvkey)))

    # check that all's well
    sslctx.check_privatekey()

    return sslctx

###
#  SSLize a connected socket, requiring a supplied cacert
###
def sslize(sock, cacert, srvcrt, srvkey, is_connect):
    sslconn = None
    try:
        sslctx = ssl_context(cacert, srvcrt, srvkey)
        sslconn = SSL.Connection(sslctx, sock)
        if is_connect:
            sslconn.set_connect_state()
        else:
            sslconn.set_accept_state()
    except:
        return traceback.format_exc()
    else:
        return sslconn

###
#  listen on a socket, maybe SSLizing
###
def listen_socket(addr, port, cacert, srvcrt, srvkey, nlisten=1):
    ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ls.bind((addr, port))
    ls.listen(nlisten)
    logging.debug("start listening")

    if cacert is not None and srvcrt is not None and srvkey is not None:
        logging.debug("start sslizing")
        ls = sslize(ls, cacert, srvcrt, srvkey, False)
        if not isinstance(ls, SSL.Connection):
            return "ERROR could not initialize SSL connection: %s\n" % str(ls)
    ls.setblocking(False)
    return ls

###
#  accept from a listening socket and hand back a SocketNB
###
def accept_socket(lsock):
    (ns, _) = lsock.accept()
    ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ns.setblocking(False)
    ns = libmu.socket_nb.SocketNB(ns)
    ns.do_handshake()

    return ns

###
#  Random string.
###
def rand_str(slen):
    ostr = ""
    for _ in range(0, slen):
        cval = int(random.random()*60)

        if cval < 26:
            ostr += chr(cval + 65)
        elif cval < 52:
            ostr += chr(cval + 71)
        else:
            ostr += str(cval - 52)

    return ostr

###
#  random green
###
def rand_green(string):
    greens = [ -2, -10, -22, -28, -29, -34, -35, -40, -41, -46, -47, -48, -70, -71, -76, -77, -82, -83, -112, -118, -148, -154, 32 ]
    ngreens = len(greens)

    ostr = ''
    for i in range(0, len(string)):
        ostr += '\033['
        tgrn = greens[random.randint(0, ngreens-1)]
        rstblink = False
        rstinvert = False
        if random.randint(0, 3):
            ostr += '1;'
        if not random.randint(0, 39):
            ostr += '5;'
            rstblink = True
        if not rstblink and not random.randint(0, 14):
            ostr += '7;'
            rstinvert = True
        if tgrn < 0:
            ostr += '38;5;' + str(-1 * tgrn) + 'm'
        else:
            ostr += str(tgrn) + 'm'
        ostr += string[i]
        if rstblink or rstinvert:
            bstr = '25' if rstblink else ''
            istr = '27' if rstinvert else ''
            sc = ';' if rstblink and rstinvert else ''
            ostr += '\033[' + bstr + sc + istr + 'm'

    ostr += '\033[0m'
    return ostr


###
#  load cert or pkey from file
###
def read_pem(fname):
    ret = ""
    with open(fname) as f:
        started = False
        for line in f:
            if line[:11] == "-----BEGIN ":
                started = True
                continue
            elif line[:9] == "-----END ":
                break

            if started:
                ret += line.rstrip()

    return ret


class ForkedPdb(pdb.Pdb):
    """A Pdb subclass that may be used
    from a forked multiprocessing child
    Borrowed from <http://stackoverflow.com/a/23654936> for debugging.
    """
    def interaction(self, *args, **kwargs):
        _stdin = sys.stdin
        try:
            sys.stdin = open('/dev/stdin')
            pdb.Pdb.interaction(self, *args, **kwargs)
        finally:
            sys.stdin = _stdin


def mock_launch(n, func, akid, secret, event, regions):
    for i in xrange(n):
        p = Process(target=lambda_setup, args=(json.loads(event),))
        p.start()


def lambda_setup(event):
    os.chdir(os.path.dirname(os.path.realpath(__file__)) + '/../mock_lambda/')
    sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../mock_lambda/')
    event['rm_tmpdir'] = 0
    import lambda_function_template
    lambda_function_template.lambda_handler(event, None)
