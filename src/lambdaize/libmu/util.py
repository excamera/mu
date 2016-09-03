#!/usr/bin/python

import random
import socket
import traceback

from OpenSSL import SSL, crypto

import libmu.socket_nb
import libmu.defs

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
def connect_socket(addr, port, cacert):
    # connect to the master for orders
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.connect((addr, port))

    # if we have a cacert, this means we should use SSL for this connection
    if cacert is not None:
        s = ssl_connect(s, cacert)
        if not isinstance(s, SSL.Connection):
            return "ERROR could not initialize SSL connection: %s\n" % str(s)

    # wrap in non-blocking socket reader/writer class
    s.setblocking(False)
    s = libmu.socket_nb.SocketNB(s)
    s.do_handshake()

    return s

###
#  SSLize a connected socket, requiring a supplied cacert
###
def ssl_connect(sock, cert):
    sslconn = None
    try:
        # general setup: TLSv1.2, no compression, paranoid ciphers
        sslctx = SSL.Context(SSL.TLSv1_2_METHOD)
        sslctx.set_options(SSL.OP_NO_COMPRESSION)
        sslctx.set_cipher_list(libmu.defs.Defs.cipher_list)

        # require verification
        # only thing that matters is that the cert chain checks out
        sslctx.set_verify(SSL.VERIFY_PEER, lambda _, __, ___, ____, ok: ok)

        # use CA cert provided during lambda invocation
        fmt_cert = format_ssl_cert(cert)
        x509_cert = crypto.load_certificate(crypto.FILETYPE_PEM, fmt_cert)
        sslctx.get_cert_store().add_cert(x509_cert)

        # turn the provided socket into an SSL socket
        sslconn = SSL.Connection(sslctx, sock)
        sslconn.set_connect_state()
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

    if cacert is not None and srvcrt is not None and srvkey is not None:
        ls = ssl_listen(ls, srvcrt, srvkey)
        if not isinstance(ls, SSL.Connection):
            return "ERROR could not initialize SSL connection: %s\n" % str(ls)

    ls.setblocking(False)

    return ls

###
#  SSLize a listening socket using a supplied certificate chain and key
###
def ssl_listen(sock, chain, key):
    sslconn = None
    try:
        sslctx = SSL.Context(SSL.TLSv1_2_METHOD)
        sslctx.set_options(SSL.OP_NO_COMPRESSION)
        sslctx.set_cipher_list(libmu.defs.Defs.cipher_list)
        sslctx.set_verify(SSL.VERIFY_NONE, lambda *_: True)

        # certificate chain
        has_cert = False
        for cert in chain.split(' '):
            x509_cert = crypto.load_certificate(crypto.FILETYPE_PEM, format_ssl_cert(cert))
            if not has_cert:
                sslctx.use_certificate(x509_cert)
                has_cert = True
            else:
                sslctx.add_extra_chain_cert(x509_cert)

        # private key
        sslctx.use_privatekey(crypto.load_privatekey(crypto.FILETYPE_PEM, format_ssl_key(key)))

        # check that all's well
        sslctx.check_privatekey()

        sslconn = SSL.Connection(sslctx, sock)
        sslconn.set_accept_state()
    except:
        return traceback.format_exc()
    else:
        return sslconn

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
        cval = random.randint(0, 61)

        if cval < 26:
            ostr += chr(cval + 65)
        elif cval < 52:
            ostr += chr(cval + 71)
        else:
            ostr += str(cval - 52)

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
