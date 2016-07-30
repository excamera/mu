#!/usr/bin/python

import collections
import os
import select
import socket
import sys
import time

from OpenSSL import SSL

NUM_PARTS = 1
USE_SSL = True

class StateMachineState(object):
    # pylint: disable=too-many-instance-attributes,no-self-use
    def __init__(self, prevState, sock, actorNum):
        self.timestamp = time.time()
        self.prevState = prevState

        if prevState is not None:
            self.actorNum = prevState.actorNum
            self.sock = prevState.sock
            self.want_write = prevState.want_write
            self.ssl_write = prevState.ssl_write
            self.expectlen = prevState.expectlen
            self.recv_queue = prevState.recv_queue
            self.recv_buf = prevState.recv_buf
            self.send_buf = prevState.send_buf
            self.handshaking = prevState.handshaking
        else:
            self.actorNum = actorNum
            self.sock = sock
            self.want_write = False
            self.ssl_write = None
            self.expectlen = None
            self.recv_queue = collections.deque()
            self.recv_buf = ""
            self.send_buf = None
            self.handshaking = False

    def get_timestamps(self):
        obj = self
        tstamps = []
        while obj is not None:
            tstamps.append(obj.timestamp)
            obj = obj.prevState
        return tstamps

    def __repr__(self):
        return "%s: %s" % (type(self), str(self))

    def __str__(self):
        return "Actor#%d: %s" % (self.actorNum, self.str_extra())

    def str_extra(self):
        return "(base class)"

    def transition(self, _):
        return self

    def close_socket(self):
        # forcibly close the socket (ignore errors)
        try:
            self.sock.shutdown()
            self.sock.close()
        except:
            pass
        del self.sock

    def do_read(self):
        try:
            # 16384 is max TLS record size
            self.recv_buf += self.sock.recv(16384)
        except SSL.WantReadError:
            return self
        except SSL.WantWriteError:
            self.ssl_write = True
            return self
        else:
            self.ssl_write = None

        while True:
            # we are still trying to figure out how long this packet is
            if self.expectlen is None:
                if len(self.recv_buf) >= 5:
                    # expect 4 digits followed by space
                    # this would also work with 5 digits
                    try:
                        self.expectlen = int(self.recv_buf[0:5])
                    except ValueError as e:
                        # erroneous message from other side
                        return ErrorState(self, self.sock, self.actorNum, e)
                    self.recv_buf = self.recv_buf[5:]
                else:
                    break

            # we know how much we want, now we just have to get it all
            else:
                if len(self.recv_buf) >= self.expectlen:
                    self.recv_queue.append(self.recv_buf[:self.expectlen])
                    self.recv_buf = self.recv_buf[self.expectlen:]
                    self.expectlen = None
                else:
                    break

        self.want_write = len(self.recv_queue) > 0 or self.send_buf is not None
        return self

    def do_write(self):
        if self.ssl_write is not None:
            self.send_raw(self.send_buf if self.send_buf is not None else '')
            return self

        if self.send_buf is not None:
            self.send_raw(self.send_buf)
            retval = self
        else:
            msg = self.recv_queue.popleft()
            retval = self.transition(msg)

        retval.want_write = len(retval.recv_queue) > 0 or retval.send_buf is not None
        return retval

    def do_handshake(self):
        self.want_write = False
        try:
            self.sock.do_handshake()
        except SSL.WantWriteError:
            self.want_write = True
            self.handshaking = True
        except SSL.WantReadError:
            self.handshaking = True
        else:
            self.handshaking = False

    def send_command(self, msg):
        self.send_raw("%04d %s" % (len(msg), msg))

    def send_raw(self, msg):
        try:
            slen = self.sock.send(msg)
        except SSL.WantWriteError:
            self.send_buf = msg
            return
        except SSL.WantReadError:
            self.send_buf = msg
            self.ssl_write = False
            return
        else:
            self.ssl_write = None

        if slen < len(msg):
            self.send_buf = msg[slen:]
        else:
            self.send_buf = None

    def fileno(self):
        return self.sock.fileno()

class TerminalState(StateMachineState):
    def __init__(self, prevState, sock, actorNum):
        super(TerminalState, self).__init__(prevState, sock, actorNum)
        self.close_socket()

# a terminal error state
class ErrorState(TerminalState):
    def __init__(self, prevState, sock, actorNum, err):
        super(ErrorState, self).__init__(prevState, sock, actorNum)
        self.err = err

    def str_extra(self):
        return str(self.err)

# a terminal non-error state
class FinalState(TerminalState):
    def str_extra(self):
        return "(finished)"


# initial state: set up the remote end
class SetKeysState(StateMachineState):
    keysToSend = [ "set:inkey:{0}/{0}{1}.y4m"
                 , "set:targfile:/tmp/{0}{1}.y4m"
                 , "set:cmdinfile:/tmp/{0}{1}.y4m"
                 , "set:cmdoutfile:/tmp/{0}{1}.ivf"
                 , "set:fromfile:/tmp/{0}{1}.ivf"
                 , "set:outkey:{0}/out/{0}{1}.ivf"
                 ]

    def __init__(self, prevState, sock, actorNum, vidName="bbb"):
        super(SetKeysState, self).__init__(prevState, sock, actorNum)
        self.vidName = vidName
        self.keyNum = 0

        # generate keys for this actor
        keysToSend = list(self.__class__.keysToSend)
        for i in range(0, len(keysToSend)):
            keysToSend[i] = keysToSend[i].format(self.vidName, "%06d" % self.actorNum)
        self.keysToSend = keysToSend

    def transition(self, msg):
        if self.keyNum >= len(self.keysToSend) or msg[:2] != 'OK':
            return ErrorState(self, self.sock, self.actorNum, msg)

        # send message and transition
        self.send_command(self.keysToSend[self.keyNum])
        self.keyNum += 1

        if self.keyNum >= len(self.keysToSend):
            return DownloadState(self, self.sock, self.actorNum)
        else:
            return self

    def str_extra(self):
        return "(waiting to send key #%d)" % self.keyNum


class OnePassState(StateMachineState):
    command = None
    extra = None
    nextState = FinalState
    expectMessage = "OK"

    def transition(self, msg):
        if msg[:len(self.expectMessage)] != self.expectMessage:
            return ErrorState(self, self.sock, self.actorNum, msg)

        if self.command is not None:
            self.send_command(self.command)

        return self.nextState(self, self.sock, self.actorNum)

    def str_extra(self):
        return self.extra

class QuitWaitState(OnePassState):
    expectMessage = "BYE"
    extra = "(waiting for quit ack)"

class QuitState(OnePassState):
    command = "quit:"
    extra = "(waiting to quit)"
    nextState = QuitWaitState

class UploadState(OnePassState):
    command = "upload:"
    extra = "(waiting to begin upload)"
    nextState = QuitState

class ConvertState(OnePassState):
    command = "run:"
    extra = "(waiting to begin conversion)"
    nextState = UploadState

class DownloadState(OnePassState):
    command = "retrieve:"
    extra = "(waiting to begin download)"
    nextState = ConvertState


#### begin script ####

if len(sys.argv) > 1:
    NUM_PARTS = int(sys.argv[1])

# bro, you listening to this?
global lsock
lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
lsock.bind(('0.0.0.0', 13579))
lsock.listen(NUM_PARTS + 10) # lol like the kernel listens to me

if USE_SSL:
    # set SSL context
    sslctx = SSL.Context(SSL.TLSv1_2_METHOD)
    sslctx.set_options(SSL.OP_NO_COMPRESSION)
    sslctx.set_cipher_list("ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-SHA256:ECDHE-RSA-RC4-SHA:ECDHE-RSA-AES256-SHA:HIGH:!aNULL:!eNULL:!EXP:!LOW:!MEDIUM:!MD5")
    sslctx.set_verify(SSL.VERIFY_NONE, lambda *_: True)
    
    # set up server key
    sslctx.use_certificate_chain_file(os.environ.get('CERTIFICATE_CHAIN', 'server_chain.pem'))
    sslctx.use_privatekey_file(os.environ.get('PRIVATE_KEY', 'server_key.pem'))
    sslctx.check_privatekey()
    
    # set up server SSL connection
    lsock = SSL.Connection(sslctx, lsock)
    lsock.set_accept_state()

states = []
peers = []
def handle_server_sock():
    global lsock
    (ns, ps) = lsock.accept()
    ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ns.setblocking(False)
    nstate = SetKeysState(None, ns, len(states))

    if USE_SSL:
        nstate.do_handshake()

    states.append(nstate)
    peers.append(ps)

    if len(states) == NUM_PARTS:
        # no need to listen any longer, we have all our connections
        try:
            lsock.shutdown()
            lsock.close()
        except:
            pass

        lsock = None

while True:
    # if everyone is in a terminal state, we're done
    if all([isinstance(s, TerminalState) for s in states]) and lsock is None:
        break

    def rwsplit(sts):
        rs = []
        ws = []
        for st in sts:
            if isinstance(st, TerminalState):
                continue

            if st.ssl_write is not None:
                flag = st.ssl_write
            else:
                flag = st.want_write

            if flag:
                ws.append(st)
            else:
                rs.append(st)
        return (rs, ws)

    (readSocks, writeSocks) = rwsplit(states)

    if lsock is not None:
        readSocks += [lsock]

    (rfds, wfds, _) = select.select(readSocks, writeSocks, [], 30)

    if len(rfds) == 0 and len(wfds) == 0:
        print "TIMEOUT!!!"
        break

    for r in rfds:
        if r is lsock:
            handle_server_sock()

        else:
            rnext = r.do_read()
            states[rnext.actorNum] = rnext

    for w in wfds:
        wnext = w.do_write()
        states[wnext.actorNum] = wnext


for st in states:
    print str(list(reversed(st.get_timestamps())))
