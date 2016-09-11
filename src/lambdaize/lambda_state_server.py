#!/usr/bin/python

import select
import socket
import time

import libmu
import libmu.server

class ServerInfo(object):
    port_number = 13337
    num_parts = 1024
    profiling = None

    cacert = None
    srvcrt = None
    srvkey = None

    # number of seconds to keep tombstone messages before discarding
    tombstone_timeout = 600

class StateSocket(libmu.SocketNB):
    stateid = None
    partner = None

    def initialize(self):
        if not self.want_handle:
            return None

        if self.partner is not None:
            return None

        msg = self.dequeue()
        parts = msg.split(':', 3)

        try:
            if len(parts) != 4 or parts[0] != "HELLO_STATE":
                return None
            myid = "%s_%d" % (parts[1], int(parts[2]))
            self.stateid = myid
            self.partner = "%s_%d" % (parts[1], int(parts[3]))
        except:
            self.close()
            return None
        else:
            return myid


def rwsplit(sts, ret, tmbs):
    diffs = {}
    for idx in sts:
        st = sts[idx]
        if st.sock is not None:
            val = select.POLLIN

            if st.ssl_write or st.want_write:
                val = val | select.POLLOUT

            if ret.get(idx) != val:
                ret[idx] = val
                diffs[idx] = True

        else:
            ret[idx] = 0
            diffs[idx] = False

            if st.want_handle:
                plist = tmbs.setdefault(st.partner, [])
                while st.want_handle:
                    tombstone = (st.dequeue(), time.time())
                    plist.append(tombstone)

    return diffs

def handle_server_sock(lsock, state_id_map, state_fd_map):
    (ns, _) = lsock.accept()
    ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ns.setblocking(False)

    nstate = StateSocket(ns)
    nstate.do_handshake()

    temp_id = libmu.util.rand_str(32)
    state_id_map[temp_id] = nstate
    state_fd_map[nstate.fileno()] = nstate
    nstate.stateid = temp_id

    if libmu.Defs.debug:
        print "SERVER new connection, assigning tempid %s" % temp_id

def run():
    lsock = libmu.server.setup_server_listen(ServerInfo)
    lsock_fd = lsock.fileno()

    state_fd_map = {}
    rwflags = {}
    state_id_map = {}
    tombstones = {}
    npasses_out = 0
    poll_obj = select.poll()
    poll_obj.register(lsock_fd, select.POLLIN)

    def show_status():
        npstates = len([ 1 for s in state_id_map if state_id_map[s].partner is None ])
        tstates = len(state_id_map)
        fstates = len(state_fd_map)
        rwstates = len(rwflags)
        tstones = len(tombstones)
        print "SERVER status: conn_id=%d conn_fd=%d conn_flags=%d partnerless=%d tombstones=%d" % (tstates, fstates, rwstates, npstates, tstones)

        # enhanced output in debugging mode
        if libmu.Defs.debug:
            for f in state_fd_map:
                st = state_fd_map[f]
                print "%d: (%s) %s" % (f, st.stateid, str(st.sock))

    while True:
        dflags = rwsplit(state_id_map, rwflags, tombstones)

        for idx in dflags:
            if rwflags.get(idx, 0) != 0:
                poll_obj.register(state_id_map[idx], rwflags[idx])
            else:
                try:
                    poll_obj.unregister(state_id_map[idx])
                except:
                    pass

            if not dflags[idx]:
                if libmu.Defs.debug:
                    print "SERVER Deleting state %s" % idx
                assert len(state_id_map[idx].recv_queue) == 0
                assert state_id_map[idx].sock is None

                fno = state_id_map[idx].fileno()
                state_id_map[idx].close()
                rwflags[idx] = None
                state_id_map[idx] = None
                state_fd_map[fno] = None
                del rwflags[idx]
                del state_id_map[idx]
                del state_fd_map[fno]

        if lsock is None and lsock_fd is not None:
            poll_obj.unregister(lsock_fd)
            lsock_fd = None

        if npasses_out == 100:
            npasses_out = 0
            show_status()

        pfds = poll_obj.poll(1000 * 10)
        npasses_out += 1

        if len(pfds) == 0:
            show_status()

        # read all ready sockets for new messages
        for (fd, ev) in pfds:
            if (ev & select.POLLIN) != 0:
                if lsock is not None and fd == lsock_fd:
                    handle_server_sock(lsock, state_id_map, state_fd_map)

                else:
                    state_fd_map[fd].do_read()

        # handle messages from each connection
        for idx in state_id_map:
            state = state_id_map[idx]
            if state.partner is None and state.want_handle:
                newid = state.initialize()
                rwflags[newid] = rwflags[idx]
                del state_id_map[idx]
                del rwflags[idx]
                if newid is not None:
                    state_id_map[newid] = state
                    if libmu.Defs.debug:
                        print "SERVER got hello identifier %s from tempid %s" % (newid, idx)
                else:
                    if libmu.Defs.debug:
                        print "SERVER (warning) dropping client with tempid %s" % idx
                    continue

            if state_id_map.get(state.partner) is not None:
                if libmu.Defs.debug and state.want_handle:
                    print "SERVER message from %s to %s" % (state.stateid, state.partner)
                while state.want_handle:
                    state_id_map[state.partner].enqueue(state.dequeue())

        # handle tombstone messages
        to_delete = []
        for tid in tombstones:
            tstones = tombstones[tid]

            # partner is connected! send its messages
            if state_id_map.get(tid) is not None:
                to_delete.append(tid)
                for (msg, _) in tstones:
                    state_id_map[tid].enqueue(msg)

            # every once in a while, go through all the tombstones and get rid of old ones
            elif len(pfds) == 0:
                for tidx in reversed(range(0, len(tstones))):
                    now = time.time()
                    if now - tstones[tidx][1] > ServerInfo.tombstone_timeout:
                        del tstones[tidx]

                if len(tstones) == 0:
                    to_delete.append(tid)

        # need to delete afterwards because we cannot modify dictionary during iteration
        for tid in to_delete:
            del tombstones[tid]

        # send ready messages on each connection that's writable
        for (fd, ev) in pfds:
            if (ev & select.POLLOUT) != 0:
                state_fd_map[fd].do_write()

def main():
    libmu.server.options(ServerInfo)
    run()

if __name__ == '__main__':
    main()
