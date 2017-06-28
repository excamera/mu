#!/usr/bin/python
import Queue
import json
import select
import socket
import threading
import logging

import pylaunch
from collections import defaultdict

import libmu.defs
import libmu.machine_state
import libmu.util


class Task(object):

    def __init__(self, lambda_func, init_state, incoming_events, outgoing_queues, event, regions=None):
        self.lambda_func = lambda_func
        self.constructor = init_state
        self.incoming_events = incoming_events
        self.outgoing_queues = outgoing_queues
        self.event = event
        self.regions = ["us-east-1"] if regions is None else regions
        self.current_state = None
        self.rwflag = 0

    def __str__(self):
        return "task in waiting" if self.current_state is None else self.current_state.__class__.__name__

    def start(self, ns):
        self.current_state = self.constructor(ns, self.incoming_events, self.outgoing_queues)
        self.current_state.do_handshake()

    def do_handle(self):
        self.current_state = self.current_state.do_handle()

    def do_read(self):
        self.current_state = self.current_state.do_read()

    def do_write(self):
        self.current_state = self.current_state.do_write()


class Tracker(object):

    config = defaultdict(lambda: None)

    with open('mu_conf.json', 'r') as f:
        c = json.load(f)
    for k, v in c.iteritems():
        config[k] = v

    started = False
    started_lock = threading.Lock()
    should_stop = False

    submitted_queue = Queue.Queue()
    waiting_queue = Queue.Queue()

    with open(config['aws_access_key_id_file'], 'r') as f:
        akid = f.read().strip()
    with open(config['aws_secret_access_key_file'], 'r') as f:
        secret = f.read().strip()

    cacert = libmu.util.read_pem(config['cacert_file']) if config['cacert_file'] is not None else None
    srvcrt = libmu.util.read_pem(config['srvcrt_file']) if config['srvcrt_file'] is not None else None
    srvkey = libmu.util.read_pem(config['srvkey_file']) if config['srvkey_file'] is not None else None

    @classmethod
    def _handle_server_sock(cls, ls, tasks, fd_task_map):
        (ns, _) = ls.accept()  # may be changed to accept more than one conn
        ns.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        ns.setblocking(False)

        try:
            new_task = Tracker.waiting_queue.get(block=False)  # assume all tasks are the same
        except Queue.Empty as e:
            logging.warning("get response from lambda, but no one's waiting?")
            return

        new_task.start(ns)

        tasks.append(new_task)
        fd_task_map[new_task.current_state.fileno()] = new_task

    @classmethod
    def _main_loop(cls):
        lsock = libmu.util.listen_socket('0.0.0.0', cls.config['port_number'], cls.cacert, cls.srvcrt,
                                         cls.srvkey, cls.config['backlog'])
        lsock_fd = lsock.fileno()

        tasks = []
        fd_task_map = {}
        poll_obj = select.poll()
        poll_obj.register(lsock_fd, select.POLLIN)
        npasses_out = 0

        while True:
            if cls.should_stop:
                if lsock is not None:
                    try:
                        lsock.shutdown(0)
                        lsock.close()
                    except:
                        logging.warning("failure shutting down the lsock")
                        pass
                    lsock = None

            dflags = []
            for (tsk, idx) in zip(tasks, range(0, len(tasks))):
                st = tsk.current_state
                val = 0
                if st.sock is not None:
                    if not isinstance(st, libmu.machine_state.TerminalState):  # always listening
                        val = val | select.POLLIN

                    if st.ssl_write or st.want_write:
                        val = val | select.POLLOUT

                    if val != tsk.rwflag:
                        tsk.rwflag = val
                        dflags.append(idx)

                else:
                    tsk.rwflag = 0
                    dflags.append(idx)
                    if not isinstance(st, libmu.machine_state.TerminalState):
                        tsk.current_state = libmu.machine_state.ErrorState(tsk.current_state, "sock closed in %s" % str(tsk))
                        logging.warning("socket closed abnormally: %s" % str(tsk))

            for idx in dflags:
                if tasks[idx].rwflag != 0:
                    poll_obj.register(tasks[idx].current_state, tasks[idx].rwflag)
                else:
                    try:
                        poll_obj.unregister(tasks[idx].current_state)
                    except Exception as e:
                        logging.error("unregister: "+str(e.message))
                        pass

            pfds = poll_obj.poll(2000)
            npasses_out += 1

            if len(pfds) == 0:
                continue

            # look for readable FDs
            for (fd, ev) in pfds:
                if (ev & select.POLLIN) != 0:
                    if lsock is not None and fd == lsock_fd:
                        logging.debug("listening sock got data in")
                        cls._handle_server_sock(lsock, tasks, fd_task_map)

                    else:
                        logging.debug("conn sock got data in")
                        task = fd_task_map[fd]
                        task.do_read()

            for (fd, ev) in pfds:
                if (ev & select.POLLOUT) != 0:
                    logging.debug("conn sock got data out")
                    task = fd_task_map[fd]
                    task.do_write()

            for tsk in [t for t in tasks if isinstance(t.current_state, libmu.machine_state.TerminalState)]:
                try:
                    poll_obj.unregister(tsk.current_state)
                except Exception as e:
                    logging.warning(e.message)
                try:
                    tsk.current_state.close()
                except Exception as e:
                    logging.warning(e.message)
                del fd_task_map[tsk.current_state.fileno()]

            tasks = [t for t in tasks if not isinstance(t.current_state, libmu.machine_state.TerminalState)]
            for tsk in tasks:
                if tsk.current_state.want_handle:
                    tsk.do_handle()

    @classmethod
    def _invocation_loop(cls):
        testsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        testsock.connect(("lambda.us-east-1.amazonaws.com", 443))  # assume that's correct
        addr = testsock.getsockname()[0]
        testsock.close()

        while not cls.should_stop:
            pending = []
            pending.append(cls.submitted_queue.get(block=True))
            while True:
                try:
                    pending.append(cls.submitted_queue.get(block=True, timeout=0.01))  # 10ms without a submission, we consider it a "batch"
                except Queue.Empty:
                    break
            for t in pending:
                cls.waiting_queue.put(t)
            pending[0].event['addr'] = addr
            logging.debug("akid: "+cls.akid)
            logging.debug("secret: "+cls.secret)
            pylaunch.launchpar(len(pending), pending[0].lambda_func, cls.akid, cls.secret,
                               json.dumps(pending[0].event), pending[0].regions)  # currently assume all the tasks use same function/region

    @classmethod
    def _start(cls):
        with cls.started_lock:
            if cls.started:
                return
            mt = threading.Thread(target=cls._main_loop)
            mt.setDaemon(True)
            mt.start()
            it = threading.Thread(target=cls._invocation_loop)
            it.setDaemon(True)
            it.start()
            cls.started = True

    @classmethod
    def stop(cls):
        cls.should_stop = True

    @classmethod
    def submit(cls, task):
        if not cls.started:
            cls._start()
        cls.submitted_queue.put(task)

    @classmethod
    def kill(cls, task):
        pass
