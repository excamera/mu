#!/usr/bin/python

import time

from libmu.defs import Defs
import libmu.handler
from libmu.socket_nb import SocketNB
import libmu.util

class MachineState(SocketNB):
    expect = None
    extra = "(base class)"

    def __init__(self, prevState, actorNum=0):
        super(MachineState, self).__init__(prevState)

        if isinstance(prevState, MachineState):
            self.prevState = prevState
            self.actorNum = prevState.actorNum
            self.timestamps = prevState.timestamps
            self.info = prevState.info

        else:
            # first time we're being initialized
            self.prevState = None
            self.actorNum = actorNum
            self.timestamps = []
            self.info = {}

        self.messages = []
        self.timestamps.append(time.time())

    def get_timestamps(self):
        return self.timestamps

    def __repr__(self):
        return "%s: %s" % (type(self), str(self))

    def __str__(self):
        return "Actor#%d: %s" % (self.actorNum, self.str_extra())

    def str_extra(self):
        return self.extra

    def transition(self, _):
        return self

    def info_updated(self):
        pass

    def do_handle(self):
        ### handle INFO messages
        info_updated = False
        for msg in list(self.recv_queue):
        # use list(deque) so that we can modify the deque inside the iteration
            if msg[:4] == 'INFO':
                if Defs.debug:
                    print "SERVER HANDLING", msg
                info_updated = True
                self.recv_queue.remove(msg)

                vv = msg[5:].split(':', 1)
                if len(vv) != 2 or len(vv[0]) < 1:
                    raise AttributeError("improper INFO message received")

                self.info[vv[0]] = vv[1]

        if info_updated:
            self.info_updated()

        ### handle state transitions
        state = self
        state.update_flags()

        retries = []
        while state.want_handle:
            msg = state.dequeue()
            if Defs.debug:
                print "SERVER HANDLING", msg

            if msg[:4] == "FAIL":
                return ErrorState(self, msg)

            try:
                state = state.transition(msg)
            except ValueError:
                if Defs.debug:
                    print "SERVER REQUEUEING", msg
                retries.append(msg)

            if Defs.debug:
                print repr(state)
            state.update_flags()

        # put any that were skipped back in the queue
        state.recv_queue.extend(retries)
        state.update_flags()

        return state

    def do_read(self):
        try:
            super(MachineState, self).do_read()
        except Exception as e:  # pylint: disable=broad-except
            return ErrorState(self, str(e))

        if self.want_handle:
            return self.do_handle()

        return self

    def do_write(self):
        try:
            super(MachineState, self).do_write()
        except Exception as e:  # pylint: disable=broad-except
            return ErrorState(self, str(e))

        return self

    def get_expect(self):
        return self.expect

class TerminalState(MachineState):
    extra = "(terminal state)"

    def __init__(self, prevState, actorNum=0):
        super(TerminalState, self).__init__(prevState, actorNum)

class ErrorState(TerminalState):
    def __init__(self, prevState, err="", actorNum=0):
        super(ErrorState, self).__init__(prevState, actorNum)
        self.close()
        self.err = err

    def str_extra(self):
        return str(self.err)

class OnePassState(MachineState):
    command = None
    expect = None
    extra = "(one-pass state)"
    nextState = TerminalState

    def __init__(self, prevState, actorNum=0):
        super(OnePassState, self).__init__(prevState, actorNum)

        if self.expect is None:
            self.expect = libmu.util.rand_str(32)
            self.kick()

    def kick(self):
        # schedule ourselves for immediate run
        self.recv_queue.appendleft(self.expect)
        self.want_handle = True

    def transition(self, msg):
        if msg[:len(self.expect)] != self.expect:
            return ErrorState(self, msg)

        if self.command is not None:
            self.enqueue(self.command)

        self.messages.append(msg)

        return self.post_transition()

    def post_transition(self):
        return self.nextState(self)

class MultiPassState(MachineState):
    nextState = TerminalState
    extra = "(multi-pass state)"

    def __init__(self, prevState, actorNum=0):
        super(MultiPassState, self).__init__(prevState, actorNum)
        self.cmdNum = 0
        self.commands = []
        self.expects = []

    def transition(self, msg):
        if self.cmdNum >= len(self.commands) or msg[:len(self.expects[self.cmdNum])] != self.expects[self.cmdNum]:
            return ErrorState(self, msg)

        # send message and transition
        command = self.commands[self.cmdNum]
        self.cmdNum += 1
        if command is not None:
            self.enqueue(command)

        self.messages.append(msg)

        if self.cmdNum >= len(self.commands):
            return self.nextState(self)

        if self.expects[self.cmdNum] is None:
            self.expects[self.cmdNum] = libmu.util.rand_str(32)
            self.kick()

        self.timestamps.append(time.time())
        return self

    def kick(self):
        self.recv_queue.appendleft(self.expects[self.cmdNum])
        self.want_handle = True

    def str_extra(self):
        return "(%s (key #%d))" % (self.extra, self.cmdNum)

    def get_expect(self):
        return self.expects[self.cmdNum]

class CommandListState(MultiPassState):
    nextState = TerminalState
    commandlist = []
    pipelined = False

    def __init__(self, prevState, actorNum=0):
        super(CommandListState, self).__init__(prevState, actorNum)

        # explicit expect if given, otherwise set expect based on previous command
        self.expects = [ self.commandlist[0][0] if isinstance(self.commandlist[0], tuple) else "OK" ]
        self.commands = [ cmd[1] if isinstance(cmd, tuple) else cmd for cmd in self.commandlist ]
        pre_expects = [ cmd[0] if isinstance(cmd, tuple) else libmu.handler.expected_response(pc)
                        for (cmd, pc) in zip(self.commandlist[1:], self.commands[:-1]) ]

        if self.pipelined:
            # in pipelined mode, we send all commands at once, and wait for all responses afterward
            self.commands = [ cmd for cmd in self.commands if cmd is not None ]
            pre_expects = [ exp for exp in pre_expects if pre_expects is not None ]
            self.expects += [None] * (len(self.commands) - 1) + pre_expects
            self.commands += [None] * (len(pre_expects))
            assert len(self.expects) == len(self.commands), "Could not pipeline this state. Are there no commands?"

        else:
            # if we're not pipelined, then we just do command-response
            self.expects += pre_expects

        if self.expects[0] is None:
            self.expects[0] = libmu.util.rand_str(32)
            self.kick()

class IfElseState(OnePassState):
    extra = "(ifelse state)"
    consequentState = TerminalState
    alternativeState = TerminalState

    def testfn(self):
        # pylint: disable=no-self-use
        return True

    def post_transition(self):
        if self.testfn():
            return self.consequentState(self)
        else:
            return self.alternativeState(self)

class ForLoopState(OnePassState):
    loopState = TerminalState
    exitState = TerminalState

    breakKey = '_loop_break'
    iterKey = 'iter_key'
    iterInit = 0
    iterFin = 0

    def __init__(self, prevState, actorNum=0):
        super(ForLoopState, self).__init__(prevState, actorNum)

        # initialize the loop
        if self.info.get(self.breakKey) is None:
            self.info[self.iterKey] = self.iterInit - 1
            self.info[self.breakKey] = False

    def post_transition(self):
        if self.info[self.iterKey] >= (self.iterFin - 1) or self.info[self.breakKey]:
            del self.info[self.breakKey]
            return self.exitState(self)
        else:
            self.info[self.iterKey] += 1
            return self.loopState(self)

    def str_extra(self):
        return "(for loop, start:%d iter:%d end:%d)" % (self.iterInit, self.info[self.iterKey], self.iterFin)

class SuperpositionState(MachineState):
    command = None
    state_constructors = [TerminalState]
    nextState = TerminalState

    def __init__(self, prevState, actorNum=0):
        super(SuperpositionState, self).__init__(prevState, actorNum)
        states = []
        for s in self.state_constructors:
            states.append(s(prevState, actorNum))
        self.states = states

    def str_extra(self):
        retstr = "(superposition:"
        for s in self.states:
            retstr += ' ' + s.str_extra()
        retstr += ')'
        return retstr

    def get_expect(self):
        exps = []
        for s in self.states:
            exp = s.get_expect()
            if isinstance(exp, list):
                exps.extend(exp)
            else:
                exps.append(exp)

        return exps

    def info_updated(self):
        for s in self.states:
            s.info_updated()

        self.update_flags()

    def _match_expect(self, msg, expect):
        if isinstance(expect, list):
            return any([ self._match_expect(msg, exp) for exp in expect ])
        elif msg[:len(expect)] == expect:
            return True
        else:
            return False

    def transition(self, msg):
        handled = False
        for i in range(0, len(self.states)):
            state = self.states[i]
            if isinstance(state, TerminalState):
                continue

            if self._match_expect(msg, state.get_expect()):
                handled = True
                state = state.transition(msg)
                self.states[i] = state
                break

        if not handled:
            raise ValueError()

        self.update_flags()

        if all([ isinstance(state, TerminalState) for state in self.states]):
            return self.nextState(self)
        else:
            return self

class InfoWatcherState(OnePassState):
    extra = "(infowatcher)"

    def __init__(self, prevState, actorNum=0):
        # need to set random expect string first to prevent OnePassState from kicking us
        if self.expect is None:
            self.expect = libmu.util.rand_str(32)

        super(InfoWatcherState, self).__init__(prevState, actorNum)

    def info_updated(self):
        self.kick()
