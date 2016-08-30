#!/usr/bin/python

import select
import sys
import os
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))
# insert parent directory in search path, since test/ lives alongside libmu

from libmu import CommandListState, OnePassState, TerminalState, ErrorState, SuperpositionState, ForLoopState, InfoWatcherState, Defs

import test.util as tutil

# NOTE
# This test is much more complicated than it needs to be!
# The idea is to test all of the state transition
# machinery.
#
# LoopSetBState -> ImmediateSetBState -> BlockSetState is a "for loop" that runs 3 times
#
# TestSuperpositionState runs the above for-loop at the same time as UploadDownloadState
#
# InfoWatchTestState runs an InfoWatcher and, in parallel, runs a CommandList that kicks
# the InfoWatcher. There's some tricky deadlock stuff that could happen here, so this
# test makes sure that the infrastructure is in place to prevent it.
#
# With Superposition states, you need to make sure that the expect values you set are
# specific enough to keep the "wrong" superposition state from getting kicked!
#

class FinalState(TerminalState):
    extra = "(finished)"

class FinishState(CommandListState):
    nextState = FinalState
    commandlist = [ ("OK:RETRIEVE", "run:diff ##TMPDIR##/test.txt ##TMPDIR##/test2.txt")
                  , ("OK:RETVAL(0)", "run:rm ##TMPDIR##/test.txt ##TMPDIR##/test2.txt")
                  , "quit:"
                  ]

class IWKicker(CommandListState):
    nextState = TerminalState
    commandlist = [ ("OK:UPLOAD", "geti:nonblock")
                  , ("OK:GETI", None)
                  ]

class RetrieveIWState(InfoWatcherState):
    nextState = TerminalState
    command = "retrieve:"
    extra = "(waiting to kick off retrieve)"

class InfoWatchTestState(SuperpositionState):
    nextState = FinishState
    state_constructors = [RetrieveIWState, IWKicker]

class UploadDownloadState(CommandListState):
    nextState = TerminalState
    commandlist = [ ("OK:RUNNING", None)
                  , ("OK:RETVAL(0)", "upload:")
                  ]

class BlockSetState(OnePassState):
    nextState = None
    command = None
    expect = "OK:SETI"
    extra = "(setting blocking-run)"

class ImmediateSetBState(OnePassState):
    nextState = BlockSetState
    command = "seti:nonblock:0"
    extra = "(setting blocking-run)"

class LoopSetBState(ForLoopState):
    loopState = ImmediateSetBState
    iterFin = 3

# need to do this here so we don't have use-before-def
BlockSetState.nextState = LoopSetBState

class TestSuperpositionState(SuperpositionState):
    nextState = InfoWatchTestState
    state_constructors = [UploadDownloadState, LoopSetBState]

class StartState(CommandListState):
    nextState = TestSuperpositionState
    commandlist = [ ("OK:HELLO", "set:inkey:testkey")
                  , "set:targfile:##TMPDIR##/test2.txt"
                  , "set:cmdinfile:curl http://www.google.com | md5sum > ##TMPDIR##/test.txt"
                  , "set:fromfile:##TMPDIR##/test.txt"
                  , "set:outkey:testkey"
                  , "seti:nonblock:1"
                  , "run:"
                  ]

def test_server(sock, *_):
    state = StartState(sock)
    print repr(state)

    while True:
        if isinstance(state, ErrorState):
            raise Exception("ERROR: %s" % repr(state))

        if isinstance(state, TerminalState) and not state.want_write:
            state.close()
            break

        wstate = [state] if state.want_write or state.ssl_write else []
        (rfds, wfds, _) = select.select([state], wstate, [], Defs.timeout)

        if len(rfds) == 0 and len(wfds) == 0:
            raise Exception("timeout in select")

        if len(rfds) > 0:
            state = state.do_read()
            print repr(state)

        if len(wfds) > 0:
            state = state.do_write()

        # in case we got multiple responses in one go
        # NOTE *never* loop here! Because of the reordering machinery for
        # superposition states, you can end up in an infinite loop!!!
        if state.want_handle:
            state = state.do_handle()
            print repr(state)

def run_tests():
    cmdstring = """ ##INFILE## """
    Defs.debug = True
    tutil.run_one_test(test_server, cmdstring, True, True)

if __name__ == "__main__":
    run_tests()
