#!/usr/bin/python

import sys
import os
sys.path.insert(1, os.path.abspath(os.path.join(sys.path[0], os.pardir)))
# insert parent directory in search path, since test/ lives alongside libmu

import test.run as run
import test.states as states
import test.encsrv as encsrv

run.run_tests()
states.run_tests()
encsrv.run_tests()
