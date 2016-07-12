#!/usr/bin/python

import socket
import sys
import time

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
s.bind(('0.0.0.0', 13579))

while True:
    # every message is 12 bytes long
    (msg, (addr, port)) = s.recvfrom(16)
    t = time.time()
    print "%s :: %s:%d :: %f" % (msg, addr, port, t)
    print >> sys.stderr, "%s :: %s:%d :: %f" % (msg, addr, port, t)
