#!/usr/bin/python

import fcntl
import os

# very simple socket-like wrapper around raw file descriptors
class FDWrapper(object):
    def __init__(self, fd):
        self.fd = fd

    def set_blocking(self, blocking):
        if self.fd is None:
            return None

        flags = fcntl.fcntl(self.fd, fcntl.F_GETFL, 0)

        if blocking:
            flags = flags & ~os.O_NONBLOCK
        else:
            flags = flags | os.O_NONBLOCK

        fcntl.fcntl(self.fd, fcntl.F_SETFL, flags)

    def recv(self, length):
        if self.fd is None:
            return None

        return os.read(self.fd, length)

    def send(self, msg):
        if self.fd is None:
            return None

        return os.write(self.fd, msg)

    def close(self):
        if self.fd is None:
            return None

        os.close(self.fd)
        self.fd = None

    @staticmethod
    def shutdown(*_):
        pass

    def fileno(self):
        return self.fd

    @staticmethod
    def getsockname():
        return (None, None)
