"""
a minimal lightweight logger (note: no GC yet)
"""

_logger_dict = {}

class Logger(object):
    def __init__(self):
        self.cached = []
    def debug(self, msg):
        self.cached.append(msg)
    def info(self, msg):
        self.cached.append(msg)
    def warning(self, msg):
        self.cached.append(msg)
    def error(self, msg):
        self.cached.append(msg)

def getLogger(logger):
    l = _logger_dict.get(logger, Logger())
    _logger_dict[logger] = l
    return l

