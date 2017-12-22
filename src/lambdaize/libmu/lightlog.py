"""
a minimal lightweight logger (note: need manual handling)
"""
import time
import joblog_pb2

_logger_dict = {}

class Logger(object):
    def __init__(self):
        self.cached = []

    def debug(self, **kwargs):
        if 'ts' not in kwargs:
            kwargs['ts'] = time.time() # at least we know the time ...
        self.cached.append(kwargs)

    info = warning = error = debug

    def serialize(self):
        log = joblog_pb2.JobLog()
        for l in self.cached:
            r = log.record.add()
            for k,v in l.iteritems():
                setattr(r, k, v)
        return log.SerializeToString()


def getLogger(logger):
    l = _logger_dict.get(logger, Logger())
    _logger_dict[logger] = l
    return l

