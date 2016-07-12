#!/usr/bin/python

import re
import sys
import matplotlib.pyplot as plt

#                    start/finish:   id#             IP address                   port           time
log_re = re.compile("^([a-z ]{6}):([0-9]{6}) :: ((?:[0-9]{1,3}\.){3}[0-9]{1,3}):([0-9]+) :: ([0-9]+\.[0-9]+)$")

def process_log_line(line, log, basetime=None):
    mat = log_re.match(line)
    if mat is None:
        print >> sys.stderr, "WARNING: Could not process log line '%s'" % line
        return

    (tag, ser, ip, port, time) = mat.groups()
    time = float(time)
    retval = None
    if basetime is None:
        retval = time
        time = 0
    else:
        retval = basetime
        time = time - basetime

    ll = log.setdefault(ser, {})
    # check that IP and port line up
    if 'ip' in ll:
        assert(ll['ip'] == ip)
        assert(ll['port'] == port)
    else:
        # record IP and port
        ll['ip'] = ip
        ll['port'] = port

        # record it in the general log of IP addresses
        log.setdefault('ips', {}).setdefault(ip, []).append(port)

    # record the time in the appropriate bucket
    ll.setdefault(tag, []).append(time)
    ll.setdefault('data', []).append(time)

    if tag == 'finish' and 'start ' not in ll:
        ll.setdefault('start ', []).append(time - 5)
        print >> sys.stderr, "WARNING: found 'finish' without 'start '. Faking it..."

    return retval

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: %s <logfile>" % sys.argv[0]
        exit(1)

    log = {}
    with open(sys.argv[1]) as logfile:
        basetime = None
        for line in logfile:
            basetime = process_log_line(line.strip(), log, basetime)

    
    # pull 'ips' out of the log so that we can sort without a special case
    ips = log['ips']
    del log['ips']
    logSorted = sorted(log, key=lambda ent: log[ent]['start '][0])
    print >> sys.stderr, "Plotting %d lambdas" % len(logSorted)

    bottom = []
    height = []
    xpos = []
    for (idx, ent) in enumerate(logSorted):
        if 'finish' not in log[ent]:
            log[ent].setdefault('finish', []).append(log[ent]['start '][0] + 5)
            print >> sys.stderr, "WARNING: found 'start ' without 'finish'. Faking it..."

        def average(l):
            return sum(l) / len(l)

        startmean = average(log[ent]['start '])
        stopmean = average(log[ent]['finish'])
        barheight = stopmean - startmean
        xpos.append(idx+0.6)
        bottom.append(startmean)
        height.append(barheight)

    bars = plt.bar(xpos, height, 0.8, bottom, color='gray')
    plt.title('Lambda jobs')
    plt.xlabel('jobs (sorted by start time)')
    plt.ylabel('job start and stop time')

    # similar to code at
    # http://matplotlib.org/examples/api/barchart_demo.html
    #for (i, b) in enumerate(bars):
    #    height = b.get_height()
    #    plt.text(b.get_x() + b.get_width()/2, b.get_y() + b.get_height() + 0.1, logSorted[i], ha='center', va='bottom', rotation='vertical')

    plt.savefig('%s.png' % sys.argv[1], dpi=150)
