#!/usr/bin/python

import ast
import re
import sys

import matplotlib.patches as mpat
import matplotlib.pyplot as plt

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: %s <logfile>" % sys.argv[0]
        exit(1)

    log = []
    with open(sys.argv[1]) as logfile:
        for line in logfile:
            log.append(ast.literal_eval(line))

    basetime = log[0][0]
    log = [ [ lent - basetime for lent in logline ] for logline in log ]

    bottom = []
    height = []
    xpos = []
    colors = []
    barcolors = ['blue', 'green', 'cyan', 'magenta', 'black', 'yellow']
    barregions = ['InitState', 'SetKeysState', 'DownloadState', 'ConvertState', 'UploadState', 'QuitState']

    for (idx, logent) in enumerate(log):
        for (bidx, bot, top) in zip(range(0, len(logent)), logent[:-1], logent[1:]):
            xpos.append(idx+0.5)
            bottom.append(bot)
            height.append(top - bot)
            colors.append(barcolors[bidx])

    patches = []
    for i in range(0,len(barcolors)):
        patches.append(mpat.Patch(color=barcolors[i], label=barregions[i]))

    plt.legend(handles=patches, loc=2)
    bars = plt.bar(xpos, height, 1, bottom, color=colors, edgecolor="none")
    plt.title('Lambda jobs')
    plt.xlabel('jobs (sorted by start time)')
    plt.ylabel('phase start and stop time')

    # similar to code at
    # http://matplotlib.org/examples/api/barchart_demo.html
    #for (i, b) in enumerate(bars):
    #    height = b.get_height()
    #    plt.text(b.get_x() + b.get_width()/2, b.get_y() + b.get_height() + 0.1, logSorted[i], ha='center', va='bottom', rotation='vertical')

    plt.savefig('%s.pdf' % sys.argv[1], dpi=300)
