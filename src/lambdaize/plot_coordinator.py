'''0:[(4.446370840072632, 'GrayScaleConfigState'), (4.854034900665283, 'GrayScaleRetrieveLoopState'), (4.854344844818115, 'GrayScaleRetrieveAndRunState'), (6.9176459312438965, 'GrayScaleRetrieveLoopState'), (6.91789698600769, 'GrayScaleRetrieveAndRunState'), (8.985846996307373, 'GrayScaleRetrieveLoopState'), (8.986078977584839, 'GrayScaleRetrieveAndRunState'), (10.90290904045105, 'GrayScaleRetrieveLoopState'), (10.903133869171143, 'GrayScaleRetrieveAndRunState'), (13.395503997802734, 'GrayScaleRetrieveLoopState'), (13.395728826522827, 'GrayScaleRetrieveAndRunState'), (15.343348026275635, 'GrayScaleRetrieveLoopState'), (15.343575954437256, 'GrayScaleRetrieveAndRunState'), (17.202186822891235, 'GrayScaleRetrieveLoopState'), (17.20243787765503, 'GrayScaleRetrieveAndRunState'), (19.076025009155273, 'GrayScaleRetrieveLoopState'), (19.076257944107056, 'GrayScaleRetrieveAndRunState'), (20.837929010391235, 'GrayScaleRetrieveLoopState'), (20.838155031204224, 'GrayScaleRetrieveAndRunState'), (22.638805866241455, 'GrayScaleRetrieveLoopState'), (22.639071941375732, 'GrayScaleRetrieveAndRunState'), (24.305959939956665, 'GrayScaleRetrieveLoopState'), (24.30620503425598, 'GrayScaleQuitState'), (24.306448936462402, 'FinalState')]'''

import sys
import re
from optparse import OptionParser

###
#  Global Variables
LOG_FILE    = "./output.out"
OUTPUT_FILE = "./output.dat"
log_entries = []
###


###
#  Parser for cmd line args
parser = OptionParser()
parser.add_option( "-f"
                 , "--file" 
                 , dest    ="log_filename"
                 , help    ="Corodinator Log file"
                 , metavar ="FILE"
                 )
(options, args) = parser.parse_args()
LOG_FILE = options.log_filename

with open(LOG_FILE) as fd:
    log_entries = fd.readlines()

fd.close()

ofd = open(OUTPUT_FILE, "w")
###


###
#  Write Refined Log entry to OUTPUT_FILE
def write_header(state_arr, ofd):
  for state in state_arr:
    ofd.write(state + "\t")
  ofd.write("\n")

def write_refined_log_entry(num, state_arr, state_map, ofd):
  if num == 0:
    write_header(state_arr, ofd)
  base = 0
  diff = 0
  ofd.write(str(num) + "\t")
  for index in range(0, len(state_arr)):
    state  = state_arr[index]
    ts_arr = state_map[state]
    diff = 0
    for ts in ts_arr:
      diff = diff + (float(ts) - float(base))
      base = ts
    ofd.write(str(diff) + "\t")
  ofd.write("\n")
###


###
#  Main entry
num = 0
for log_entry in log_entries:
  state_map = {}
  state_arr = []
  counter   = 0
  tuples    = re.findall("\((\S*), '(\S*)'\)",
                         log_entry,
                         re.M | re.I)
  for index in range(0, len(tuples)):
    entry = tuples[index]
    state = entry[1]
    if state not in state_arr:
      state_arr.append(state)
  for index in range(0, len(tuples)):
    entry     = tuples[index]
    timestamp = entry[0]
    state     = entry[1]
    if state in state_map:
      state_map[state].append(timestamp)
    else:
      state_map[state] = [timestamp]
  write_refined_log_entry(num, state_arr, state_map, ofd)
  print (log_entry)
  num = num + 1
ofd.close()
###
