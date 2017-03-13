import os
import re
import sys

inputf  = sys.argv[1]
outputf = sys.argv[2]

lines = []
re_pattern = "^(\S+):\[\((\S+), 'FfmpegVideoSplitterConfigState'\), \((\S+), 'FfmpegVideoSplitterRetrieveLoopState'\), \((\S+), 'FfmpegVideoSplitterRetrieveAndRunState'\), \((\S+), 'FfmpegVideoSplitterRetrieveLoopState'\), \((\S+), 'FfmpegVideoSplitterUploadLoopState'\),.*\((\S+), 'FfmpegVideoSplitterUploadLoopState'\), \((\S+), 'FinalState'\)\]$"

with open(inputf, "r") as fd:
	lines = fd.readlines()
	fd.close()

print ("Writtng..............\n")
fd = open(outputf, "w")
fd.write("#lambda_id start config split upload finish\n")

for line in lines:
	m = re.match(re_pattern, line)
	if m:
		lambda_id = m.group(1)
	        time_2 = float(m.group(2))
        	time_3 = float(m.group(3))
		time_4 = float(m.group(4))
		time_5 = float(m.group(5))
		time_6 = float(m.group(6))
		time_7 = float(m.group(7))
		time_8 = float(m.group(8))
		launch = time_2
		config = time_3 - time_2
		run    = time_5 - time_3
		upload = time_7 - time_5
		finish = time_8 - time_7
		output_string = lambda_id + "\t" + str(launch) + "\t" + str(config) + "\t" + str(run) + "\t" + str(upload) + "\t" + str(finish) + "\n"
		fd.write(output_string)
	else:
		print (line)
fd.flush()
fd.close()
