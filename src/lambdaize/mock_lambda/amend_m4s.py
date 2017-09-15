#!/usr/bin/python

import sys
import mmap
import struct

def amend_m4s(filename, seqno, time_offset=None):
    f = open(filename, 'r+b')
    mm = mmap.mmap(f.fileno(), 0)
    # first remove lmsg (if any)
    styp_size = get_value(mm[0:4])
    lmsg_pos = mm.find('lmsg', 0, styp_size)
    if lmsg_pos > 0:
        mm.move(lmsg_pos, lmsg_pos+4, mm.size()-styp_size)
        f.truncate(mm.size()-4)
        styp_size -= 4
        mm[0:4] = struct.pack('>I', styp_size)
    # then get the duration
    sidx_pos = styp_size
    sidx_size = get_value(mm[sidx_pos:sidx_pos+4])
    duration = get_value(mm[sidx_pos+36:sidx_pos+40])
    if time_offset is None:
        time_offset = duration*(seqno-1)
    # update earlist prez time
    mm[sidx_pos+20:sidx_pos+24] = struct.pack('>I', time_offset)
    
    moof_pos = sidx_pos + sidx_size
    mfhd_pos = moof_pos + 8
    mfhd_size = get_value(mm[mfhd_pos:mfhd_pos+4])
    # update FragmentSequenceNumber
    mm[mfhd_pos+12:mfhd_pos+16] = struct.pack('>I', seqno)

    traf_pos = mfhd_pos + mfhd_size
    tfhd_pos = traf_pos + 8
    tfhd_size = get_value(mm[tfhd_pos:tfhd_pos+4])

    tfdt_pos = tfhd_pos + tfhd_size
    # update baseMediaDecodeTime
    mm[tfdt_pos+12:tfdt_pos+16] = struct.pack('>I', time_offset)

    mm.close()
    f.close()


def get_value(bytes):
    return struct.unpack('>I', bytes)[0]

if __name__=='__main__':
    if len(sys.argv) == 3:
        amend_m4s(sys.argv[1], int(sys.argv[2]))
    elif len(sys.argv) == 4:
        amend_m4s(sys.argv[1], int(sys.argv[2]), float(sys.argv[3]))
    else:
        sys.stderr.write(str('usage: '+sys.argv[0]+' filename FragmentSequenceNumber [TimeOffset]\n'))
        sys.exit(1)
