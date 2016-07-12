#!/usr/bin/python

import socket
import time

def lambda_handler(event, context):
    myid = 0
    if 'id' in event:
        myid = int(event['id'])
    addr = "www.example.com"
    if 'addr' in event:
        addr = event['addr']
    port = 80
    if 'port' in event:
        port = int(event['port'])

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    msg1 = "start :%6.6d" % myid
    msg2 = "finish:%6.6d" % myid
    s.sendto(msg1, (addr, port))
    s.sendto(msg1, (addr, port))
    time.sleep(5)
    s.sendto(msg2, (addr, port))
    s.sendto(msg2, (addr, port))

    s.close()
    return 0
