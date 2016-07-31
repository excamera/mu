#!/usr/bin/python

class Defs(object):
    timeout = 30
    header_len = 13
    header_fmt = "%012d %s"
    cipher_list = "ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-SHA256:ECDHE-RSA-RC4-SHA:ECDHE-RSA-AES256-SHA:HIGH:!aNULL:!eNULL:!EXP:!LOW:!MEDIUM:!MD5:!RC4:!DES:!3DES"
    debug = False
    executable = ''
