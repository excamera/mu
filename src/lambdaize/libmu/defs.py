#!/usr/bin/python

class Defs(object):
    timeout = 300
    header_len = 13
    header_fmt = "%012d %s"
    cipher_list = "ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-SHA256:ECDHE-RSA-RC4-SHA:ECDHE-RSA-AES256-SHA:HIGH:!aNULL:!eNULL:!EXP:!LOW:!MEDIUM:!MD5:!RC4:!DES:!3DES"
    debug = False
    cmdstring = ''

    @staticmethod
    def make_cmdstring(*_):
        return Defs.cmdstring

    @staticmethod
    def make_retrievestring(_, vals):
        bucket = vals.get('bucket')
        key = vals.get('inkey')
        filename = vals.get('targfile')
        success = bucket is not None and key is not None and filename is not None

        return (success, bucket, key, filename)

    @staticmethod
    def make_uploadstring(_, vals):
        bucket = vals.get('bucket')
        key = vals.get('outkey')
        filename = vals.get('fromfile')
        success = bucket is not None and key is not None and filename is not None

        return (success, bucket, key, filename)
