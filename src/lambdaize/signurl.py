#!/usr/bin/env python
import optparse
import sys
import os
from boto.s3.connection import S3Connection

def sign(bucket, path, access_key, secret_key, https, expiry):
    c = S3Connection(access_key, secret_key)
    return c.generate_url(
        expires_in=long(expiry),
        method='GET',
        bucket=bucket,
        key=path,
        query_auth=True,
        force_http=(not https)
    )

def invoke_sign(s3_bucket, s3_path):
    ak = os.environ["AWS_ACCESS_KEY_ID"]
    sk = os.environ["AWS_SECRET_ACCESS_KEY"]
    url = sign(
        bucket=s3_bucket,
        path=s3_path,
        access_key=ak,
        secret_key=sk,
        https=True,
        expiry=long(631138519)
    )
    return url
