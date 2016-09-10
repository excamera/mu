# Running `xc-enc` on Lambda #

Running [xc-enc](https://github.com/alfalfa/alfalfa) on Lambda takes a few special steps compared to
running other computations, e.g., [png2y4m](https://github.com/excamera/mu/tree/master/README.md).
This is because the workers need to communicate with one another; to do this, they need help
with NAT traversal.

This document briefly demonstrates how to do this. I assume that you've read and understood
the first part of the png2y4m guide in the previous link (i.e., you've installed the necessary
packages, set up your environment, etc).

## Building binaries ##

To run xc-enc, we need the xc-enc and comp-states binary, both of which are part of
[alfalfa](https://github.com/alfalfa/alfalfa). We need to statically link them, like so:

    mkdir -p /tmp/mu_example/alfalfa
    cd /tmp/mu_example
    git clone https://github.com/alfalfa/alfalfa
    cd alfalfa
    ./autogen.sh
    ./configure --enable-all-static
    make -j$(nproc)

Now we're ready to build a lambda function:

    cd /tmp/mu_example/alfalfa/src/frontend
    MEM_SIZE=1536 TIMEOUT=300 /tmp/mu_example/mu/src/lambdaize/lambdaize.sh \
        xc-enc \
        ''
        ''
        comp-states

Note that we're not specifying any special flags for xc-enc. This is because
[xcenc\_server.py](https://github.com/excamera/mu/tree/master/src/lambdaize/xcenc_server.py) hard
codes these commands.

    {
        "CodeSize": 13395164,
        "CodeSha256": "something",
        "Version": "$LATEST",
        "Runtime": "python2.7",
        "FunctionArn": "arn:aws:lambda:us-east-1:0123456789:function:xc-enc_XXXXXXXX",
        "FunctionName": "xc-enc_XXXXXXXX",
        "Handler": "lambda_function.lambda_handler",
        "Description": "xc-enc",
        "LastModified": "2016-09-10T00:00:00.000+0000",
        "MemorySize": 1536,
        "Timeout": 300,
        "Role": "arn:aws:iam::0123456789:role/somerole"
    }

Great! On to the next step

## Running the NAT traversal server ##

Because of the way Lambdas are configured, we need some help to do NAT traversal, in the form of
[lambda\_state\_server.py](https://github.com/excamera/mu/tree/master/src/lambdaize/lambda_state_server.py).

In principle you can run this server anywhere you'd like, but it's best to run it on EC2, and
preferably in the same region as your Lambda workers, to minimize latency. If you run on EC2,
you don't need a particularly beefy instance: an m4.large seems like plenty. (In particular,
more cores won't really help, since lambda\_state\_server is single threaded, at least for now.)

I'd recommend using the latest Debian image, and possibly dist-upgrading to Testing.  You will
need to make sure that the [security group settings](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
you've chosen allow connections from your Lambda instances. The easiest way to do this is to allow
connections from anywhere. (Don't worry: connections to the both the orchestration and traversal
server require client certificates, the orchestration server provides these when invoking the
workers.)

You'll also need to set up your dev environment (see the [mu top-level README](https://github.com/excamera/mu))
and build mu on your EC2 machine.

Finally, lambda\_state\_server needs an SSL certificate and key, and a CA key. If you've
already generated one (see aforementioned README for instructions), just copy `ca_cert.pem`,
`server_cert.pem`, and `server_key.pem` to your EC2 instance. Note that at a minimum, your
orchestration server (below) needs the same CA cert and a server cert signed by that CA. (You
can just use the same server cert for both servers.)

Commandline options:

    Usage: ./lambda_state_server.py [args ...]

      switch         description                                     default
      --             --                                              --
      -h:            show this message
      -D:            enable debug                                    (disabled)
      -P pFile:      profiling data output file                      (None)

      -n nParts:     launch nParts lambdas                           (1024)

      -t portNum:    listen on portNum                               (13337)

      -c caCert:     CA certificate file                             (None)
      -s srvCert:    server certificate file                         (None)
      -k srvKey:     server key file                                 (None)
         (hint: you can generate new keys with <mu>/bin/genkeys.sh)
         (hint: you can use CA_CERT, SRV_CERT, SRV_KEY envvars instead)

Let's launch the orchestration server on the EC2 instance:

    cd /tmp/mu_example
    ./mu/src/lambdaize/lambda_state_server.py \
        -c /tmp/mu_example/ssl/ca_cert.pem \
        -s /tmp/mu_example/ssl/server_cert.pem \
        -k /tmp/mu_example/ssl/server_key.pem

Take note of your EC2 instance's public IP address; we'll need it below.

## Running the orchestration server ##

Now it's time to actually launch the jobs. Note that you can do this from the same EC2 instance
as your traversal server (but beware that the orchestration server can peg your CPU, so you might
want to opt for an m4.xlarge instance if you're running the orchestration server there).

As before, build mu. Remember that your orchestration server's certificate needs to be signed
by the same CA as your traversal server! (Again: you can just use the same cert if you'd like.)

    Usage: ./xcenc_server.py [args ...]

    You must also set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY envvars.

      switch         description                                     default
      --             --                                              --
      -h:            show this message
      -D:            enable debug                                    (disabled)
      -O oFile:      state machine times output file                 (None)
      -P pFile:      profiling data output file                      (None)

      -n nParts:     launch nParts lambdas                           (1)
      -o nOffset:    skip this many input chunks when processing     (0)
      -p nPasses:    number of xcenc passes                          (7)
      -S s_ac_qi:    use s_ac_qi for S quantizer                     (127)
      -Y y_ac_qi:    use y_ac_qi for Y quantizer                     (30)

      -v vidName:    video name                                      ('sintel-1k-y4m_06')
      -b bucket:     S3 bucket in which videos are stored            ('excamera-us-east-1')

      -t portNum:    listen on portNum                               (13579)
      -H stHostAddr: hostname or IP for nat punching host            (127.0.0.1)
      -T stHostPort: port number for nat punching host               (13337)
      -l fnName:     lambda function name                            ('xcenc')
      -r r1,r2,...:  comma-separated list of regions                 ('us-east-1')

      -c caCert:     CA certificate file                             (None)
      -s srvCert:    server certificate file                         (None)
      -k srvKey:     server key file                                 (None)
         (hint: you can generate new keys with <mu>/bin/genkeys.sh)
         (hint: you can use CA_CERT, SRV_CERT, SRV_KEY envvars instead)

So:

    cd /tmp/mu_example
    ./mu/src/lambdaize/xcenc_server.py \
        -n 25 \
        -p 5 \
        -H <EC2_IP_ADDRESS> \
        -T 13337 \
        -l xc-enc_XXXXXXXX \
        -b bucketName
        -v videoName
        -c /tmp/mu_example/ssl/ca_cert.pem \
        -s /tmp/mu_example/ssl/server_cert.pem \
        -k /tmp/mu_example/ssl/server_key.pem

Output ivf files will appear in `s3://bucketName/videoName/out/%08d.ivf`, and the output from
comp-states will appear in `s3://bucketName/videoName/comp_txt/%08d.txt`.
