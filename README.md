[![Build Status](https://travis-ci.org/excamera/mu.svg?branch=master)](https://travis-ci.org/excamera/mu)

# Example (WIP) #

In this example, we are going to run lambdas that grab PNG files stored on S3 as
`mybucket:sintel-1k-png16/%08d.png`, encode them 6 frames at a time as Y4M files,
and upload them to `mybucket:sintel-1k-y4m_06/%08d.y4m`.

If you want more information on running xc-enc, see
[src/lambdaize/README\_xc-enc.md](https://github.com/excamera/mu/tree/master/src/lambdaize/README_xc-enc.md).

## Prerequisites ##

I assume that you've already got the `mybucket:sintel-1k-png16/%08d.png` files. You should
get these [from Xiph](http://media.xiph.org/sintel/sintel-1k-png16/) and upload them to S3.

I also assume you're using a Debian-ish system of recent vintage (I'm running Debian testing
as of September 2016).

You will need the following packages:

    apt-get install build-essential g++-5 automake pkg-config \
                    python-dev python-boto3 libssl-dev python-openssl \
                    libpng-dev zlib1g-dev libtool libtool-bin awscli

You'll also need an AWS ID, both for the
[AWS CLI](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
and for the mu scripts (after you've run `aws configure`, your credentials will be in `~/.aws/credentials`).
You will also need a lambda
[execution role](http://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-create-iam-role.html).
Put these in your environment now so that you don't forget!

    export AWS_ACCESS_KEY_ID=xxxxxx
    export AWS_SECRET_ACCESS_KEY=yyyyyy
    export AWS_ROLE=arn:aws:iam::0123456789:role/somerole

## Getting started: building binaries ##

To start, let's build the [mu](https://github.com/excamera/mu) repository:

    mkdir -p /tmp/mu_example
    cd /tmp/mu_example
    git clone https://github.com/excamera/mu
    cd mu
    ./autogen.sh
    ./configure
    make -j$(nproc)

The other thing we'll need is the [daala\_tools](https://github.com/alfalfa/daala_tools) repo,
which contains the `png2y4m` tool we are going to run on each lambda worker.

**Important:** note `STATIC=1` in the `make` invocation. The
[lambda environment](http://docs.aws.amazon.com/lambda/latest/dg/current-supported-versions.html)
probably does not have the same system libraries as our machine, so to be safe, we should only
use statically linked binaries on lambda workers.

    cd /tmp/mu_example
    git clone https://github.com/alfalfa/daala_tools
    cd daala_tools
    make -j$(nproc) STATIC=1

## Assembling the lambda function ##

The next step is preparing a lambda function. Our goal is for the lambda to execute a command
like `./png2y4m -o /tmp/somefile.y4m /tmp/%08d.png`, which will convert PNGs to a Y4M.  (Don't
worry, we'll figure out how the PNGs get downloaded below.)

To do this, we'll invoke the `lambdaize.sh` script in the `mu` repo:

    cd /tmp/mu_example
    MEM_SIZE=1536 TIMEOUT=180 ./mu/src/lambdaize/lambdaize.sh \
        ./daala_tools/png2y4m \
        '' \
        '-i -d -o ##OUTFILE## ##INFILE##'

`MEM_SIZE` and `TIMEOUT` are configuration options for the lambda function.  Note that this
command will use `AWS_ROLE` (see above) as the role for executing the lambda function we've
just created. The command's output looks something like:

    {
        "CodeSize": 3996942,
        "LastModified": "2016-09-01T00:00:00.000+0000",
        "MemorySize": 1536,
        "CodeSha256": "yv+mJC0/2hsjTcu3BpFwWyhix1YVRimph8O1y8Oy/Lw=",
        "Description": "png2y4m",
        "FunctionName": "png2y4m_cP4Mf5pn",
        "Role": "arn:aws:iam::0123456789:role/somerole",
        "Handler": "lambda_function.lambda_handler",
        "Runtime": "python2.7",
        "Timeout": 180,
        "Version": "1",
        "FunctionArn": "arn:aws:lambda:us-east-1:0123456789:function:png2y4m_cP4Mf5pn"
    }

Your new lambda function's name is `png2y4m_cP4Mf5pn`, and you will find a correspondingly-named
zipfile in `/tmp/mu_example`. `lambdaize.sh` generates a random suffix and appends it to the
lambda function name to avoid collisions with existing functions.  If you forget the name
of your function, you can invoke `aws lambda list-functions`.

## Coordinating server ##

Finally, we will run a server to launch and coordinate the lambda instances. The full script is in
[mu/src/lambdaize/png2y4m\_server.py](https://github.com/excamera/mu/blob/master/src/lambdaize/png2y4m_server.py).

    Usage: ./png2y4m_server.py [args ...]

    You must also set the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY envvars.

      switch         description                                     default
      --             --                                              --
      -h:            show this message
      -D:            enable debug                                    (disabled)
      -O oFile:      state machine times output file                 (None)
      -P pFile:      profiling data output file                      (None)

      -n nParts:     launch nParts lambdas                           (1)
      -f nFrames:    number of frames to process in each chunk       (6)
      -o nOffset:    skip this many input chunks when processing     (0)

      -v vidName:    video name                                      ('sintel-1k')
      -b bucket:     S3 bucket in which videos are stored            ('excamera-us-east-1')
      -i inFormat:   input format ('png16', 'y4m_06', etc)           ('png16')

      -t portNum:    listen on portNum                               (13579)
      -l fnName:     lambda function name                            ('png2y4m')
      -r r1,r2,...:  comma-separated list of regions                 ('us-east-1')

      -c caCert:     CA certificate file                             (None)
      -s srvCert:    server certificate file                         (None)
      -k srvKey:     server key file                                 (None)
         (hint: you can generate new keys with <mu>/bin/genkeys.sh)
         (hint: you can use CA_CERT, SRV_CERT, SRV_KEY envvars instead)

We will need to generate SSL certs:

	mkdir -p /tmp/mu_example/ssl
    cd /tmp/mu_example/ssl
	/tmp/mu_example/mu/bin/genkeys.sh

Now we're ready to go!

    /tmp/mu_example/mu/src/lambdaize/png2y4m_server.py \
        -n 5 \
        -l png2y4m_cP4Mf5pn \
        -b mybucket \
        -c /tmp/mu_example/ssl/ca_cert.pem \
        -s /tmp/mu_example/ssl/server_cert.pem \
        -k /tmp/mu_example/ssl/server_key.pem

That's it! You're encoding files.

## In more detail... ##

### pylaunch ###

Coordinating servers use the `pylaunch` module to launch many lambdas at once in parallel.
This module is an interface to [liblaunch](https://github.com/excamera/mu/tree/master/src/launch).
Usage:

    pylaunch.launchpar(num_to_launch, lambda_function_name, \
                       access_key_id, secret_access_key, \
                       json_payload, [ region1, region2, ... ])

### `machine_state.py` overview ###

[libmu/machine\_state.py](https://github.com/excamera/mu/tree/master/src/lambdaize/libmu/machine_state.py)
provides general functionality for building coordinating servers.

At a high level, the idea is that we can build a state machine out of these generic classes, and
that state machine drives the computation for each worker. Each state in the machine represents
a pair, (expected client message, server command); the client always "goes first". Client
responses depend on the prior command; all responses indicating success begin with "OK".
(For more information on commands and responses, see
[libmu/handler.py](https://github.com/excamera/mu/tree/master/src/lambdaize/libmu/handler.py).)

We represent state machines as subclasses of `MachineState`, which is itself a subclass of
`SocketNB`. `SocketNB` is a wrapper around socket-like objects that handles non-blocking reads
and writes, a simple chunking protocol, etc.

`MachineState` defines the general state transition framework, but one should probably not inherit
directly from `MachineState`. Instead, most of the time a state will inherit from classes like
`TerminalState`, `CommandListState`, or `ForLoopState`. These are the three subclasses we
use in `png2y4m\_server.py`;
[xcenc\_server.py](https://github.com/excamera/mu/tree/master/src/lambdaize/xcenc_server.py) encodes
a more complex state machine that makes use of several other subclasses.

Immediately below I give a bit more background on each of the parent classes we use in building the
`png2y4m_server.py` state machine; below, I discuss the state machine classes themselves.

#### `TerminalState` ####

`TerminalState` is simple: it's a state from which the machine never transitions. In
`png2y4m_server.py`, we have `FinalState`, which simply overrides the `extra` attribute to make
the string representation of the state more comprehensible in debug mode.

Another important subclass of `TerminalState` is `ErrorState`. If a state machine enters this
state, the server will report a corresponding error after execution.

#### `CommandListState` ####

A `CommandListState` comprises a list of (client response, server command), and tracks the progress
through this command list. (One can think of a `CommandListState` as a straight-line sequence
of independent states.)

The `commandlist` attribute is a list of strings or tuples from which the `CommandListState`
builds the set of expected responses and the resulting commands. If an entry in `commandlist`
is a string, this is interpreted as the command that the server will send. The state will
automatically decide an expected response based on the previous command (or just "OK" for the
first command).

If an entry in `commandlist` is a tuple, this is interpreted as `(client_response, server_command)`.
This allows more explicit control over the client's expected response. A special case for both
`client_response` and `server_command` is `None`. In the case of `client_response`, `None` means
that the state machine should immediately send the command and transition to the next state.
For `server_response`, this means that there is no command, after a response is received.
We will see how both of these are useful later.

After a `CommandListState` sends its last command, it transitions to the state whose constructor
is specified in the `nextState` property.

#### `ForLoopState` ####

A `ForLoopState` encodes a loop with an incrementing counter. `iterKey` is a dictionary key
associated with the iteration counter; the counter is stored in the dictionary `self.info`, which
is always carried from one state to the next. `iterInit` is the first value given to the counter,
and `iterFin` is the final value. If the value in `self.info` corresponding to the key specified
by `breakKey` is not `None`, iteration ends the next time the machine reaches the `ForLoopState`.

Each time the state machine enters the `ForLoopState`, it consults the loop counter and decides
whether to transition to `loopState` (continue looping) or `exitState` (finish looping).

Most of the time, the `expect` and `command` properties are both `None` for a `ForLoopState`,
i.e., the state machine transitions to the next state immediately.

### Coordinating png2y4m ###

In this case, our state machine is pretty simple:

1. Configure the lambda with instance-specific settings.
2. Retrieve each input PNG from S3.
3. Run the command on the retrieved files.
4. Upload the resulting Y4M.

Because each state has to refer to the state that comes after it, the classes corresponding to each
state need to be defined in reverse order in the source file. Let's start with `PNG2Y4MConfigState`,
which is the state machine's entry point.

#### `PNG2Y4MConfigState` ####

This state is a subclass of the `CommandListState` (described above) that sets a few variables
in the worker. Its constructor first invokes the `CommandListState` constructor, then computes
the commands to send based on the worker number and the video being transcoded.

Note that the final command is `None`; the state machine will wait for the response from the
penultimate command (`seti:nonblock:0`) and immediately transition to the next state.

#### `PNG2Y4MRetrieveLoopState` ####

This state is a subclass of the `ForLoopState` that controls the number of frames that are
downloaded. (Note that the constructor is overridden here because the ServerInfo object might
be changed at run time.)

If the looping is not yet finished, this state goes to `PNG2Y4MRetrieveAndRunState`, else it goes to
`PNG2Y4MUploadState`.

#### `PNG2Y4MRetrieveAndRunState` ####

This is once again a `CommandListState` subclass. It sets variables that determine which S3 object
to retrieve and the corresponding output filename, then retrieves the object. Here again we add
a final `None` state to delay transition back to the loop header until the `retrieve:` command
is complete.

Note that the first `expect` is `None` because every path leading to this state has already
waited for outstanding responses from the client; similarly, the final command is `None`,
which makes this state wait for the client's response before transitioning back to the loop header.

Note also that we override the `nextState` property *after* `PNG2Y4MRetrieveLoopState` is defined
to prevent use-before-define errors.

#### `PNG2Y4MUploadState` ####

Another `CommandListState` that runs the png2y4m conversion command and then uploads the result,
then transitions to the FinalState.
