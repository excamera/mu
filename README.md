[![Build Status](https://travis-ci.org/excamera/mu.svg?branch=master)](https://travis-ci.org/excamera/mu)

# Example (WIP) #

In this example, we are going to run lambdas that grab PNG files stored on S3 as
`excamera-us-east-1:sintel-1k-png16/%08d.png`, encode them 6 frames at a time as Y4M files,
and upload them to `excamera-us-east-1:sintel-1k-y4m_06/%08d.y4m`.

## Prerequisites ##

I'm assuming you're using a Debian-ish system of recent vintage (I'm running Debian testing).

You will need the following packages:

    apt-get install build-essential g++-5 automake libssl-dev \
                    python-dev python-boto3 python-openssl \
                    libpng-dev zlib1g-dev libtool libtool-bin \
                    awscli

You'll also need an AWS ID, both for the
[AWS CLI](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)
and for the mu scripts (after you've run `aws configure`, your credentials will be in `~/.aws/credentials`).
You will also need a lambda
[execution role](http://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-create-iam-role.html).
Put these in your environment now so that you don't forget!

    export AWS_ACCESS_KEY_ID=xxxxxx
    export AWS_SECRET_ACCESS_KEY=yyyyyy
    export AWS_ROLE=lambdarole

## Getting started: building binaries ##

To start, let's build the [mu](https://github.com/excamera/mu) repository:

    mkdir -p /tmp/mu_example
    cd /tmp/mu_example
    git clone https://github.com/excamera/mu
    cd mu
    ./autogen.sh
    ./configure
    make -j$(nproc)

The other thing we'll need is the [daala\_tools](https://github.com/alfalfa/daala_tools) repo.
**Important:** note `STATIC=1` in the `make` invocation.

    cd /tmp/mu_example
    git clone https://github.com/alfalfa/daala_tools
    cd daala_tools
    make -j$(nproc) STATIC=1

## Building the lambda function ##

The next step is preparing a lambda function. Our goal is for the lambda to execute a command
like `./png2y4m -o /tmp/somefile.y4m /tmp/%08d.png`, which will convert PNGs to a Y4M.  (Don't
worry, we'll figure out how the PNGs get downloaded below.)

To do this, we'll invoke the `lambdaize.sh` script in the `mu` repo:

    cd /tmp/mu_example
    MEM_SIZE=512 TIMEOUT=120 ./mu/src/lambdaize/lambdaize.sh \
        ./daala_tools/png2y4m \
        '' \
        '-o ##OUTFILE## ##INFILE##'

`MEM_SIZE` and `TIMEOUT` are configuration options for the lambda function.  Note that this
command will use `AWS_ROLE` (see above) as the role for executing the lambda function we've
just created.

## Building the coordinating server ##

Finally, we need a server to coordinate lambda instances. The full script is in
[mu/src/lambdaize/png2y4m\_server.py](https://github.com/excamera/mu/blob/master/src/lambdaize/png2y4m_server.py).
In this section, we'll walk through it.

### pylaunch ###

Coordinating servers can use the `pylaunch` module to launch many lambdas at once in parallel.
This module is an interface to the C++ library in `mu`. Usage:

    pylaunch.launchpar(num_to_launch, lambda_function_name, \
                       access_key_id, secret_access_key, \
                       json_payload, [ region1, region2, ... ])

### `machine_state.py` overview ###

First, an overview: [libmu/machine\_state.py](https://github.com/excamera/mu/tree/master/src/lambdaize/libmu/machine_state.py)
provides general functionality for building coordinating servers.

At a high level, the idea is that we can build a state machine out of these generic classes, and
that state machine drives the computation for each worker. Each state in the machine represents
a pair, (expected client message, server command); the client always "goes first". Client
responses depend on the prior command; all responses indicating success begin with "OK".
(For more information on commands and responses, see
[libmu/handler.py](https://github.com/excamera/mu/tree/master/src/lambdaize/libmu/handler.py).)

We represent state machines as subclasses of `MachineState`. `MachineState` defines the general
state transition framework, but one should never inherit directly from `MachineState`. Instead,
most of the time a state will inherit from classes like `TerminalState`, `CommandListState`,
or `ForLoopState`. These are the three subclasses we will be using in this example;
[xcenc\_server.py](https://github.com/excamera/mu/tree/master/src/lambdaize/xcenc_server.py) encodes
a more complex state machine that makes use of several other subclasses.

#### `TerminalState` ####

`TerminalState` is simple: it's a state from which the machine never transitions. In
`png2y4m_server.py`, we have `FinalState`, which simply overrides the `extra` attribute to make
the string representation of the state more comprehensible.

Another important subclass of `TerminalState` is `ErrorState`. If a state machine enters this
state, the server will abort execution.

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
This allows us more explicit control over the client's expected response. A special case for both
`client_response` and `server_command` is `None`. In the case of `client_response`, `None` means
that the state machine should immediately send the command and transition to the next state.
For `server_response`, this means that there is no command, after a response is received.
We will see how both of these are useful later.

After a `CommandListState` sends its last command, it transitions to the state whose constructor
is supplied in the `nextState` property.

#### `ForLoopState` ####

A `ForLoopState` encodes a loop with an incrementing counter. `iterKey` is a string naming
a dictionary key associated with the iteration counter; the counter is stored in the dictionary
`self.info`, which is always carried from one state to the next. `iterInit` is the first value
given to the counter, and `iterFin` is the final value. If the value in `self.info` corresponding
to the key specified by `breakKey` is not `None`, iteration ends the next time the machine
is in the `ForLoopState`.

Each time the state machine enters the `ForLoopState`, it consults the loop counter and decides
whether to transition to `loopState` (continue looping) or `exitState` (finish looping).

Most of the time, the `expect` and `command` properties are both `None` for a `ForLoopState`.

### Coordinating png2y4m ###

In this case, our state machine is pretty simple:

1. Configure the lambda with instance-specific settings.
2. Retrieve each input PNG from S3.
3. Run the command on the retrieved files.
4. Upload the resulting Y4M.

Because each state has to refer to the state that comes after it, the classes corresponding
to each state need to be defined in reverse order. Let's start with `PNG2Y4MConfigState`,
which is the first state.

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

If the looping is not yet finished, this state goes to `PNG2Y4MRetrieveState`, else it goes to
`PNG2Y4MConvertAndUploadState`.

#### `PNG2Y4MRetrieveState` ####

This is once again a `CommandListState` subclass. It sets variables that determine which S3 object
to retrieve and the corresponding output filename, then retrieves the object. Here again we add
a final `None` state to delay transition back to the loop header until the `retrieve:` command
is complete.

Note that the first `expect` is `None` because every path leading to this state has already
waited for outstanding responses from the client; similarly, the final command is `None`,
which makes this state wait for the client's response before transitioning back to the loop header.

Note also that we override the `nextState` property *after* `PNG2Y4MRetrieveLoopState` is defined
to prevent use-before-define errors.

#### `PNG2Y4MConvertAndUploadState` ####

Another `CommandListState` that runs the png2y4m conversion command and then uploads the result,
then transitions to the FinalState.

## Putting it all together ##

Now that we've installed the lambda function, we can launch the coordinating server, which will
launch the requested number of lambda instances and coordinate their execution. Usage:

    ./png2y4m_server.py <num_to_launch> <lambda_function_name> [num_frames [num_offset [video_name]]]
