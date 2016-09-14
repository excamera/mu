#!/bin/bash
# given some kind of executable file,
# make an AWS lambda package out of it
#
# (C) 2016 Riad S. Wahby <rsw@cs.stanford.edu>
#          and the excamera project (https://github.com/excamera/)

set -o pipefail
DEFAULT_MEM_SIZE=128
DEFAULT_TIMEOUT=3
DEFAULT_REGION=us-east-1
EXTRA_PACKAGE_FILE=lambda_extra_packages.tar.gz
SKIPLIBS=("linux-vdso" "libc" "libpthread" "ld-linux" "librt" "libdl")

#
# check arguments
#
if [ ${#@} -lt 1 ]; then
    echo "Usage: $0 <executable> [pre-args [post-args [extrafile1 [extrafile2...]]]]"
    echo "Environment variables:"
    echo "    AWS_ROLE (required, no default) role to use when executing function"
    echo "    MEM_SIZE (optional, default ${DEFAULT_MEM_SIZE}, range 128-1536) size of lambda's memory"
    echo "    TIMEOUT (optional, default ${DEFAULT_TIMEOUT}) execution timeout in seconds"
    echo "    SKIP_UPLOAD (optional) don't upload the resulting zipfile to lambda"
    echo "    ALLOW_LD (optional) allow dynamic executable (this could break!)"
    echo "    REGION (optional, default ${DEFAULT_REGION}) install lambda in given region"
    exit 1
fi

if [ ! -x "$1" ] || [ ! -f "$1" ]; then
    echo "It appears that $1 is not an executable file. Giving up."
    exit 1
fi

LDDOUT=( $(ldd "$1" | cut -d '>' -f 2-) )
if [ $? == 0 ]; then
    if [ -z $ALLOW_LD ]; then
        echo "It appears that $1 is a dynamic executable. Giving up."
        echo "You can bypass this error by setting ALLOW_LD, but things can break."
        exit 1
    fi
else
    LDDOUT=()
fi

if [ -z $AWS_ROLE ]; then
    echo "Please specify an AWS role in the AWS_ROLE envvar. Giving up."
    exit 1
fi

#
# make sure needed executables are available
#
type -p aws &>/dev/null
if [ $? != 0 ]; then
    echo "Could not find aws executable in your path."
    echo "Hint: try \`apt-get install awscli\` and then \`aws configure\`"
    exit 1
fi
type -p zip &>/dev/null
if [ $? != 0 ]; then
    echo "Could not find zip executable in your path."
    echo "Hint: try \`apt-get install zip\`"
    exit 1
fi
type -p head &>/dev/null
if [ $? != 0 ]; then
    echo "Could not find head executable in your path."
    echo "Hint: try \`apt-get install coreutils\`"
    exit 1
fi
type -p readlink &>/dev/null
if [ $? != 0 ]; then
    echo "Could not find readlink executable in your path."
    echo "Hint: try \`apt-get install coreutils\`"
    exit 1
fi

#
# find the lambda template file
#
LAMBDAIZE_DIRNAME=$(readlink -f "$(dirname "$0")")
LAMBDA_FILE_TEMPLATE="$LAMBDAIZE_DIRNAME"/lambda_function_template.py
if [ $? != 0 ] || [ ! -f "$LAMBDA_FILE_TEMPLATE" ]; then
    echo "Cannot find "$LAMBDA_FILE_TEMPLATE". Giving up."
    exit 1
fi

#
# set defaults
#
if [ -z $MEM_SIZE ]; then
    MEM_SIZE="$DEFAULT_MEM_SIZE"
fi
if [ -z $TIMEOUT ]; then
    TIMEOUT="$DEFAULT_TIMEOUT"
fi
if [ -z $REGION ]; then
    REGION="$DEFAULT_REGION"
fi

#
# create tempdir
#
SAVEPWD=$(pwd -P)
WORKFILE="$(basename "$1")"
TMPDIR=$(mktemp -d ${WORKFILE}_XXXXXXXX)
if [ $? != 0 ]; then
    echo "ERROR: Could not make tempdir."
    exit 1
fi
LAMBDA_FILE="$TMPDIR"/lambda_function.py
FUNNAME=$(basename "$TMPDIR")
ZIPFILE="$SAVEPWD"/"$FUNNAME".zip
echo "Function name: $TMPDIR"

#
# make a local copy of the executable
#
cp "$1" "$TMPDIR"
shift

#
# "extra_packages" tarball and libmu
#
EXTRA_PACKAGES="$LAMBDAIZE_DIRNAME"/"$EXTRA_PACKAGE_FILE"
if [ -f "$EXTRA_PACKAGES" ]; then
    tar -C "$TMPDIR" --strip-components 1 -xzpf "$EXTRA_PACKAGES"
    if [ $? != 0 ]; then
        echo "ERROR: could not extract extra packages '""$EXTRA_PACKAGES""'"
        cd "$SAVEPWD"
        rm -r "$TMPDIR"
        exit 1
    fi
fi
LIBMU_DIRECTORY="$LAMBDAIZE_DIRNAME"/libmu
if [ ! -d "$LIBMU_DIRECTORY" ]; then
    echo "ERROR: cannot find libmu."
    cd "$SAVEPWD"
    rm -r "$TMPDIR"
    exit 1
fi
cp -R "$LIBMU_DIRECTORY" "$TMPDIR"

#
# if we're allowing dynamic libraries, copy over the libraries
#
if [ ! -z $ALLOW_LD ] && [ ${#LDDOUT[@]} -ge 1 ]; then
    mkdir "$TMPDIR"/solibs
    cd "$TMPDIR"/solibs

    # build up regex from SKIPLIBS
    SKIPLIBS_RE="("
    for i in $(seq 0 $((${#SKIPLIBS[@]} - 1))); do
        if [ $i != 0 ]; then
            SKIPLIBS_RE+="|"
        fi
        SKIPLIBS_RE+="${SKIPLIBS[$i]}"
    done
    SKIPLIBS_RE+=")"

    for i in $(seq 0 $((${#LDDOUT[@]} - 1))); do
        THIS=${LDDOUT[$i]}
        if ! [[ $THIS =~ ^\( || $THIS =~ $SKIPLIBS_RE ]]; then
            cp "$THIS" .
        fi
    done

    cd "$SAVEPWD"
    PRE_ARGS="LD_LIBRARY_PATH=./solibs ""$1"
else
    PRE_ARGS="$1"
fi
shift

#
# build lambda_function.py
#
head -n -1 "$LAMBDA_FILE_TEMPLATE" > "$LAMBDA_FILE"
echo 'cmdstring = """'"$PRE_ARGS"' ./'"$WORKFILE"' '"$1"'"""' >> "$LAMBDA_FILE"
shift

#
# copy the rest of the arguments into the bundle
#
for i in "$@"; do
    cp -R "$i" "$TMPDIR"
done

#
# create archive
#
cd "$TMPDIR"
zip -q -r "$ZIPFILE" .
cd "$SAVEPWD"
rm -r "$TMPDIR"

#
# upload to lambda
#
if [ -z "$SKIP_UPLOAD" ]; then
    aws lambda create-function \
        --runtime python2.7 \
        --role "$AWS_ROLE" \
        --handler lambda_function.lambda_handler \
        --function-name "$FUNNAME" \
        --description "$WORKFILE" \
        --timeout "$TIMEOUT" \
        --memory-size "$MEM_SIZE" \
        --publish \
        --region "$REGION" \
        --zip-file fileb://"$ZIPFILE"
fi
