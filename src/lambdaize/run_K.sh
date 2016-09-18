#!/bin/bash

. env_setup
. k_fn_name

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]; then
    echo "Usage: $0 n_phase_2 n_workers n_offset y_val"
    exit 1
fi

KFDIST=$1
NWORKERS=$2
NOFFSET=$3
YVAL=$4
if [ -z "$PORTNUM" ]; then
    PORTNUM=13579
fi
if [ -z "$STATEPORT" ]; then
    STATEPORT=13330
fi
if [ -z "$STATETHREADS" ]; then
    STATETHREADS=24
fi
if [ ! -z "$DEBUG" ]; then
    DEBUG="-D"
else
    DEBUG=""
fi
if [ ! -z "$NOUPLOAD" ]; then
    echo "WARNING: no upload"
    UPLOAD=""
else
    UPLOAD="-u"
fi
if [ -z "$SSIM_ONLY" ]; then
    SSIM_ONLY=""
else
    SSIM_ONLY=1
fi

set -u
echo -en "\033]0; ${REGION} k${KFDIST} n${NWORKERS} o${NOFFSET} y${YVAL} \a"

if [ -z "$SSIM_ONLY" ]; then
    ./xcenc_server.py \
        ${DEBUG} \
        ${UPLOAD} \
        -n ${NWORKERS} \
        -o ${NOFFSET} \
        -X $((${NWORKERS} / 2)) \
        -Y ${YVAL} \
        -K ${KFDIST} \
        -v sintel-4k-y4m_06 \
        -b excamera-${REGION} \
        -r ${REGION} \
        -l ${FN_NAME} \
        -t ${PORTNUM} \
        -h ${REGION}.x.tita.nyc \
        -T ${STATEPORT} \
        -R ${STATETHREADS} \
        -H ${REGION}.x.tita.nyc \
        -O xcenc_transitions.log
fi

if [ $? = 0 ] && [ ! -z "${UPLOAD}" ]; then
    ./dump_ssim_server.py \
        ${DEBUG} \
        -n ${NWORKERS} \
        -o ${NOFFSET} \
        -X $((${NWORKERS} / 2)) \
        -Y ${YVAL} \
        -K ${KFDIST} \
        -v sintel-4k-y4m_06 \
        -b excamera-${REGION} \
        -r ${REGION} \
        -l ${FN_NAME} \
        -t ${PORTNUM} \
        -h ${REGION}.x.tita.nyc \
        -O dump_ssim_transitions.log
fi
