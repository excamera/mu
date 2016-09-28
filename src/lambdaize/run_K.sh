#!/bin/bash

. env_setup
. k_fn_name

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]; then
    echo "Usage: $0 kf_dist n_workers n_offset y_val"
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
if [ -z "$FRAMES" ]; then
    NUM_FRAMES=6
else
    NUM_FRAMES=$FRAMES
fi
FRAME_STR=$(printf "_%02d" $NUM_FRAMES)
if [ -z "$SEVEN_FRAMES" ]; then
    VID_SUFFIX=$FRAME_STR
    XCENC_EXEC="xcenc"
    DUMP_EXEC="dump_ssim"
    FRAME_SWITCH=""
else
    VID_SUFFIX=""
    XCENC_EXEC="xcenc7"
    DUMP_EXEC="dump_ssim7"
    FRAME_SWITCH="-f $NUM_FRAMES"
fi

mkdir -p logs
LOGFILESUFFIX=k${KFDIST}_n${NWORKERS}_o${NOFFSET}_y${YVAL}_$(date +%F-%H:%M:%S)
echo -en "\033]0; ${REGION} ${LOGFILESUFFIX//_/ }\a"
set -u

if [ -z "$SSIM_ONLY" ]; then
    ./${XCENC_EXEC}_server.py \
        ${DEBUG} \
        ${UPLOAD} \
        ${FRAME_SWITCH} \
        -n ${NWORKERS} \
        -o ${NOFFSET} \
        -X $((${NWORKERS} / 2)) \
        -Y ${YVAL} \
        -K ${KFDIST} \
        -v sintel-4k-y4m"${VID_SUFFIX}" \
        -b excamera-${REGION} \
        -r ${REGION} \
        -l ${FN_NAME} \
        -t ${PORTNUM} \
        -h ${REGION}.x.tita.nyc \
        -T ${STATEPORT} \
        -R ${STATETHREADS} \
        -H ${REGION}.x.tita.nyc \
        -O logs/${XCENC_EXEC}_transitions_${LOGFILESUFFIX}.log
fi

if [ $? = 0 ] && [ ! -z "${UPLOAD}" ]; then
    ./${DUMP_EXEC}_server.py \
        ${DEBUG} \
        -n ${NWORKERS} \
        -o ${NOFFSET} \
        -X $((${NWORKERS} / 2)) \
        -Y ${YVAL} \
        -K ${KFDIST} \
        -v sintel-4k-y4m${FRAME_STR} \
        -b excamera-${REGION} \
        -r ${REGION} \
        -l ${FN_NAME} \
        -t ${PORTNUM} \
        -h ${REGION}.x.tita.nyc \
        -O logs/${DUMP_EXEC}_transitions_${LOGFILESUFFIX}.log
fi
