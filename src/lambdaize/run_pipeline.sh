#!/bin/bash

source /home/kvasukib/.bashrc

cd /home/kvasukib/lambda/pipeline/external/mu/src/lambdaize/

# Set basic information
bucket="excamera-sintel"
if $1 != ""
then
  bucket=$1
fi

key="input.mp4"
if $2 != ""
then
    key=$2
fi

lambda_count=888

#ffmpeg_lambda="lambda_affinity_itFtRmyk"
lambda_function="lambda_affinity_too6krJq"
frames_in_1s=24
output_folder="video-mp4-png-split-gs"
input_folder="video-mp4-png-split"

job="job_"
tim=`echo $(($(date +%s%N)/1000000))`
job_id=$job$tim
mkdir $job_id

# Run Stage 1
echo "[Stage 1] Splitting the video into PNG Images with $lambda_count lambdas on $bucket bucket" 
timeout 300 python ffmpeg_split_upload_server.py -f $frames_in_1s -n $lambda_count -b $bucket -c ~/tmp/ssl/ca_cert.pem -s ~/tmp/ssl/server_cert.pem -k ~/tmp/ssl/server_key.pem -l $lambda_function -O $job_id/stage1_output.txt -P $job_id/stage1_profile.txt | tee -a $job_id/stage1.log

# Run Stage 2
num_objects=`aws s3 ls s3://$bucket/$input_folder/ --recursive | wc -l`
lambda_count=600
num_frames=$(($num_objects/$lambda_count))

echo "[Stage 2] Grayscaling the PNG Images with $lambda_count lambdas on $bucket bucket"
timeout 300 python gray_scale_server.py -n $lambda_count -f $num_frames -b $bucket -c ~/tmp/ssl/ca_cert.pem -s ~/tmp/ssl/server_cert.pem -k ~/tmp/ssl/server_key.pem -l $lambda_function -O $job_id/stage2_output.txt -P $job_id/stage2_profile.txt | tee -a $job_id/stage2.log

cd $job_id

# Run Stage 3

echo "[Stage 3] Downloading images from s3..."
aws s3 sync s3://$bucket/$output_folder/ .

echo "[Stage 3] Merging images into a video..."
ffmpeg -r $frames_in_1s -i '%08d-gs.png' -c:v libx264 -pix_fmt yuv420p output.mp4

echo "[Stage 3] DASHing the video..."
MP4Box -dash 10000 -frag 10000 -rap -segment-name segment_ output.mp4

mpd_file=`ls *.mpd`
echo "[Stage 3] MPD file is here... $mpd_file"

cp *.mpd /srv/www/excamera/demo/grayscale/demo/static/output/
cp *.m4s /srv/www/excamera/demo/grayscale/demo/static/output/
cp *.mp4 /srv/www/excamera/demo/grayscale/demo/static/output/

cd ..
