#!/bin/bash

source /home/kvasukib/.bashrc

cd /home/kvasukib/lambda/pipeline/external/mu/src/lambdaize/

# Set basic information
bucket="excamera-ffmpeg-input"
if $1 != ""
then
  bucket=$1
fi

key="input.mp4"
if $2 != ""
then
    key=$2
fi

lambda_count=185

ffmpeg_lambda="lambda_affinity_itFtRmyk"
frames_in_1s=30
folder="video-mp4-png-split-gs"

job="job_"
tim=`echo $(($(date +%s%N)/1000000))`
job_id=$job$tim
mkdir $job_id

# Run Stage 1
echo "[Stage 1] Splitting the video into PNG Images with $lambda_count lambdas on $bucket bucket" 
timeout 300 python ffmpeg_split_upload_server.py -f $frames_in_1s -n $lambda_count -b $bucket -c ~/tmp/ssl/ca_cert.pem -s ~/tmp/ssl/server_cert.pem -k ~/tmp/ssl/server_key.pem -l $ffmpeg_lambda | tee -a $job_id/stage1.log

# Run Stage 2
echo "[Stage 2] Grayscaling the PNG Images with $lambda_count lambdas on $bucket bucket"

gray_scale_lambda="ffmpeg_KApSgDOT"
num_objects=`aws s3 ls s3://$bucket/video-mp4-png-split/ --recursive | wc -l`
lambda_count=600
num_frames=$(($num_objects/$lambda_count))

timeout 300 python gray_scale_server.py -n $lambda_count -f $num_frames -b $bucket -c ~/tmp/ssl/ca_cert.pem -s ~/tmp/ssl/server_cert.pem -k ~/tmp/ssl/server_key.pem -l $gray_scale_lambda  | tee -a $job_id/stage2.log

cd $job_id

# Run Stage 3

echo "[Stage 3] Downloading images from s3..."
aws s3 sync s3://$bucket/$folder/ .

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

<< "COMMENT"
output_bucket="excamera-ffmpeg-output"
output_folder="output"
mpd_key="output_dash.mpd"

for f in *.mpd ; do
  aws s3 cp $f s3://$output_bucket/
done

for f in *.mp4 ; do
  aws s3 cp $f s3://$output_bucket/
done

for f in *.m4s ; do
  aws s3 cp $f s3://$output_bucket/
done

cd ..

mpd_url="https://s3-us-west-2.amazonaws.com/$output_bucket/output_dash.mpd"
mpd_signed_url=`python sign_url.py $output_bucket $mpd_key`

echo "Signed URL of mpd : $mpd_signed_url"
COMMENT
