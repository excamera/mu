source ~/.bashrc

# Set basic information
bucket="excamera-ffmpeg-input"
if $1 != ""
then
  bucket=$1
fi

lambda_count=185
if $2 != ""
then
  lambda_count=$2
fi

ffmpeg_lambda="lambda_affinity_itFtRmyk"
frames_in_1s=24

# Run Stage 1
echo "[Stage 1] Splitting the video into PNG Images with $lambda_count lambdas on $bucket bucket" 
python ffmpeg_split_upload_server.py -n $lambda_count -b $bucket -c ~/tmp/ssl/ca_cert.pem -s ~/tmp/ssl/server_cert.pem -k ~/tmp/ssl/server_key.pem -l $ffmpeg_lambda

# Run Stage 2
echo "[Stage 2] Grayscaling the PNG Images with $lambda_count lambdas on $bucket bucket"
gray_scale_lambda="ffmpeg_KApSgDOT"
num_objects=`aws s3 ls s3://$bucket/video-mp4-png-split/ --recursive | wc -l`
lambda_count=185
num_frames=$(($num_objects/$lambda_count))
python gray_scale_server.py -n $lambda_count -f $num_frames -b $bucket -c ~/tmp/ssl/ca_cert.pem -s ~/tmp/ssl/server_cert.pem -k ~/tmp/ssl/server_key.pem -l $gray_scale_lambda

# Run Stage 3
folder="video-mp4-png-split-gs"
job="job_"
tim=`echo $(($(date +%s%N)/1000000))`
job_id=$job$tim
mkdir $job_id
cd $job_id

echo "[Stage 3] Downloading images from s3..."
aws s3 sync s3://$bucket/$folder/ .

echo "[Stage 3] Merging images into a video..."
ffmpeg -r 60 -f image2 -s 1920x1080 -i '%08d.png' -vcodec libx264 -crf 25 -pix_fmt yuv420p output.mp4

echo "[Stage 3] DASHing the video..."
MP4Box -dash 10000 -frag 1000 -rap output.mp4

mpd_file=`ls *.mpd`
echo "[Stage 3] MPD file is here... $mpd_file"

echo "[Stage 3] Removing $job_id ..."
cd ..
rm -rf $job_id
