#!/bin/bash
set -x -e
# install pip & tmux
sudo yum -y install git
#setting env variables
export S3DISTCPJAR="/home/hadoop/lib/emr-s3distcp-1.0.jar"

