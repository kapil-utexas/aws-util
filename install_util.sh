#!/bin/bash
# A basic emr-cluster env setup script 
sudo yum -y update
sudo yum -y install git
git clone https://github.com/kapil-utexas/aws-util.git
cp -r aws_utils/.vim/ .
cp aws_utils/.vimrc
./setup_jupyter_pyspark.sh
#setting env variables

