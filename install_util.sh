#!/bin/bash
# A basic emr-cluster env setup script
#just copy this to the emr cluster first
#sudo yum -y update
sudo yum -y install git
git clone https://github.com/kapil-utexas/aws-util.git
cp -r aws-util/.vim/ .
cp aws-util/.vimrc
sh setup_jupyter_pyspark.sh
#setting env variables

