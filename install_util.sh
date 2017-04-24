#!/bin/bash
# A basic emr-cluster env setup script
#just copy this to the emr cluster first
#sudo yum -y update

if [ $# eq 1 ]
  then
    echo "Installing iPython Spark and Necessary Utilities.... Please be Patient!"
    cp aws-util/setup_jupyter_pyspark.sh .
    sh setup_jupyter_pyspark.sh
exit
fi

echo "If you want to install Python please run the script with "python" flag"
sudo yum -y install git
git clone https://github.com/kapil-utexas/aws-util.git
cp -r aws-util/.vim .
cp aws-util/.vimrc .
sudo yum install vim-X11 vim-common vim-enhanced vim-minimal
#setting env variables

