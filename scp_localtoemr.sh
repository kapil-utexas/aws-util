#!/bin/bash
#this scripts facilitates scp from local (your machine) to remote (EMR)
# Getting your AWS MFA profile name
PROFILE=`cat $HOME/.aws/credentials | grep mfa`
PROFILE=${PROFILE:1}
PROFILE=`echo ${PROFILE%?}`

# Assuming east coast
REGION='us-east-1'

# Assume user's ssh key is named kp___.pem
PEMLOCATION=`echo $HOME/.ssh/kp*.pem`

# After running the ./emr-configure.sh script, copy the cluster id at the end,
# and input it as the only argument into this script.

CLUSTERID=$1
PATH_LOCAL_SRC=$2
PATH_REMOTE_DEST=$3
if [ $# -ne 3 ]
  then
    echo "Not enough arguments supplied: Need CLUSTERID, PATH_LOCAL_SRC, PATH_REMOTE_DEST"
    exit
fi

# Get the public DNS of the EMR cluster
dns=`echo 'aws emr describe-cluster --cluster-id "$CLUSTERID" --region "$REGION" --profile "$PROFILE" | grep MasterPublicDnsName'`
dns=`eval $dns`
dns=${dns:32}
dns_length=${#dns}
dns=${dns:0:`expr $dns_length - 2`}

# Plug that DNS into scp
scp -i "$PEMLOCATION" "$PATH_LOCAL_SRC" hadoop@"$dns":/home/hadoop/"$PATH_REMOTE_DEST"
