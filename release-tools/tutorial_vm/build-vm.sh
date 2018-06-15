#!/bin/bash

set -e
set -x

if [ $# -lt 1 ]; then
    echo "Usage: $0 VM_VERSION"
    exit 1
fi


#----------
# Variables
#----------

export AWS_PROFILE='vm-import@pegasus'
export AWS_DEFAULT_PROFILE=${AWS_PROFILE}

URL='http://mirrors.usc.edu/pub/linux/distributions/centos/7/isos/x86_64'
ISO=`curl --silent ${URL}/sha256sum.txt | grep 'Minimal'`
ISO_NAME=`echo $ISO | cut -d' ' -f2`

VM_NAME='PegasusTutorialVM'
VM_VERSION=$1
DEV_RELEASE=0
shift

if [[ "${VM_VERSION}" == *dev ]]; then
    echo "AWS VM will be skipped. AWS VM is not generated for development versions"
    DEV_RELEASE=1
fi


#-----------------------
# Step 1: Build Base Box
#-----------------------

BASE_OUT_FILE="output/base/pegasus-tutorial-base-vm-${VM_VERSION}.ovf"

packer build -var "iso_url=${URL}" \
             -var "iso_name=${ISO_NAME}" \
             -var "out_file=`basename ${BASE_OUT_FILE} .ovf`" \
             -var "vm_version=${VM_VERSION}" \
             -machine-readable \
             $@ 00-base.json | tee log-00.txt



#-----------------------------------
# Step 2:
#   Build VirtualBox VM
#   Build AWS VM (only for releases)
#-----------------------------------

VBOX_OUT_FILE="output/virtualbox/${VM_NAME}-${VM_VERSION}.ova"
AWS_OUT_FILE="output/aws/${VM_NAME}-${VM_VERSION}.ova"

EXTRA_ARGS=''
if [ ${DEV_RELEASE} -eq 1 ]; then
    EXTRA_ARGS='-except aws'
fi


packer build -var "base_ovf_path=${BASE_OUT_FILE}" \
             -var "vm_name=${VM_NAME}" \
             -var "vm_version=${VM_VERSION}" \
             -parallel=false \
             -machine-readable \
             ${EXTRA_ARGS} \
             $@ 01-pegasus-vm.json | tee log-01.txt


#----------------------------------------------------
# Step 3a: Upload VirtualBox Box to Pegasus Downloads
#----------------------------------------------------

echo "scp ${VBOX_OUT_FILE} download.pegasus.isi.edu:/srv/download.pegasus.isi.edu/public_html/pegasus/${VM_VERSION}/"
chmod 644 ${VBOX_OUT_FILE}
scp ${VBOX_OUT_FILE} download.pegasus.isi.edu:/srv/download.pegasus.isi.edu/public_html/pegasus/${VM_VERSION}/


if [ ${DEV_RELEASE} -eq 1 ]; then
    echo "Skipping AWS VM... AWS VM is not generated for development versions"
    exit 0
fi


#-------------------------------
# Step 3b: Configure AWS EC2 AMI
#-------------------------------

# Get AMI ID from log-02.txt
AMI_ID=`grep "aws: AMIs were created" log-01.txt  | grep --extended-regexp --only-matching --word-regexp ami-[a-zA-Z0-9]*`

# Copy Image
NEW_AMI=`aws ec2 copy-image --source-region 'us-west-2' \
                   --source-image-id ${AMI_ID} \
                   --name "Pegasus Tutorial VM ${VM_VERSION}" \
                   --description "Pegasus Tutorial VM ${VM_VERSION}"`

WAIT="aws ec2 wait image-available --image-id ${NEW_AMI}"
$WAIT || $WAIT

# Make it public
aws ec2 modify-image-attribute --image-id "${NEW_AMI}" --launch-permission "{\"Add\": [{\"Group\":\"all\"}]}"

# Get snapshot associated with old AMI
SNAP_ID=`aws ec2 describe-images --image-ids ${AMI_ID} --query 'Images[*].BlockDeviceMappings[*].Ebs.SnapshotId'`

# De-register Old AMI
aws ec2 deregister-image --image-id "${AMI_ID}"

# Delete old AMI's snapshot
aws ec2 delete-snapshot --snapshot-id "${SNAP_ID}"

# Tag new AMI and snapshot
NEW_SNAP=`aws ec2 describe-images --image-ids ${NEW_AMI} --query 'Images[*].BlockDeviceMappings[*].Ebs.SnapshotId'`
aws ec2 create-tags --resources "${NEW_AMI}" "${NEW_SNAP}" --tags Key=Name,Value="Pegasus Tutorial VM ${VM_VERSION}"
