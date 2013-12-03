#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 IMAGE"
    exit 1
fi

image=$1
#location="http://gaul.isi.edu/sl/6x/x86_64/os/"
#location="http://mirrors.usc.edu/pub/linux/distributions/centos/6/os/x86_64/"
location="http://gaul.isi.edu/centos/6/os/x86_64/"
#ks="http://gaul.isi.edu/sl/pegasus-tutorial.cfg"

set -e
set -x

name=$(mktemp -u tutorial_vm.XXXXXXXXXXXX)

dir=$(cd $(dirname $0) && pwd)
VERSION=$($dir/../../release-tools/getversion)
GITHASH=$(cd $dir && git rev-parse HEAD)
sed -e "s/@@VERSION@@/$VERSION/" -e "s/@@GITHASH@@/$GITHASH/" $dir/pegasus-tutorial.cfg.in > pegasus-tutorial.cfg

virt-install \
    --name $name \
    --ram 1024 \
    --vcpus=1 \
    --os-type=linux \
    --os-variant=rhel6 \
    --graphics none \
    --disk path=$image.ec2,size=8 \
    --location $location \
    --initrd-inject=pegasus-tutorial.cfg \
    --extra-args "ks=file:/pegasus-tutorial.cfg console=ttyS0" \
    --force \
    --noreboot > $image.log

virsh undefine $name

# Create virtualbox image
qemu-img convert -f raw -O vmdk $image.ec2 $image.vmdk

# Create futuregrid image
dd if=$image.ec2 of=$image.fg bs=1M skip=1

# Zip all the images
zip $image.vmdk.zip $image.vmdk
rm $image.vmdk
gzip $image.ec2
gzip $image.fg


