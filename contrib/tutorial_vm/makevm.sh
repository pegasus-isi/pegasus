#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 IMAGE"
    exit 1
fi

image=$1
#location="http://gaul.isi.edu/sl/6x/x86_64/os/"
location="http://mirrors.usc.edu/pub/linux/distributions/centos/6/os/x86_64/"
#ks="http://gaul.isi.edu/sl/pegasus-tutorial.cfg"

set -e
set -x

virt-install \
    -n $image \
    -r 1024 \
    --vcpus=1 \
    --os-type=linux \
    --os-variant=rhel6 \
    --accelerate \
    --hvm \
    --serial pty \
    --graphics none \
    --disk path=$image.img,size=8 \
    --location $location \
    --initrd-inject=$PWD/pegasus-tutorial.cfg \
    -x "ks=file:/pegasus-tutorial.cfg console=ttyS0" \
    --force \
    --noreboot 
    #--wait 20 \
    # --prompt
    # This doesn't work on RHEL6
    #--filesystem source=$PWD/../../,target=/mnt,mode=mapped \
    # This doesn't work because users don't own the default network
    #--network network:default

qemu-img convert -f raw -O vmdk $image.img $image.vmdk

zip $image.zip $image.vmdk

virsh undefine $image

