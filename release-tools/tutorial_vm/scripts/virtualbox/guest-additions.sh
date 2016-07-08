#!/bin/bash

set -e


VBOX_VERSION=$(cat /root/.vbox_version)

yum -y install bzip2 gcc kernel-devel kernel-headers

cd /tmp

mount -o loop ~/VBoxGuestAdditions_$VBOX_VERSION.iso /mnt

/mnt/VBoxLinuxAdditions.run

umount /mnt

rm  --recursive --force \
    /var/log/{vboxadd*,VBoxGuestAdditions*} \
    ~/VBoxGuestAdditions_$VBOX_VERSION.iso


