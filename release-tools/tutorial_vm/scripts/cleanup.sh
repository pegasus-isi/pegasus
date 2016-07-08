#!/bin/bash

set -e

package-cleanup --assumeyes --oldkernels --count=1

yum -y remove gcc kernel-devel `package-cleanup --leaves | grep -v 'Loaded plugins' | xargs`

yum clean all


find /var/log -type f -exec truncate --size 0 '{}' \;

rm --recursive --force /root/anaconda-ks.cfg /tmp/*
