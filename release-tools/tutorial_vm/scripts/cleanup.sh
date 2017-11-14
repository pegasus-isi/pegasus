#!/bin/bash

package-cleanup --assumeyes --oldkernels --count=1

yum -y remove gcc kernel-devel python-pip python-devel gcc

while true; do
    LEAVES=`package-cleanup --leaves --quiet`

    if [ "${LEAVES}" == '' ]; then
        break
    fi

    yum -y remove ${LEAVES}
done

yum clean all

rm --recursive --force /root/* /var/lib/dhclient/* /tmp/*

truncate --size 0 `find /var/log -type f | xargs`

truncate --size 0 ~/.bash_history ; history -c
