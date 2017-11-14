#!/bin/bash

set -e


USERID=501
GROUPID=501

groupadd --gid ${GROUPID} ${USERNAME}

useradd  --uid ${USERID}  \
         --gid ${USERNAME} \
         --groups wheel \
         --home-dir /home/${USERNAME} \
         --create-home ${USERNAME}


if [ -z "${PASSWD}" ]; then
    PASSWD='EMPTY'
    echo "${PASSWD}" | passwd --stdin ${USERNAME}
else
    echo "${PASSWD}" | passwd --stdin ${USERNAME}
fi


cat > /etc/sudoers.d/${USERNAME} << EOT
${USERNAME}        ALL=(ALL)        NOPASSWD: ALL
Defaults:${USERNAME} !requiretty
EOT

chmod 0440 /etc/sudoers.d/${USERNAME}
