#!/bin/bash

set -e

yum -y install epel-release
yum -y install ${URL}

# Install Docker

curl --location --output /etc/yum.repos.d/docker-ce.repo \
     "https://download.docker.com/linux/centos/docker-ce.repo"

yum -y install docker-ce singularity

systemctl enable docker
systemctl start  docker

chgrp docker /var/run/docker.sock
chmod 660    /var/run/docker.sock
usermod --append --groups docker ${USERNAME}
