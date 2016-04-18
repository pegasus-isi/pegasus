#!/bin/sh


yum update -y

yum install -y java java-devel ant ant-junit gcc gcc-c++ make python-devel \
    python-pip openssl-devel libcurl-devel git

cat > /etc/yum.repos.d/condor.repo <<END
[condor]
name=Condor
baseurl=http://www.cs.wisc.edu/condor/yum/stable/rhel7
enabled=1
gpgcheck=0
END
yum install -y condor
chkconfig condor on

# Update condor config
cat > /etc/condor/condor_config.local <<END
CONDOR_HOST = \$(IP_ADDRESS)
TRUST_UID_DOMAIN = True
END
