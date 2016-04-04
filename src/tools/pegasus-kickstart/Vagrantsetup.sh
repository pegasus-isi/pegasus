#!/bin/sh


yum update -y

yum install -y java java-devel ant ant-junit gcc gcc-c++ make python-devel \
    python-pip openssl-devel libcurl-devel

