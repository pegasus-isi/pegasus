#!/bin/bash

systemctl stop    firewalld
systemctl disable firewalld

yum -y autoremove firewalld
