#!/bin/bash

set -e


yum -y update

echo "Rebooting"
reboot
sleep 60
