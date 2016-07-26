#!/bin/bash

set -e


yum -y update

echo "Rebooting"
sh -c 'sleep 5 ; reboot' &
killall sshd
sleep 60
