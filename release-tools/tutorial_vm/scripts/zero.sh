#!/bin/bash

cat /dev/zero > /tmp/zero ; sync ; sleep 2 ; sync ; rm /tmp/zero

truncate --size 0 ~/.bash_history ; history -c
