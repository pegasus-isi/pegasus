#!/bin/bash

echo "user=$USER" >> $KICKSTART_METADATA
echo "pwd=$PWD" >> $KICKSTART_METADATA
echo "foo=bar" >> $KICKSTART_METADATA
echo "date='$(date)'" >> $KICKSTART_METADATA

echo "stdout message goes here"
