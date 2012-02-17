#!/bin/bash

# Pegasus ships with a couple of basic notification tools. Below
# we show how to notify via email and gtalk.

# all notifications will be sent to email
# change $USER to your full email addess
$PEGASUS_SHARE_DIR/notification/email -t $USER

# this sends notifications about failed jobs to gtalk.
# note that you can also set which events to trigger on in your DAX.
# set jabberid to your gmail address, and put in yout
# password
# uncomment to enable
#if [ "x$PEGASUS_STATUS" != "x" -a "$PEGASUS_STATUS" != "0" ]; then
#    $PEGASUS_SHARE_DIR/notification/jabber --jabberid FIXME@gmail.com --password FIXME --host talk.google.com
#fi

