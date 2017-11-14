#!/bin/bash

set -e


mkdir /root/.pegasus

cat >> /etc/rc.local <<EOT
# Start Pegasus Service
pegasus-service --host 0.0.0.0 --port 5000 > /var/log/pegasus-service.log 2>&1 &
EOT

chmod +x /etc/rc.d/rc.local
