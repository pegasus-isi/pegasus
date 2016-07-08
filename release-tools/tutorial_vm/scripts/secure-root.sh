#!/bin/bash

set -e


NEW=`openssl rand -base64 32`
echo "New Password: ${NEW}"

passwd <<EOT
${NEW}
${NEW}
EOT
