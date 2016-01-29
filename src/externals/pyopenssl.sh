#!/bin/bash

# PM-997: OSX El Capitan and later do not have OpenSSL headers required to
# compile the pyOpenSSL package. Fortunately, they already have pyOpenSSL
# installed, so we don't need to install it ourselves.
PYOPENSSL="Yes"
if [ -x "/usr/bin/sw_vers" ]; then
    OSX=$(/usr/bin/sw_vers -productVersion)
    MAJOR=$(echo $OSX | cut -d. -f1)
    MINOR=$(echo $OSX | cut -d. -f2)
    # El Capitan is version 10.11
    if [ "$MAJOR" -eq 10 ] && [ "$MINOR" -ge 11 ]; then
        PYOPENSSL="No"
    fi
fi

echo $PYOPENSSL
