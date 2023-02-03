#!/bin/bash

VERSION=`git log -1 --date=short --pretty=format:"%H %ad"`

cat <<EOF
#ifndef VERSION_H
#define VERSION_H

#define KEG_VERSION "$VERSION"

#endif /* VERSION_H */
EOF
