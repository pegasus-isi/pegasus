#!/bin/bash

set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 DAX"
    exit 1
fi

pegasus-plan -Dpegasus.catalog.site.file=sites.xml \
             -Dpegasus.catalog.transformation.file=tc.txt \
             -Dpegasus.catalog.replica=File \
             -Dpegasus.catalog.replica.file=rc.txt \
             -Dpegasus.register=false \
             -s local -o local --submit --dir submit --dax $1

