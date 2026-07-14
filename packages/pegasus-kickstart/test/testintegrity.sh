#!/bin/bash

set -e

$PEGASUS_BIN_DIR/pegasus-kickstart -s testintegrity.data ls >test.out 2>test.err

if ! (cat test.out | grep 'sha256: 0e42a8e0c247d101c6c7d60130b681bf1e9d613bdbc1adf18005cc34a8834c0b') >/dev/null 2>&1; then
    echo "Missing/incorrect checksum in kickstart record!"
    exit 1
fi

if ! (cat test.out | grep 'checksum_timing') >/dev/null 2>&1; then
    echo "Missing/incorrect checksum timing in kickstart record!"
    exit 1
fi

exit 0
