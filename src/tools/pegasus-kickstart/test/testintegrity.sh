#!/bin/bash

../pegasus-kickstart -s testintegrity.data ls >test.out 2>test.err

if ! (cat test.out | grep 'sha256: c8ea869cd618ff99b394f5ce2962689adad3e32fa6d2924938f3837fdee08fe6') >/dev/null 2>&1; then
    echo "Missing/incorrect checksum in kickstart record!"
    exit 1
fi

if ! (cat test.out | grep 'checksum_timing') >/dev/null 2>&1; then
    echo "Missing/incorrect checksum timing in kickstart record!"
    exit 1
fi

exit 0


