#!/bin/bash

echo "Hello!"

chmod 755 keg

for FILE in "$@"; do
    echo "Generating file $FILE"
    ./keg -T 30 -o $FILE=100M
done

ls -lh

