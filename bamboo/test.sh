#!/bin/bash

set -e
set -x

export HOME=$PWD
mkdir -p .pegasus

source .virtualenv/bin/activate

python setup.py test

