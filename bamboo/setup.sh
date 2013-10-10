#!/bin/bash

set -e
set -x

virtualenv .virtualenv

source .virtualenv/bin/activate

# The --find-links is there for the Pegasus WMS package
python setup.py develop --find-links http://gaul.isi.edu/python/

