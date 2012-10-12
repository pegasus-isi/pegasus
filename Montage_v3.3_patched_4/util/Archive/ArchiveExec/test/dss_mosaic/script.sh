#!/bin/sh

echo "mArchiveList dss DSS2B M31 2.0 2.0 dss.tbl"
time  mArchiveList dss DSS2B M31 2.0 2.0 dss.tbl

echo "mArchiveExec -d 2 dss.tbl"
time  mArchiveExec -d 2 dss.tbl
