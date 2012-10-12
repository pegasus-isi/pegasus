#!/bin/sh

echo "mArchiveList sdss g M51 0.2 0.2 sdss.tbl"
time  mArchiveList sdss g M51 0.2 0.2 sdss.tbl

echo "mArchiveExec -d 2 sdss.tbl"
time  mArchiveExec -d 2 sdss.tbl
