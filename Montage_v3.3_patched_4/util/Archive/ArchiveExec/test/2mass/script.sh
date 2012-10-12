#!/bin/sh

echo "mArchiveList 2mass J M51 0.2 0.2 2mass.tbl"
time  mArchiveList 2mass J M51 0.2 0.2 2mass.tbl

echo "mBestImage 2mass.tbl 202.48219 47.23151"
time  mBestImage 2mass.tbl 202.48219 47.23151

echo "mArchiveGet http://irsa.ipac.caltech.edu/cgi-bin/FileDownload/nph-download?raw=1&ref=/ti08/980527n/s024/image/ji0240232.fits.gz m51_2mass.fits"
time  mArchiveGet "http://irsa.ipac.caltech.edu/cgi-bin/FileDownload/nph-download?raw=1&ref=/ti08/980527n/s024/image/ji0240232.fits.gz" m51_2mass.fits

echo "mJPEG -ct 1 -gray m51_2mass.fits 1% 99.99% 2 -out m51_2mass.jpg"
time  mJPEG -ct 1 -gray m51_2mass.fits 1% 99.99% 2 -out m51_2mass.jpg
