#!/bin/sh

echo "mArchiveList dss DSS2B M51 0.2 0.2 dss.tbl"
time  mArchiveList dss DSS2B M51 0.2 0.2 dss.tbl

echo "mBestImage dss.tbl 202.48219 47.23151"
time  mBestImage dss.tbl 202.48219 47.23151

echo "mArchiveGet -r http://stdatu.stsci.edu/cgi-bin/dss_search?v=2b&r=202.482190&d=47.231510&w=12.00&h=12.00&format=FITS m51_dss.fits"
time  mArchiveGet -r "http://stdatu.stsci.edu/cgi-bin/dss_search?v=2b&r=202.482190&d=47.231510&w=12.00&h=12.00&format=FITS" m51_dss.fits

echo "mJPEG -ct 1 -gray m51_dss.fits 0% 100% 0 -out m51_dss.jpg"
time  mJPEG -ct 1 -gray m51_dss.fits 0% 100% 0 -out m51_dss.jpg
