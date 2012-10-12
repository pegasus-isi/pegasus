#!/bin/sh

echo "mArchiveList dposs F M51 0.2 0.2 dposs.tbl"
time  mArchiveList dposs F M51 0.2 0.2 dposs.tbl

echo "mBestImage dposs.tbl 202.48219 47.23151"
time  mBestImage dposs.tbl 202.48219 47.23151

echo "mArchiveGet -r http://users.sdsc.edu/~kremenek/SRB/get_DPOSS_file_by_name_in_pixels.cgi?FN=f270.fits&X1=13626&X2=715&Y1=19787&Y2=715 m51_dposs.fits"
time  mArchiveGet -r "http://users.sdsc.edu/~kremenek/SRB/get_DPOSS_file_by_name_in_pixels.cgi?FN=f270.fits&X1=13626&X2=715&Y1=19787&Y2=715" m51_dposs.fits

echo "mJPEG -ct 1 -gray m51_dposs.fits 0% 100% 0 -out m51_dposs.jpg"
time  mJPEG -ct 1 -gray m51_dposs.fits 0% 100% 0 -out m51_dposs.jpg
