#!/bin/bash

set -e

TIF=$1
PNG=$2
YEAR=$3

cat >color.txt <<EOF
0    255 255 200
50   255 200  50
200  180   0   0 
nv    50  50  50
EOF

gdaldem color-relief $TIF color.txt temp.tif

# label
convert temp.tif -gravity North -background lavender -splice 0x50 -pointsize 40 -annotate +0+10 "$YEAR" temp.png

# legend
convert temp.png legend.png -geometry +100+100 -composite $PNG

rm -f color.txt temp.tif temp.png

