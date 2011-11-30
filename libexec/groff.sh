#!/bin/sh
#
# takes a file input and a file output
#
echo "calling groff $1 $2" # >> /dev/tty
case "$2" in
    *.ps)
	exec groff -mandoc $1 > $2
	;;
    *.txt)
	exec groff -mandoc -Tlatin1 $1 > $2
	;;
    *.html)
	exec groff -mandoc -Thtml $1 > $2
	;;
    *)
	echo "Illegal usage of script" 1>&2
	exit 1
	;;
esac

