#!/bin/sh
#
# run a number of tests
#
if ! make all; then 
    echo "FATAL: Unable to create all helper programs" 1>&2
    exit 1
fi

if perl -MXML::Twig -le 'print $XML::Twig::VERSION' 2>&1 >> /dev/null; then 
    xml_grep=`type -p xml_grep`
else
    xml_grep=':'
fi

OUTFILE=`mktemp` || exit 1
ERRFILE=`mktemp` || exit 1
trap 'rm -f $OUTFILE $ERRFILE' 0

kickstart=../pegasus-kickstart
for try in hello forkme grandfather threadme threadach alarmme \
    fifo5lf.sh fifo5nolf.sh; do
    echo '+---------------------------------------------------------+'
    printf "| %-55s |\n"   "`date -Ins` $try"
    echo '+---------------------------------------------------------+' 

    $kickstart $try > $OUTFILE 2> $ERRFILE
    rc=$?
    echo "kickstart itself returned with exit code $rc"

    $xml_grep --nowrap 'invocation/statcall[@id="stdout"]/data' $OUTFILE
    $xml_grep --nowrap 'invocation/statcall[@id="stderr"]/data' $OUTFILE
    $xml_grep --nowrap 'invocation/mainjob/status' $OUTFILE
    if [ -s $ERRFILE ]; then
	# something happened
	echo "--- &< stderr &< ---"
	cat $ERRFILE
	echo "--- &< stderr &< ---"
    fi
    echo ''
done
