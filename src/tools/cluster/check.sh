#!/bin/bash
#
# attempt to test various toggles and dials
#
if [ ! -x seqexec ]; then
    make seqexec || exit 42
fi

false=`type -P false`
true=`type -P true`
cat <<EOF > check.1
$false
$false
$false
$false
EOF

cat <<EOF > check.2
$true
$true
$true
$true
EOF

cat <<EOF > check.3
$true
$true
$false
$true
EOF

for parallel in 1 2; do
    for mode in '' '-f' '-e'; do
	for i in 1 2 3; do
	    arg="./seqexec -n $parallel $mode check.${i}"
	    echo ""
	    echo "### $arg ###"
	    $arg
	    echo "# \$?=$?"
	done
	echo "-----------------------------------------------------"
    done
    echo "====================================================="
done
