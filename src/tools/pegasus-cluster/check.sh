#!/bin/bash
#
# $Id$
# attempt to test various toggles and dials
#
test ./pegasus-cluster || exit 42

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
	    arg="./pegasus-cluster -n $parallel $mode check.${i}"
	    echo ""
	    echo "### $arg ###"
	    $arg
	    echo "# \$?=$?"
	done
	echo "-----------------------------------------------------"
    done
    echo "====================================================="
done
