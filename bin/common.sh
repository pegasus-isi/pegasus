#!/bin/sh
#
# Common structure to all shell wrappers. This file
# is sourced from the shell wrapper scripts. 
# $Id$
#
if [ "X${JAVA_HOME}" = "X" ]; then
    echo "ERROR: Please set your JAVA_HOME variable" 1>&2
    exit 1
fi

if [ "X${PEGASUS_HOME}" = "X" ]; then
    echo "ERROR: Please set your PEGASUS_HOME variable" 1>&2
    exit 1
fi

# normalize PEGASUS_HOME - remove trailing slashes, double slashes, ...
# this is needed as the java code do string compares on the classpath
PEGASUS_HOME=`(cd $PEGASUS_HOME && pwd)`
export PEGASUS_HOME

# build the classpath
CLASSPATH=`( find ${PEGASUS_HOME}/lib -name '*.jar' | sort | tr '\012' ':' ; echo "" ) | sed -e 's/::/:/g' -e 's/^://' -e 's/:$//'`
if [ "x${PEGASUS_CLASSPATH_PREPEND}" != "x" ]; then
    CLASSPATH="${PEGASUS_CLASSPATH_PREPEND}:${CLASSPATH}"
fi
export CLASSPATH

addon=''
while [ true ]; do
    case "$1" in
	-[XD][_a-zA-Z]*)
	    addon="$addon $1"
	    shift
	    ;;
	-D)
	    shift
	    addon="$addon -D$1"
	    shift
	    ;;
	*)
	    break
	    ;;
     esac
done

# set no_heap_setup to anything, if you do NOT want heap setup
# FIXME: What about a user specifying their own values, but not
#        using the env vars? Will JRE take the first or last found?
if [ "X$no_heap_setup" = "X" ]; then
    test "X${JAVA_HEAPMAX}" = "X" || addon="$addon -Xmx${JAVA_HEAPMAX}m"
    test "X${JAVA_HEAPMIN}" = "X" || addon="$addon -Xms${JAVA_HEAPMIN}m"
fi
