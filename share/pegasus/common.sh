#!/bin/bash
#
# Common structure to all shell wrappers. This file
# is sourced from the shell wrapper scripts. 
# $Id$
#

# If JAVA_HOME is not set, try some system defaults. This is useful for
# RPMs and DEBs which have explicit Java dependencies
if [ "X${JAVA_HOME}" = "X" ]; then
    for TARGET in \
        /usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/jre \
        /usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0/jre \
        /usr/lib/jvm/java-6-openjdk/jre \
    ; do
        if [ -e "${TARGET}" -a -x "${TARGET}/bin/java" ]; then
            JAVA_HOME="${TARGET}"
            export JAVA_HOME
            break
        fi
    done

    # macos
    if [ "X${JAVA_HOME}" = "X" -a -x /usr/libexec/java_home ]; then
        JAVA_HOME=`/usr/libexec/java_home -version 1.6`
    fi
fi

# Find Java
if [ "X${JAVA_HOME}" != "X" ]; then
	JAVA="${JAVA_HOME}/bin/java"
fi
if [ ! -x "${JAVA}" ]; then
	JAVA="`which java`"
fi
if [ ! -e "${JAVA}" ]; then
	echo "ERROR: java not found. Please set JAVA_HOME or PATH."
	exit 1
fi

JAVA_VERSION=`${JAVA} -mx128m -version 2>&1 | awk '/^java version/ {gsub(/"/,""); print $3}'`
if [ `echo ${JAVA_VERSION} | cut -c1,3` -lt 16 ]; then
	echo "ERROR: Java 1.6 or later required. Please set JAVA_HOME or PATH to point to a newer Java."
	exit 1
fi

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

