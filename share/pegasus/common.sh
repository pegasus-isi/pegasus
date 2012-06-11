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

# start with empty arguments
addon=''

# pegasus will figure out java heap min/max unless the user has set
# JAVA_HEAPMIN/JAVA_HEAPMIN
if [ "X${JAVA_HEAPMIN}" = "X" -a "X${JAVA_HEAPMAX}" = "X" ]; then

    # default - should be ok on most systems
    heap_max=512

    # Linux: use /proc/meminfo to determine better defaults
    if [ -e /proc/meminfo ]; then
        mem_total=`(cat /proc/meminfo | grep MemTotal: | awk '{print $2;}') 2>/dev/null`
        if [ "X$mem_total" != "X" -a $mem_total -gt 0 ]; then
            heap_max=$(($mem_total / 1024 / 10))
        fi
    fi

    # MacOSX: sysctl
    if [ -e /Library -a -e /usr/sbin/sysctl ]; then
        mem_total=`/usr/sbin/sysctl -n hw.memsize 2>/dev/null`
        if [ "X$mem_total" != "X" -a $mem_total -gt 0 ]; then
            heap_max=$(($mem_total / 1024 / 1024 / 10))
        fi
    fi

    # upper limit - useful for large memory systems
    if [ $heap_max -gt 4096 ]; then
        heap_max=4096
    fi

    # min is 1/2 of max - should provide good performance
    heap_min=$(($heap_max / 2))

    addon="$addon -Xms${heap_min}m -Xmx${heap_max}m"
else
    test "X${JAVA_HEAPMIN}" = "X" || addon="$addon -Xms${JAVA_HEAPMIN}m"
    test "X${JAVA_HEAPMAX}" = "X" || addon="$addon -Xmx${JAVA_HEAPMAX}m"
fi

JAVA_VERSION=`${JAVA} $addon -version 2>&1 | awk '/^java version/ {gsub(/"/,""); print $3}'`
if [ `echo ${JAVA_VERSION} | cut -c1,3` -lt 16 ]; then
	echo "ERROR: Java 1.6 or later required. Please set JAVA_HOME or PATH to point to a newer Java."
	exit 1
fi

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

