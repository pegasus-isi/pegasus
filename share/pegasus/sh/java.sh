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
        /usr/lib/jvm/default-java \
        /usr/lib/jvm/java-openjdk \
        /usr/lib/jvm/jre-openjdk \
        /usr/lib/jvm/java-sun \
        /usr/lib/jvm/jre-sun \
    ; do
        if [ -e "${TARGET}" -a -x "${TARGET}/bin/java" ]; then
            JAVA_HOME="${TARGET}"
            export JAVA_HOME
            break
        fi
    done

    # macos
    if [ "X${JAVA_HOME}" = "X" -a -x /usr/libexec/java_home ]; then
        JAVA_HOME=`/usr/libexec/java_home -version 1.6+`
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
            heap_max=$(($mem_total / 1024 / 3))
        fi
    fi

    # MacOSX: sysctl
    if [ -e /Library -a -e /usr/sbin/sysctl ]; then
        mem_total=`/usr/sbin/sysctl -n hw.memsize 2>/dev/null`
        if [ "X$mem_total" != "X" -a $mem_total -gt 0 ]; then
            heap_max=$(($mem_total / 1024 / 1024 / 3))
        fi
    fi

    # upper limit - useful for large memory systems
    if [ $heap_max -gt 16384 ]; then
        heap_max=16384
    fi

    # upper limit - ulimit
    memulimit=`(ulimit -m | grep -v -i unlimited) 2>/dev/null` || true
    if [ "X$memulimit" != "X" ]; then
        if [ $memulimit -gt 128 ]; then
            heap_max=$(($memulimit / 1024 / 2))
        fi
    else
        memulimit=`(ulimit -v | grep -v -i unlimited) 2>/dev/null` || true
        if [ "X$memulimit" != "X" ]; then
            if [ $memulimit -gt 128 ]; then
                heap_max=$(($memulimit / 1024 / 2))
            fi
        fi
    fi

    # min is 1/2 of max - should provide good performance
    heap_min=$(($heap_max / 2))

    addon="$addon -Xms${heap_min}m -Xmx${heap_max}m"
else
    test "X${JAVA_HEAPMIN}" = "X" || addon="$addon -Xms${JAVA_HEAPMIN}m"
    test "X${JAVA_HEAPMAX}" = "X" || addon="$addon -Xmx${JAVA_HEAPMAX}m"
fi

args=""
while [ $# -gt 0 ]; do
    case "$1" in
    -[XD][_a-zA-Z]*)
        addon="$addon $1"
        ;;
    -D)
        shift
        if [[ "$1" =~ "=" ]]; then
            addon="$addon -D$1"
        else
            args="$args -D $1"
        fi
        ;;
    *)
        args="$args $1"
        ;;
     esac
     shift
done

