#!/bin/sh
#
# set-up environment to run Pegasus - source me
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

# make sure we have a JAVA_HOME
if [ "X${JAVA_HOME}" = "X" ]; then
    if [ "X${JDK_HOME}" = "X" ]; then
	echo "ERROR! Please set your JAVA_HOME variable" 1>&2
	return 1
    else
	test -t 2 && echo "INFO: Setting JAVA_HOME=${JDK_HOME}" 1>&2
	JAVA_HOME="${JDK_HOME}"
	export JAVA_HOME
   fi
fi

# define PEGASUS_HOME or die
if [ "X${PEGASUS_HOME}" = "X" ]; then
    if [ "X${VDS_HOME}" = "X" ]; then
	echo "ERROR! You must set PEGASUS_HOME env variable." 1>&2
	return 1
    else
	echo "WARNING! VDS_HOME is deprecated. You should set the PEGASUS_HOME env variable." 1>&2
	PEGASUS_HOME="${VDS_HOME}"
	export PEGASUS_HOME
    fi    
fi

# portable egrep -s -q
egrepq () {
    egrep "$1" >> /dev/null 2>&1
}

# add toolkit to PATH and MANPATH
if [ "X${PATH}" = "X" ]; then
    # no previous PATH -- very suspicious
    PATH=$PEGASUS_HOME/bin
else
    # previous PATH -- check for previous existence
    x=$PEGASUS_HOME/bin
    if ! echo $PATH | egrepq "(^|:)$x($|:)" ; then
	PATH="$x:$PATH"
    fi
    unset x
fi
export PATH

if [ "X${MANPATH}" = "X" ]; then
    MANPATH=/usr/man:/usr/share/man:/usr/local/man:/usr/local/share/man:/usr/X11R6/man:$PEGASUS_HOME/man
else
    x=$PEGASUS_HOME/man
    if ! echo $MANPATH | egrepq "(^|:)$x($|:)" ; then
	MANPATH="$x:$MANPATH"
    fi
    unset x
fi
export MANPATH

if [ "X${PERL5LIB}" = "X" ]; then
    PERL5LIB=$PEGASUS_HOME/lib/perl
else
    x=$PEGASUS_HOME/lib/perl
    if ! echo $PERL5LIB | egrepq "(^|:)$x($|:)" ; then
	PERL5LIB="$x:$PERL5LIB"
    fi
    unset x
fi
export PERL5LIB

if [ "X${PYTHONPATH}" = "X" ]; then
    PYTHONPATH=$PEGASUS_HOME/lib/python
else
    x=$PEGASUS_HOME/lib/python
    if ! echo $PYTHONPATH | egrepq "(^|:)$x($|:)" ; then
	PYTHONPATH="$x:$PYTHONPATH"
    fi
    unset x
fi
export PYTHONPATH

PEGASUS_JAR=$PEGASUS_HOME/lib/pegasus.jar
if ! (echo "$CLASSPATH" | grep "$PEGASUS_JAR") >/dev/null 2>&1; then
    if [ "x$CLASSPATH" = "x" ]; then
        CLASSPATH="$PEGASUS_JAR"
    else
        CLASSPATH="$PEGASUS_JAR:$CLASSPATH"
    fi
    export CLASSPATH
fi


