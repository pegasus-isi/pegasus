#!/bin/csh
#
# set-up environment to compile GVDS - source me!
#
if ( ! $?JAVA_HOME ) then
   if ( ! $?JDK_HOME ) then
	echo "Error! Please set your JAVA_HOME variable"
	return 1
   else
	test -t 2
	if ( $? == 0 ) echo "INFO: Setting JAVA_HOME=${JDK_HOME}"
	setenv JAVA_HOME "${JDK_HOME}"
   endif
endif

# define PEGASUS_HOME or die
if ( ! $?PEGASUS_HOME ) then
    if ( ! $?VDS_HOME ) then
        echo "ERROR! You must set PEGASUS_HOME env variable."
        return 1
    else
         test -t 2
         if( $? == 0) echo "WARNING! VDS_HOME is deprectated. Please use PEGASUS_HOME env variable"
         setenv PEGASUS_HOME "${VDS_HOME}"
    endif
endif

# GT3 uses GTSR and GTPR
if ( $?GLOBUS_TCP_PORT_RANGE && ! $?GLOBUS_TCP_SOURCE_RANGE ) then
    echo 'Warning: GLOBUS_TCP_PORT_RANGE is set, but GLOBUS_TCP_SOURCE_RANGE is not.'
    echo "Setting GLOBUS_TCP_SOURCE_RANGE=$GLOBUS_TCP_PORT_RANGE"
    setenv GLOBUS_TCP_SOURCE_RANGE "$GLOBUS_TCP_PORT_RANGE"
endif

# add toolkit to PATH and MANPATH
if ( $?PATH ) then
    set x="${PEGASUS_HOME}/bin"
    set y=`echo $PATH | egrep '(^|:)'$x'($|:)' >>& /dev/null`
    if ( $? != 0 ) then
	setenv PATH "${x}:${PATH}"
    endif
    unset y
    unset x
else
    # no PATH -- very suspicious
    setenv PATH "${PEGASUS_HOME}/bin"
endif

if ( ! $?MANPATH ) then
    setenv MANPATH "/usr/man:/usr/share/man:/usr/local/man:/usr/local/share/man:/usr/X11R6/man:$PEGASUS_HOME/man"
else
    set x="${PEGASUS_HOME}/man"
    set y=`echo $MANPATH | egrep '(^|:)'$x'($|:)' >>& /dev/null`
    if ( $? != 0 ) then
	setenv MANPATH "${x}:${MANPATH}"
    endif
    unset y
    unset x
endif

if ( ! $?PERL5LIB ) then
    setenv PERL5LIB "${PEGASUS_HOME}/lib/perl"
else
    set x="${PEGASUS_HOME}/lib/perl"
    set y=`echo $PERL5LIB | egrep '(^|:)'$x'($|:)' >>& /dev/null`
    if ( $? != 0 ) then
	setenv PERL5LIB "${x}:${PERL5LIB}"
    endif
    unset y
    unset x
endif

#
# just add all jars to the CLASSPATH. 
#
set cp=`( find $PEGASUS_HOME/lib -perm -0500 -name "*.jar" | grep -v gvds | tr '\012' ':' ; echo "" ) | sed -e 's/::/:/g' -e 's/^://' -e 's/:$//'`

# merge CLASSPATH, avoid FQPN duplicates
if ( $?CLASSPATH ) then
    set cp=`(( echo "${CLASSPATH}"; echo "${cp}" ) | tr ':' '\012' | grep -v gvds.jar | grep -v ${PEGASUS_HOME}/build/classes | grep -v ${PEGASUS_HOME}/dist | sort -u | tr '\012' ':' ; echo "" ) | sed -e 's/::/:/g' -e 's/^://' -e 's/:$//'`
endif
setenv CLASSPATH "${PEGASUS_HOME}/build/classes:${cp}"
unset cp
