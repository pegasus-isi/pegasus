#!/bin/csh
#
# set-up environment to run Pegasus - source me
#

# If JAVA_HOME is not set, try some system defaults. This is useful for
# RPMs and DEBs which have explicit Java dependencies
if ( ! $?JAVA_HOME ) then
    foreach TARGET ( \
        /usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/jre \
        /usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0/jre \
        /usr/lib/jvm/java-6-openjdk/jre \
    )
        if ( -e $TARGET && -x $TARGET/bin/java ) then
            setenv JAVA_HOME $TARGET
        endif
    end
    
    # macos
    if ( ! $?JAVA_HOME && -x /usr/libexec/java_home ) then
        setenv JAVA_HOME `/usr/libexec/java_home -version 1.6`
    endif
endif

# make sure we have a JAVA_HOME
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

if ( ! $?PYTHONPATH ) then
    setenv PYTHONPATH "${PEGASUS_HOME}/lib/python"
else
    set x="${PEGASUS_HOME}/lib/python"
    set y=`echo $PYTHONPATH | egrep '(^|:)'$x'($|:)' >>& /dev/null`
    if ( $? != 0 ) then
	setenv PYTHONPATH "${x}:${PYTHONPATH}"
    endif
    unset y
    unset x
endif

#if ( -d "${PEGASUS_HOME}/contrib/gstar" ) then
#    # add G* tools to environment
#    setenv GSTAR_LOCATION "${VDS_HOME}/contrib/gstar"
#    if ( -r "${GSTAR_LOCATION}/etc/gstar-setup-env.csh" ) then
#	source "$GSTAR_LOCATION/etc/gstar-setup-env.csh"
#    endif
#endif

#
# just add all jars to the CLASSPATH. 
#
set cp=`( find ${PEGASUS_HOME}/lib -name '*.jar' | tr '\012' ':' ; echo "" ) | sed -e 's/::/:/g' -e 's/^://' -e 's/:$//'`

# merge CLASSPATH, avoid FQPN duplicates
if ( $?CLASSPATH ) then
    set cp=`perl -e 'foreach ( split /:+/, join( ":", $ENV{CLASSPATH}, "@ARGV" ) ) { $t1 .= ":$_" if ( ++$x{$_} == 1 ); } print substr($t1,1), "\n";' $cp`
endif
setenv CLASSPATH "${cp}"
unset cp
