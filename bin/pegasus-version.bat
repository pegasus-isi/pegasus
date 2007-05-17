@echo off
rem
rem just show the version number. Don't require full-fledged setup.
rem $Id: pegasus-version.bat,v 1.3 2005/07/13 16:45:22 griphyn Exp $
rem
if "%JAVA_HOME%" == "" (
    echo "Error! Please set your JAVA_HOME variable"
    exit /b 1
)

if "%PEGASUS_HOME%" == "" (
    echo "Error! Please set your PEGASUS_HOME variable"
    exit /b 1
)

if "%CLASSPATH%" == "" (
    echo "Error! Your CLASSPATH variable is suspiciously empty"
    exit /b 1
)

%JAVA_HOME%\bin\java "-pegasus.home=%PEGASUS_HOME%" org.griphyn.vdl.toolkit.VersionNumber "%*"
