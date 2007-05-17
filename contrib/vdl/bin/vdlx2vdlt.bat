@echo off
rem
rem convert a VDLx file into an VDLt file
rem $Id$
rem
if "%JAVA_HOME%" == "" (
    echo "Error! Please set your JAVA_HOME variable"
    exit /b 1
)

if "%VDS_HOME%" == "" (
    echo "Error! Please set your VDS_HOME variable"
    exit /b 1
)

if "%CLASSPATH%" == "" (
    echo "Error! Your CLASSPATH variable is suspiciously empty"
    exit /b 1
)

rem grab initial CLI properties
set addon=
:redo
set has=%1
if "%has:~0,2%" == "-D" (
    if "%has%" == "-D" (
	set addon=%addon% -D%2
	shift
    ) else (
        set addon=%addon% %has%
    )
    shift
    goto redo
)
set has=

%JAVA_HOME%\bin\java "-Dvds.home=%VDS_HOME%" %addon% org.griphyn.vdl.toolkit.VDLx2VDLt "%*"

