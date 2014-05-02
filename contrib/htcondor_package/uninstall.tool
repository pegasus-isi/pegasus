#!/bin/bash

set -e

function asroot {
    /usr/bin/sudo -p "Enter your password: " "$@"
    return $?
}

if ! /usr/sbin/pkgutil --pkgs="edu.wisc.cs.htcondor" &>/dev/null; then
    echo "HTCondor does not appear to be installed"
    echo "Hit any key to continue..."
    read nothing
    exit 1
fi

# Remove configuration files?
while true; do
    read -p "Do you want to remove the HTCondor configuration files? [y/n] " rm_config
    case $rm_config in
        [Yy]* ) break;;
        [Nn]* ) break;;
        * ) echo "Please answer yes or no.";;
    esac
done

# Remove log files?
while true; do
    read -p "Do you want to remove the HTCondor log files? [y/n] " rm_logs
    case $rm_logs in
        [Yy]* ) break;;
        [Nn]* ) break;;
        * ) echo "Please answer yes or no.";;
    esac
done

# Remove condor user?
while true; do
    read -p "Do you want to remove the 'condor' user and group? [y/n] " rm_usergroup
    case $rm_usergroup in
        [Yy]* ) break;;
        [Nn]* ) break;;
        * ) echo "Please answer yes or no.";;
    esac
done

echo "Locating files..."
declare -a htcondor_files

# Executables, manpages, and libraries
for f in $(/usr/sbin/pkgutil --files edu.wisc.cs.htcondor | grep -e "^bin/" -e "^sbin/" -e "^share/man/man1/" -e "^lib/libpyclassad" -e "^lib/libclassad" -e "^lib/libcondor_util"); do
    test -f "/usr/$f" && htcondor_files+=("/usr/$f")
done

# Python libraries
for v in "2.4" "2.5" "2.6" "2.7"; do
    test -e "/Library/Python/$v/site-packages/htcondor.so" && htcondor_files+=("/Library/Python/$v/site-packages/htcondor.so")
    test -e "/Library/Python/$v/site-packages/classad.so" && htcondor_files+=("/Library/Python/$v/site-packages/classad.so")
done

# Launch Daemon plist
test -f "/Library/LaunchDaemons/edu.wisc.cs.htcondor.plist" && htcondor_files+=("/Library/LaunchDaemons/edu.wisc.cs.htcondor.plist")

# Directories
test -d /usr/lib/condor && htcondor_files+=("/usr/lib/condor")
test -d /usr/include/condor && htcondor_files+=("/usr/include/condor")
test -d /usr/libexec/condor && htcondor_files+=("/usr/libexec/condor")
test -d /usr/share/condor && htcondor_files+=("/usr/share/condor")
test -d /var/lib/condor && htcondor_files+=("/var/lib/condor")
test -d /var/spool/condor && htcondor_files+=("/var/spool/condor")
if [[ "$rm_logs" =~ ^[yY] ]]; then
    test -d /var/log/condor && htcondor_files+=("/var/log/condor")
fi
if [[ "$rm_config" =~ ^[yY] ]]; then
    test -d /etc/condor && htcondor_files+=("/etc/condor")
fi

echo "Unloading service..."
if asroot /bin/launchctl list "edu.wisc.cs.htcondor" &> /dev/null; then
    asroot /bin/launchctl unload "/Library/LaunchDaemons/edu.wisc.cs.htcondor.plist"
    sleep 3
fi

echo "Removing files..."
asroot /bin/rm -rf "${htcondor_files[@]}"

if [[ "$rm_usergroup" =~ ^[yY] ]]; then
    echo "Removing condor user and group..."
    asroot dscl . -delete /Users/condor
    asroot dscl . -delete /Groups/condor
fi

echo "Removing package..."
asroot /usr/sbin/pkgutil --forget edu.wisc.cs.htcondor &>/dev/null

exit 0

