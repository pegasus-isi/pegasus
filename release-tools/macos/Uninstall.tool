#!/bin/bash

pegasus_package=$(/usr/sbin/pkgutil --pkgs="edu.isi.pegasus")
if [ -z "$pegasus_package" ]; then
    echo "Pegasus does not appear to be installed"
    exit 1
fi

while true; do
    read -p "Are you sure you want to uninstall Pegasus? [y/n] " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done

echo "Locating files..."
declare -a pegasus_files

# Python .pth files
for d in /Library/Python/2.*; do
    test -f $d/site-packages/pegasus.pth && pegasus_files+=("$d/site-packages/pegasus.pth")
    test -L $d/site-packages/Pegasus && pegasus_files+=("$d/site-packages/Pegasus")
done

# Executables and manpages
for f in $(/usr/sbin/pkgutil --files edu.isi.pegasus | grep -e "^bin/" -e "^share/man/man1/"); do
    test -f "/usr/local/$f" && pegasus_files+=("/usr/local/$f")
done

# Directories
test -d /usr/local/lib/pegasus && pegasus_files+=("/usr/local/lib/pegasus")
test -d /usr/local/share/pegasus && pegasus_files+=("/usr/local/share/pegasus")
test -d /usr/local/share/doc/pegasus && pegasus_files+=("/usr/local/share/doc/pegasus")

echo "Uninstalling Pegasus..."
/usr/bin/sudo -p "Enter your password: " /bin/rm -rf "${pegasus_files[@]}"
/usr/bin/sudo -p "Enter your password: " /usr/sbin/pkgutil --forget edu.isi.pegasus

exit 0

