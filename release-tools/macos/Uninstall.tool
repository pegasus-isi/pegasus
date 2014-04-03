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
for v in $(echo "2.3 2.5 2.6 2.7"); do
    test -f /Library/Python/$v/site-packages/pegasus.pth && pegasus_files+=("/Library/Python/$v/site-packages/pegasus.pth")
done

# Executables and manpages
for f in $(/usr/sbin/pkgutil --files edu.isi.pegasus | grep -e "^bin/" -e "^share/man/man1/"); do
    test -f "/usr/$f" && pegasus_files+=("/usr/$f")
done

# Directories
test -d /usr/lib/pegasus && pegasus_files+=("/usr/lib/pegasus")
test -d /usr/share/pegasus && pegasus_files+=("/usr/share/pegasus")
test -d /usr/share/doc/pegasus && pegasus_files+=("/usr/share/doc/pegasus")

echo "Uninstalling Pegasus..."
/usr/bin/sudo -p "Enter your password: " /bin/rm -rf "${pegasus_files[@]}"
/usr/bin/sudo -p "Enter your password: " /usr/sbin/pkgutil --forget edu.isi.pegasus

exit 0

