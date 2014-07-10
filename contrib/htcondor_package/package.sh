#!/bin/bash

set -e

if [ $(uname -s) != "Darwin" ]; then
    echo "Not Mac OS X"
    exit 1
fi

if [ $# -ne 2 ]; then
    echo "Usage: $0 RELEASE_DIR DISK.dmg"
    echo ""
    echo "RELEASE_DIR is the path to the directory where the Condor release"
    echo "binaries are located. DISK.dmg is the path to the output disk"
    echo "image you want to create."
    echo ""
    echo "  Example: $0 /path/to/condor-8.0.6 condor-8.0.6.dmg"
    exit 1
fi

MYDIR=$(cd $(dirname $0) && pwd)
RELEASE_DIR=$(cd $1 && pwd)
SCRATCH_DIR=$(mktemp -d -t htcondor)
PKG_DIR=$SCRATCH_DIR/pkg
DMG_DIR=$SCRATCH_DIR/dmg
DMG=$2
DIST_XML=$SCRATCH_DIR/Distribution.dist

VERSION=$($RELEASE_DIR/bin/condor_version | awk '/\$CondorVersion:/ {print $2}')

mkdir -p $DMG_DIR $PKG_DIR

# bin dir
cp -Rp $RELEASE_DIR/bin $PKG_DIR

# sbin dir
cp -Rp $RELEASE_DIR/sbin $PKG_DIR

# lib dir
mkdir -p $PKG_DIR/lib/condor
cp -Rp $RELEASE_DIR/lib/* $PKG_DIR/lib/condor
mv $PKG_DIR/lib/condor/condor/* $PKG_DIR/lib/condor
rmdir $PKG_DIR/lib/condor/condor
mv $PKG_DIR/lib/condor/libclassad* $PKG_DIR/lib/condor/libpyclassad* $PKG_DIR/lib/condor/libcondor_utils* $PKG_DIR/lib/
mv $PKG_DIR/lib/condor/python/* $PKG_DIR/lib/condor/
rmdir $PKG_DIR/lib/condor/python

# libexec dir
mkdir -p $PKG_DIR/libexec/condor
cp -Rp $RELEASE_DIR/libexec/* $PKG_DIR/libexec/condor

# include dir
mkdir -p $PKG_DIR/include/condor
cp -Rp $RELEASE_DIR/include/* $PKG_DIR/include/condor

# Man pages
mkdir -p $PKG_DIR/share
cp -Rp $RELEASE_DIR/man $PKG_DIR/share/

# Examples and etc dir
mkdir -p $PKG_DIR/share/condor
cp -Rp $RELEASE_DIR/examples $RELEASE_DIR/etc $RELEASE_DIR/src $PKG_DIR/share/condor
cp $RELEASE_DIR/README $RELEASE_DIR/LICENSE-2.0.txt $PKG_DIR/share/condor/
cp $MYDIR/uninstall.tool $PKG_DIR/share/condor/


# Add other files to the DMG (README, License, etc)
cp $MYDIR/uninstall.tool "$DMG_DIR/Uninstall HTCondor.tool"


# Create the mail wrapper to get around problems with condor mail on Mac OS X
cat > $PKG_DIR/libexec/condor/condor_mail <<END
#!/bin/bash
cat - | /usr/bin/mail "\$@"
exit 0
END
chmod 755 $PKG_DIR/libexec/condor/condor_mail


# Build the package
pkgbuild --root $PKG_DIR \
         --identifier edu.wisc.cs.htcondor \
         --version $VERSION \
         --ownership recommended \
         --install-location /usr \
         --scripts $MYDIR/scripts \
         $SCRATCH_DIR/htcondor.pkg


# Generate the distribution file
cat > $DIST_XML <<END
<?xml version="1.0" encoding="utf-8" standalone="no"?>
<installer-gui-script minSpecVersion="1">
    <title>HTCondor</title>
    <organization>edu.wisc.cs</organization>
    <domains enable_localSystem="true"/>
    <options customize="never" require-scripts="true" rootVolumeOnly="true"/>
    <welcome file="welcome.html" mime-type="text/html" />
    <license file="license.html" mime-type="text/html" />
    <conclusion file="conclusion.html" mime-type="text/html" />
    <background file="htcondor.png" mime-type="image/png" alignment="bottomleft"/>
    <pkg-ref id="edu.wisc.cs.htcondor" version="0" auth="root">htcondor.pkg</pkg-ref>
    <choices-outline>
        <line choice="edu.wisc.cs.htcondor"/>
    </choices-outline>
    <choice id="edu.wisc.cs.htcondor"
        visible="false"
        title="HTCondor"
        description="HTCondor"
        start_selected="true">
      <pkg-ref id="edu.wisc.cs.htcondor"/>
    </choice>
</installer-gui-script>
END


# Build the installer
productbuild --distribution $DIST_XML \
             --package-path $SCRATCH_DIR \
             --resources $MYDIR/resources \
             --version $VERSION \
             "$DMG_DIR/Install HTCondor.pkg"


# Build the disk image
rm -f $SCRATCH_DIR/htcondor.dmg $DMG
hdiutil create $SCRATCH_DIR/htcondor.dmg -ov -volname "HTCondor $VERSION" -fs HFS+ -srcfolder "$DMG_DIR"
hdiutil convert $SCRATCH_DIR/htcondor.dmg -format UDZO -o $DMG


# Clean up
rm -rf $SCRATCH_DIR

