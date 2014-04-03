#!/bin/bash

set -e

if [ $(uname -s) != "Darwin" ]; then
    echo "Not Mac OS X"
    exit 1
fi

if [ $# -ne 2 ]; then
    echo "Usage: $0 ROOT_DIR DMG"
    exit 1
fi

MYDIR=$(cd $(dirname $0) && pwd)
ROOT_DIR=$(cd $1 && pwd)
DIST_DIR=$(dirname $ROOT_DIR)
PKG_DIR=$DIST_DIR/pkg
DMG_DIR=$DIST_DIR/dmg
DMG=$2
PKG=$DMG_DIR/Install.pkg
DIST_XML=$DIST_DIR/Distribution.dist

VERSION=$($ROOT_DIR/bin/pegasus-version)

rm -rf $PKG_DIR $DMG_DIR
mkdir -p $DMG_DIR $PKG_DIR

# Add files to the package
cp -r $ROOT_DIR/bin $ROOT_DIR/share $ROOT_DIR/lib $PKG_DIR

# Add other files to the DMG
cp $ROOT_DIR/README $ROOT_DIR/LICENSE $ROOT_DIR/RELEASE_NOTES $ROOT_DIR/share/doc/pegasus/pegasus-user-guide.pdf $DMG_DIR/
cp $MYDIR/Uninstall.tool $DMG_DIR/

# Build the package
pkgbuild --root $PKG_DIR \
         --identifier edu.isi.pegasus \
         --version $VERSION \
         --ownership recommended \
         --install-location /usr \
         --scripts $MYDIR/scripts \
         $DIST_DIR/pegasus.pkg

# Generate the distribution file
cat > $DIST_XML <<END
<?xml version="1.0" encoding="utf-8" standalone="no"?>
<installer-gui-script minSpecVersion="1">
    <title>Pegasus Workflow Management System</title>
    <organization>edu.isi</organization>
    <domains enable_localSystem="true"/>
    <options customize="never" require-scripts="true" rootVolumeOnly="true"/>
    <welcome file="welcome.html" mime-type="text/html" />
    <license file="license.html" mime-type="text/html" />
    <conclusion file="conclusion.html" mime-type="text/html" />
    <background file="pegasusfront-black.png" mime-type="image/png" alignment="bottomleft"/>
    <pkg-ref id="edu.isi.pegasus" version="0" auth="root">pegasus.pkg</pkg-ref>
    <choices-outline>
        <line choice="edu.isi.pegasus"/>
    </choices-outline>
    <choice id="edu.isi.pegasus"
        visible="false"
        title="Pegasus"
        description="Pegasus Workflow Management System"
        start_selected="true">
      <pkg-ref id="edu.isi.pegasus"/>
    </choice>
</installer-gui-script>
END

# Build the installer
productbuild --distribution $DIST_XML \
             --package-path $DIST_DIR \
             --resources $MYDIR/resources \
             --version $VERSION \
             $PKG

# Build the dmg
rm -f $DIST_DIR/pegasus.dmg $DMG
hdiutil create $DIST_DIR/pegasus.dmg -ov -volname "Pegasus $VERSION" -fs HFS+ -srcfolder "$DMG_DIR"
hdiutil convert $DIST_DIR/pegasus.dmg -format UDZO -o $DMG

# Clean up
rm -f $DIST_XML $DIST_DIR/pegasus.pkg $DIST_DIR/pegasus.dmg
rm -rf $PKG_DIR $DMG_DIR

