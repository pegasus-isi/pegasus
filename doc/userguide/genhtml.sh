#! /bin/sh
#
# $Revision: 1.3 $

# Expected files
#    .chapterlist_html (with single # before start of numbered chapters)
#    $winword
#    mtemplate.xml (an almost-empty, minimal word doc)
#
#    Manually save needed macros from this source dir into ./VDSUGTemplate.dot
#    This file will be set as the document template. It will propagate
#    in stages to the final xml-for-html files.
# 

cwd=`pwd`
cwd=`basename $cwd`
if [ $cwd = userguide ]; then
    if [ -d html ]; then
	echo $0: ERROR: html subdirectory exists - remove it and rerun.
        exit 1
    fi
    mkdir html
    cp -p *.xml *.sh *.dot chapterlist_html html
    cd html
    exec ./genhtml.sh
    exit 0
elif [ $cwd != html ]; then
    echo $0: ERROR: Must be run in either userguide or html directory. Exiting.
    exit 1
fi

# Files used:

winword="C:/Program Files/Microsoft Office/OFFICE11/WINWORD.EXE"
mastertemplate=VDSUG_MasterTemplate.xml
chapterlist=chapterlist_html
mgen=mgen.xml
tocxml=VDSUG_TOC.xml
tochtml=VDSUG_TOC.html
newtochtml=H_TOC.html
numchaplist=`mktemp /tmp/genhtml.nchaplist.XXXXXX`
export tocsedscript=`mktemp /tmp/genhtml.tocsedscript.XXXXXX`
#webdirURL=evitable.uchicago.edu:/usr/local/apache/htdocs/vds/doc/userguide/html
webdirURL=ci.uchicago.edu:~/public_html/vds/doc/userguide/html

# Create list of numbered chapters

awk '/#/,/THEEND/ { print $1 }' <$chapterlist | grep -v '#' |
( chapno=0
  while read chapter; do
    chapno=`expr $chapno + 1`
    echo $chapno $chapter
  done
) > $numchaplist

# Create master doc from template

./genmaster.sh $chapterlist <$mastertemplate >$mgen

# Open master, Update TOC, Write out updated .xml files with TOC tags

"$winword" /mUserGuideUpdateTOC $mgen

# Set toc to attach the local word .dot file (to get UG macros)

./setwordtemplate.sh $tocxml

# Open TOC, write out as HTML

"$winword" /mUserGuideSaveAsHTML $tocxml

# Edit TOC html file to get correct per-file hyperlinks

echo -n "sed " >$tocsedscript
( chapno=0
  while read chapno chapter; do
    echo -n " -e '/a href=.*>$chapno[<\.]/s,#,H_$chapter.html#,'" >>$tocsedscript
  done
) < $numchaplist

sh $tocsedscript <$tochtml >$newtochtml

# edit XML files to give them the correct chapter number

( chapno=0
  while read chapno chapter; do
    ./setchap.sh $chapno <VDSUG_$chapter.xml >H_$chapter.xml
    ./setwordtemplate.sh H_$chapter.xml
  done
) < $numchaplist

# Open each chapter xml doc, write out as html

( chapno=0
  while read chapno chapter; do
    "$winword" /mUserGuideSaveAsHTML H_$chapter.xml
  done
) < $numchaplist

# Copy files to VDS Web server

find H_*.html H_*_files -exec scp -r '{}' $webdirURL \;

# Clean up temp files (for now, leave - they are in html dir)

#rm $numchaplist
#rm $tocsedscript
