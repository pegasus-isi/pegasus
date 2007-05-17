#! /bin/sh
#
# $Revision$

# genmaster - create a Word "master" document from a text list of chapter.
#    sets the attachedTemplate tag to the User Guide template (.dot)
#    so that the final document gets the right styles and necessary macros.
#    Current, the macros needed for document generation are manually inserted
#    into VDSUGTemplate.dot from VDSUserGuideMacros.vba
#
# usage: genmaster chapterlist_file <master_template.xml >master_gen.xml
#

replacepat='<wx:sect><w:p><w:r><w:t>CHAPTER_INSERT</w:t></w:r></w:p></wx:sect>'
wordtemplate=VDSUGTemplate.dot

chapterlist=$1

chapterxml=`
for c in \`grep -v '#' $chapterlist\`; do
cat <<END
<w:subDoc w:link="VDSUG_$c.xml"/>
<w:p></w:p>
END
done
`
chapterxml=`echo $chapterxml`

# Insert the new subdoc tags after the body tag...

sed -e "s@<w:body>@<w:body>$chapterxml@"  | 

# ...and set "attached template" to the local version of the UG word .dot template:

sed -e "s@<w:attachedTemplate w:val=\"[^\"]\+@<w:attachedTemplate w:val=\"$wordtemplate@"
