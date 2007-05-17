#! /bin/sh
#
# $Revision: 1.1 $
#
# Sets the chapter number in a userguide chapter
# (by inserting invisible "spacer" chapters at the front of the doc,
# just after the <body> element
# theres probably a better way but I have not found it yet.)
#
# Usage: setchap.sh chapterNumber <inputXMLWordDoc >outputXMLWordDoc
#
# called from genhtml.sh
#

spacer=`cat <<END
<w:p>
  <w:pPr>
    <w:pStyle w:val="Heading1"/>
    <w:rPr>
      <w:rFonts w:fareast="MS Mincho"/>
      <w:vanish/>
    </w:rPr>
  </w:pPr>
  <w:r>
    <w:rPr>
      <w:rFonts w:fareast="MS Mincho"/>
      <w:vanish/>
    </w:rPr>
    <w:t>HEADING1SPACER</w:t>
  </w:r>
</w:p>
END
`

chno=$1
i=1
while [ $i -lt $chno ]; do
  insert=${insert}${spacer}
  i=`expr $i + 1`
done

insert=`echo $insert` # To collapse to one line

sed -e "s@<w:body>@<w:body>$insert@" 
