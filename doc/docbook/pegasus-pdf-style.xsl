<?xml version='1.0'?>
<xsl:stylesheet
   xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

   <!-- This should map via /etc/xml/catalog to a file on the local system -->
   <xsl:import href="http://docbook.sourceforge.net/release/xsl/current/fo/docbook.xsl"/>
   <!-- <xsl:import href="http://yggdrasil.isi.edu/docbook-xsl/fo/docbook.xsl"/> -->

   <xsl:param name="use.role.for.mediaobject">1</xsl:param>
   <xsl:param name="preferred.mediaobject.role">fo</xsl:param>
   <xsl:param name="body.font.master">9</xsl:param>

   <xsl:attribute-set name="monospace.verbatim.properties">
       <xsl:attribute name="wrap-option">wrap</xsl:attribute>
       <xsl:attribute name="hyphenation-character">\</xsl:attribute>
       <xsl:attribute name="font-size">7</xsl:attribute>
   </xsl:attribute-set>

   <!-- to enable including content from other files, instead of 
     putting a link or copy pasting. required to get xpointer to 
     work http://www.sagehill.net/docbookxsl/DuplicateIDs.html -->

   <xsl:template name="object.id">
     <xsl:param name="object" select="."/>
     
     <xsl:variable name="id" select="@id"/>
     <xsl:variable name="xid" select="@xml:id"/>

     <xsl:variable name="preceding.id"
		   select="count(preceding::*[@id = $id])"/>

     <xsl:variable name="preceding.xid"
		   select="count(preceding::*[@xml:id = $xid])"/>
     
     <xsl:choose>
       <xsl:when test="$object/@id and $preceding.id != 0">
	 <xsl:value-of select="concat($object/@id, $preceding.id)"/>
       </xsl:when>
       <xsl:when test="$object/@id">
	 <xsl:value-of select="$object/@id"/>
       </xsl:when>
       <xsl:when test="$object/@xml:id and $preceding.xid != 0">
	 <xsl:value-of select="concat($object/@xml:id, $preceding.xid)"/>
       </xsl:when>
       <xsl:when test="$object/@xml:id">
	 <xsl:value-of select="$object/@xml:id"/>
       </xsl:when>
       <xsl:otherwise>
	 <xsl:value-of select="generate-id($object)"/>
       </xsl:otherwise>
     </xsl:choose>
   </xsl:template>

</xsl:stylesheet>

