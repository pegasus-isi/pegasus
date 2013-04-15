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


</xsl:stylesheet>

