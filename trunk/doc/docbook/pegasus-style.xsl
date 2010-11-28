<?xml version='1.0'?>
<xsl:stylesheet
   xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <xsl:import href="http://yggdrasil.isi.edu/docbook-xsl/html/chunk.xsl"/>

    <xsl:output method="html" encoding="UTF-8" indent="no"/> 

    <xsl:param name="html.ext">.php</xsl:param>

    <xsl:param name="use.id.as.filename">yes</xsl:param>
    <xsl:param name="chunker.output.encoding">UTF-8</xsl:param>
    <xsl:param name="chunker.output.indent">yes</xsl:param>
    <xsl:param name="chunk.section.depth">0</xsl:param>
    <xsl:param name="navig.showtitles">0</xsl:param>
    <!-- xsl:param name="section.label.includes.component.label">1</xsl:param -->
    <!-- xsl:param name="section.autolabel">1</xsl:param -->


    <xsl:param name="generate.toc">
        book toc
        chapter toc
    </xsl:param>
    <!-- xsl:param name="toc.section.depth" select="'1'"/ -->
    <xsl:param name="toc.max.depth" select="'2'"/>

    <xsl:param name="navig.showtitles">1</xsl:param>

    <xsl:template match="processing-instruction('php')">
        <xsl:processing-instruction name="php">
            <xsl:value-of select="."/>
        </xsl:processing-instruction>
    </xsl:template>

    <xsl:param name="local.l10n.xml" select="document('')" />
    <l:i18n xmlns:l="http://docbook.sourceforge.net/xmlns/l10n/1.0">
        <l:l10n language="en">
            <l:gentext key="nav-home" text="Table of Contents"/>
        </l:l10n>
    </l:i18n>


    <!-- ==================================================================== -->
    <xsl:template name="chunk-element-content">
        <xsl:param name="prev"/>
        <xsl:param name="next"/>
        <xsl:param name="nav.context"/>
        <xsl:param name="content">
            <xsl:apply-imports/>
        </xsl:param>

        <xsl:call-template name="user.preroot"/>
        <html>
            <head>
                <title>Pegasus WMS User Guide</title>
                <link rel="stylesheet" type="text/css" href="/css/default.css"/>   
                <script type="text/javascript" src="/js/jquery-1.3.2.min.js"></script>
                <script type="text/javascript" src="/js/pegasus_menu.js"></script>
            </head>
            <body topmargin="0" leftmargin="0" marginwidth="0" marginheight="0">
                <div id="wrap">
                    <xsl:processing-instruction name="php"> 
                        include_once( $_SERVER['DOCUMENT_ROOT']."/includes/header2.inc" );
                        include_once( $_SERVER['DOCUMENT_ROOT']."/includes/sidebar2.inc" );
                    ?</xsl:processing-instruction>
                    <div id="content">
                        <!-- breadcrumbs are turned off
                        <xsl:call-template name="breadcrumbs"/>
                        <hr/>
                        -->
                        <xsl:call-template name="header.navigation.custom">
                            <xsl:with-param name="prev" select="$prev"/>
                            <xsl:with-param name="next" select="$next"/>
                            <xsl:with-param name="nav.context" select="$nav.context"/>
                        </xsl:call-template>
                        <xsl:copy-of select="$content"/>
                        <xsl:call-template name="footer.navigation">
                            <xsl:with-param name="prev" select="$prev"/>
                            <xsl:with-param name="next" select="$next"/>
                            <xsl:with-param name="nav.context" select="$nav.context"/>
                        </xsl:call-template>
                    </div> <!-- Close content -->
                    <xsl:processing-instruction name="php"> 
                        include_once( $_SERVER['DOCUMENT_ROOT']."/includes/footer2.inc" );
                    ?</xsl:processing-instruction>
                </div> <!-- Closing Wrap -->
            </body>
        </html>
        <xsl:value-of select="$chunk.append"/>
    </xsl:template>

    <xsl:template name="breadcrumbs">
        <xsl:param name="this.node" select="."/>
        <div class="breadcrumbs">
            <xsl:for-each select="$this.node/ancestor::*">
                <span class="breadcrumb-link">
                    <a>
                        <xsl:attribute name="href">
                            <xsl:call-template name="href.target">
                                <xsl:with-param name="object" select="."/>
                                <xsl:with-param name="context" select="$this.node"/>
                            </xsl:call-template>
                        </xsl:attribute>
                        <xsl:apply-templates select="." mode="title.markup"/>
                    </a>
                </span>
                <xsl:text> &gt; </xsl:text>
            </xsl:for-each>
            <!-- And display the current node, but not as a link -->
            <span class="breadcrumb-node">
                <xsl:apply-templates select="$this.node" mode="title.markup"/>
            </span>
        </div>
    </xsl:template>

    <xsl:template name="user.header.content">
        <xsl:call-template name="breadcrumbs"/>
    </xsl:template>
    

    <!-- ==================================================================== -->
    <xsl:template name="header.navigation.custom">
      <xsl:param name="prev" select="/foo"/>
      <xsl:param name="next" select="/foo"/>
      <xsl:param name="nav.context"/>
    
      <xsl:variable name="home" select="/*[1]"/>
      <xsl:variable name="up" select="parent::*"/>
    
      <xsl:variable name="row1" select="$navig.showtitles != 0"/>
      <xsl:variable name="row2" select="count($prev) &gt; 0
                                        or (count($up) &gt; 0 
                                            and generate-id($up) != generate-id($home)
                                            and $navig.showtitles != 0)
                                        or count($next) &gt; 0"/>
    
      <xsl:if test="$suppress.navigation = '0' and $suppress.header.navigation = '0'">
        <div class="navheader">
          <xsl:if test="$row1 or $row2">
            <table width="100%" summary="Navigation header">
              <xsl:if test="$row1">
                <tr>
                  <th colspan="3" align="center">
                    <xsl:apply-templates select="." mode="object.title.markup"/>
                  </th>
                </tr>
              </xsl:if>
    
              <xsl:if test="$row2">
                <tr>
                  <td width="20%" align="{$direction.align.start}">
                    <xsl:if test="count($prev)>0">
                      <a accesskey="p">
                        <xsl:attribute name="href">
                          <xsl:call-template name="href.target">
                            <xsl:with-param name="object" select="$prev"/>
                          </xsl:call-template>
                        </xsl:attribute>
                        <xsl:call-template name="navig.content">
                          <xsl:with-param name="direction" select="'prev'"/>
                        </xsl:call-template>
                      </a>
                    </xsl:if>
                    <xsl:text>&#160;</xsl:text>
                  </td>
                 <td width="60%" align="center">
                    <xsl:choose>
                      <xsl:when test="$home != . or $nav.context = 'toc'">
                        <a accesskey="h">
                          <xsl:attribute name="href">
                            <xsl:call-template name="href.target">
                              <xsl:with-param name="object" select="$home"/>
                            </xsl:call-template>
                          </xsl:attribute>
                          <xsl:call-template name="navig.content">
                            <xsl:with-param name="direction" select="'home'"/>
                          </xsl:call-template>
                        </a>
                        <xsl:if test="$chunk.tocs.and.lots != 0 and $nav.context != 'toc'">
                          <xsl:text>&#160;|&#160;</xsl:text>
                        </xsl:if>
                      </xsl:when>
                      <xsl:otherwise>&#160;</xsl:otherwise>
                    </xsl:choose>
                  </td>
                  <td width="20%" align="{$direction.align.end}">
                    <xsl:text>&#160;</xsl:text>
                    <xsl:if test="count($next)>0">
                      <a accesskey="n">
                        <xsl:attribute name="href">
                          <xsl:call-template name="href.target">
                            <xsl:with-param name="object" select="$next"/>
                          </xsl:call-template>
                        </xsl:attribute>
                        <xsl:call-template name="navig.content">
                          <xsl:with-param name="direction" select="'next'"/>
                        </xsl:call-template>
                      </a>
                    </xsl:if>
                  </td>
                </tr>
              </xsl:if>
            </table>
          </xsl:if>
          <xsl:if test="$header.rule != 0">
            <hr/>
          </xsl:if>
        </div>
      </xsl:if>
    </xsl:template>
    

</xsl:stylesheet>

