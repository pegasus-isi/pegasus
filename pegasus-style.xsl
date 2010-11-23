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
    <xsl:param name="section.autolabel">1</xsl:param>
    <xsl:param name="section.label.includes.component.label">1</xsl:param>


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
                        <xsl:call-template name="header.navigation">
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

</xsl:stylesheet>

