/*
 * 
 *   Copyright 2007-2008 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */

package edu.isi.pegasus.planner.catalog.site.impl;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import org.griphyn.cPlanner.classes.PoolConfig;
import org.griphyn.cPlanner.classes.GlobusVersion;
import org.griphyn.cPlanner.classes.GridFTPServer;
import org.griphyn.cPlanner.classes.GridFTPBandwidth;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.JobManager;
import org.griphyn.cPlanner.classes.LRC;
import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.classes.WorkDir;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteInfo2SiteCatalogEntry;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.parser.Parser;

import org.griphyn.cPlanner.namespace.Namespace;

import java.io.File;
import java.io.IOException;

import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * A back port to old site catalog schema for the current Site Catalog API
 * 
 * This class parses XML documents that conform to site catalog schema version 2.
 * 
 * <pre>
 *  http://pegasus.isi.edu/schema/sc-2.0.xsd
 * </pre>
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class XML extends Parser implements SiteCatalog{

    /**
     * The "not-so-official" location URL of the VDLx schema definition.
     */
    public static final String SCHEMA_LOCATION =
        "http://pegasus.isi.edu/schema/sc-2.0.xsd";

    /**
     * uri namespace
     */
    public static final String SCHEMA_NAMESPACE =
        "http://pegasus.isi.edu/schema/sitecatalog";

    public PoolConfig m_pconfig = null;

    private SiteInfo m_pool_info = null;

    private String m_namespace = null;

    private String m_key = null;

    private GridFTPServer gftp = null;
    
    /**
     * The Logging instance.
     */
    private LogManager mLogger;
    
    /**
     * Stores sites in memory
     */
    //private Map<String, SiteCatalogEntry>mSiteMap;
    private SiteStore mSiteStore;
    
    /**
     * The Site Catalog file to be parser.
     */
    private String mFilename;

    /**
     * The default constructor.
     */
    public XML(){
        super( PegasusProperties.nonSingletonInstance() );
        mLogger = LogManagerFactory.loadSingletonInstance();
        
        mSiteStore = new SiteStore();
    }
    
    /**
     * Establishes a connection to the file from the properties.
     * You will need to specify a "file" property to point to the
     * location of the on-disk instance. 
     * 
     * @param props is the property table with sufficient settings to
     *              to connect to the implementation.
     * 
     * @return true if connected, false if failed to connect.
     *
     * @throws SiteCatalogException
     */
    public boolean connect( Properties props ) throws SiteCatalogException{
        if ( props.containsKey("file") )
          return connect( props.getProperty("file") );
        return false;
    } 

    /**
     * Initializes the Site Catalog Parser instance for the file.
     *
     * @param filename is the name of the file to read.
     * 
     * @return true, 
     */
    public boolean connect( String filename ){
        mFilename = filename;
        File f = new File( filename );
        if ( f.exists() && f.canRead() ){    
            return true;
        }
        else{
            throw new RuntimeException( "Cannot read or access file " + filename );
        }
    }
    
    /**
     * Close the connection to backend file.
     */
    public void close() {
        mFilename = null;
    }

    /**
     * Returns if the connection is closed or not.
     * 
     * @return
     */
    public boolean isClosed() {
        return mFilename == null;
    }

    /**
     * Loads up the Site Catalog implementation  with the sites whose
     * site handles are specified. This is a convenience method, that can 
     * allow the backend implementations to maintain soft state if required.
     * 
     * If the implementation chooses not to implement this, just do an empty
     * implementation.
     * 
     * The site handle * is a special handle designating all sites are to be 
     * loaded.
     * 
     * @param sites   the list of sites to be loaded.
     * 
     * @return the number of sites loaded.
     * 
     * @throws SiteCatalogException in case of error.
     */
    public int load( List<String> sites ) throws SiteCatalogException {
        if( this.isClosed() ){
            throw new SiteCatalogException( "Need to connect to site catalog before loading" );
        }
        mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_PARSE_SITE_CATALOG , "site-catalog.id", mFilename,
                                LogManager.DEBUG_MESSAGE_LEVEL );
        //mLogger.log( "Parsing file " + mFilename, LogManager.DEBUG_MESSAGE_LEVEL );
        

        //setting the schema Locations
        String schemaLoc = getSchemaLocation();
        mLogger.log( "Picking schema for site catalog" + schemaLoc,
                     LogManager.CONFIG_MESSAGE_LEVEL);
        String list = XML.SCHEMA_NAMESPACE + " " + schemaLoc;
        setSchemaLocations( list );
        startParser( mFilename );
        mLogger.logEventCompletion( LogManager.DEBUG_MESSAGE_LEVEL );
        
        return mSiteStore.list().size();
    }

    /**
     * Not implemented as yet.
     * 
     * @param entry
     * @return
     * @throws edu.isi.pegasus.planner.catalog.site.SiteCatalogException
     */
    public int insert(SiteCatalogEntry entry) throws SiteCatalogException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Lists  the site handles for all the sites in the Site Catalog.
     *
     * @return A set of site handles.
     * 
     * @throws SiteCatalogException in case of error.
     */
    public Set<String> list() throws SiteCatalogException {
        return ( mSiteStore == null )?
                new HashSet():
                mSiteStore.list();
               
    }

    
    /**
     * Retrieves the <code>SiteCatalogEntry</code> for a site.
     *
     * @param handle   the site handle / identifier.
     * 
     * @return SiteCatalogEntry in case an entry is found , or <code>null</code>
     *         if no match is found.
     * 
     * 
     * @throws SiteCatalogException in case of error.
     */
    public SiteCatalogEntry lookup( String handle ) throws SiteCatalogException {
        return ( mSiteStore == null )?
                null:
                mSiteStore.lookup( handle );
    }

    /**
     * Not yet implemented as yet.
     * 
     * @param handle
     * @return
     * @throws edu.isi.pegasus.planner.catalog.site.SiteCatalogException
     */
    public int remove( String handle ) throws SiteCatalogException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public void startParser( String configxml ) {
        try {
            this.testForFile( configxml );
            mParser.parse( configxml );
        } catch ( IOException ioe ) {
            mLogger.log( "IO Error :" + ioe.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL );
        } catch ( SAXException se ) {

            if ( mLocator != null ) {
                mLogger.log( "Error in " + mLocator.getSystemId() +
                    " at line " + mLocator.getLineNumber() +
                    "at column " + mLocator.getColumnNumber() + " :" +
                    se.getMessage() , LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
    }

    public void endDocument() {

    }

    public void endElement( String uri, String localName, String qName ) {
            if ( localName.trim().equalsIgnoreCase( "sitecatalog" ) ) {
                handleConfigTagEnd();
            } else if ( localName.trim().equalsIgnoreCase( "site" ) ) {
                handlePoolTagEnd();
            } else if ( localName.trim().equalsIgnoreCase( "lrc" ) ) {
                handleLRCTagEnd();
            } else if ( localName.trim().equalsIgnoreCase( "jobmanager" ) ) {
                handleJobManagerTagEnd();
            } else if ( localName.trim().equalsIgnoreCase( "profile" ) ) {
                handleProfileTagEnd( m_pool_info );
            } else if ( localName.trim().equalsIgnoreCase( "gridftp" ) ) {
                handleGridFtpTagEnd();
            } else if ( localName.trim().equalsIgnoreCase( "workdirectory" ) ) {
                handleWorkDirectoryTagEnd( m_pool_info );
            }

            else if ( localName.trim().equalsIgnoreCase( "bandwidth" ) ) {
                handleGridFtpBandwidthTagEnd();
            } else {
                mLogger.log(
                    "Unkown element end reached :" + uri + ":" +
                    localName + ":" + qName + "-******" + mTextContent +
                    "***********", LogManager.ERROR_MESSAGE_LEVEL );
                mTextContent.setLength( 0 );
            }

    }

    public void startElement( String uri, String localName, String qName,
        Attributes attrs ) {
        try {
            if ( localName.trim().equalsIgnoreCase( "sitecatalog" ) ) {
                handleConfigTagStart();
            } else if ( localName.trim().equalsIgnoreCase( "site" ) ) {
                m_pool_info = handlePoolTagStart( m_pconfig, attrs );
            } else if ( localName.trim().equalsIgnoreCase( "lrc" ) ) {
                handleLRCTagStart( m_pool_info, attrs );
            } else if ( localName.trim().equalsIgnoreCase( "jobmanager" ) ) {
                handleJobManagerTagStart( m_pool_info, attrs );
            } else if ( localName.trim().equalsIgnoreCase( "profile" ) ) {
                handleProfileTagStart( m_pool_info, attrs );
            } else if ( localName.trim().equalsIgnoreCase( "gridftp" ) ) {
                handleGridFtpTagStart( m_pool_info, attrs );
            } else if ( localName.trim().equalsIgnoreCase( "workdirectory" ) ) {
                handleWorkDirectoryTagStart( m_pool_info, attrs );
            } else if ( localName.trim().equalsIgnoreCase( "bandwidth" ) ) {
                handleGridFtpBandwidthTagStart( m_pool_info, attrs );
            }

            else {
                mLogger.log(
                    "Unknown element in xml :" + uri + ":" +
                    localName + ":" + qName, LogManager.ERROR_MESSAGE_LEVEL );
            }
        } catch ( Exception e ) {
            e.printStackTrace();
        }

    }

    public String getSchemaLocation() {
        // treat URI as File, yes, I know - I need the basename
        File uri = new File( XML.SCHEMA_LOCATION );
        // create a pointer to the default local position
        File poolconfig = new File( this.mProps.getSysConfDir(),
            uri.getName() );

        return this.mProps.getPoolSchemaLocation( poolconfig.getAbsolutePath() );

    }


    /**
     *
     * @return PoolConfig Returns a new <code>PoolConfig<code> object when
     * it encounters start of XML.
     *
     * @see org.griphyn.cPlanner.classes.PoolConfig
     */
    private PoolConfig handleConfigTagStart() {
        m_pconfig = new PoolConfig();
        return m_pconfig;
    }

    /**
     *
     * @param pcfg Takes the PoolConfig class.
     * @param attrs Takes the atrributes returned in XML.
     *
     * @return SiteInfo returns the reference to the PooInfo ojject
     *
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.SiteInfo
     * @see org.griphyn.cPlanner.classes.PoolConfig
     */
    private SiteInfo handlePoolTagStart( PoolConfig pcfg,
        Attributes attrs ) throws Exception {
        m_pool_info = new SiteInfo();
        String handle = new String( attrs.getValue( "", "handle" ) );

        //set the id of object
        m_pool_info.setInfo(SiteInfo.HANDLE,handle);
        if ( attrs.getValue( "", "gridlaunch" ) != null ) {
            String gridlaunch = new String( attrs.getValue( "", "gridlaunch" ) );
            gridlaunch = (gridlaunch == null || gridlaunch.length() == 0 ||
                          gridlaunch.equalsIgnoreCase("null"))?
                          null:
                          gridlaunch;
            m_pool_info.setInfo( SiteInfo.GRIDLAUNCH, gridlaunch );
        }
        if ( attrs.getValue( "", "sysinfo" ) != null ) {
            String sysinfo = new String( attrs.getValue( "", "sysinfo" ) );
            m_pool_info.setInfo( SiteInfo.SYSINFO, sysinfo );
        }
        //pcfg.add( handle, m_pool_info );

        return m_pool_info;
    }

    /**
     *
     * @param pinfo Poolinfo object that is to be populated
     * @param attrs Attributes for the element
     * @throws Exception
     */
    private void handleProfileTagStart( SiteInfo pinfo, Attributes attrs ) throws
        Exception {
        m_namespace = new String( attrs.getValue( "", "namespace" ) );
        m_key = new String( attrs.getValue( "", "key" ) );
    }

    /**
     *
     * @param pinfo Poolinfo object that is to be populated
     * @param attrs Attributes for the element
     * @throws Exception
     */
    private  void handleLRCTagStart( SiteInfo pinfo, Attributes attrs ) throws
        Exception {
        LRC lrc = new LRC( attrs.getValue( "", "url" ) );
        pinfo.setInfo( SiteInfo.LRC, lrc );
    }

    /**
     * @param pinfo Poolinfo object that is to be populated
     * @param attrs Attributes for the element
     * @throws Exception
     */
    private void handleGridFtpTagStart( SiteInfo pinfo, Attributes attrs ) throws
        Exception {
        gftp = new GridFTPServer();

        String gftp_url = new String( attrs.getValue( "", "url" ) );
        gftp.setInfo( GridFTPServer.GRIDFTP_URL, gftp_url );
        GlobusVersion globusver = new GlobusVersion(
            new Integer(attrs.getValue( "", "major" ) ).intValue(),
            new Integer( attrs.getValue( "", "minor" ) ).intValue(),
            new Integer( attrs.getValue( "", "patch" ) ).intValue() );
        gftp.setInfo( GridFTPServer.GLOBUS_VERSION,
            globusver.getGlobusVersion() );

        if ( attrs.getValue( "", "storage" ) != null ) {
            gftp.setInfo( GridFTPServer.STORAGE_DIR,
                new String( attrs.getValue( "", "storage" ) ) );

        }
        if ( attrs.getValue( "", "total-size" ) != null ) {
            gftp.setInfo( GridFTPServer.TOTAL_SIZE,
                new String( attrs.getValue( "", "total-size" ) ) );
        }
        if ( attrs.getValue( "", "free-size" ) != null ) {
            gftp.setInfo( GridFTPServer.FREE_SIZE,
                new String( attrs.getValue( "", "free-size" ) ) );
        }

        //following line commented by sk setppolinfo is now called in handleGridFtpTagstop()
        //pinfo.setPoolInfo(GvdsPoolInfo.GRIDFTP, gftp);
    }

    /**
     * sk added function to handle gridftpbandwidth tag
     * @param pinfo Poolinfo object that is to be populated
     * @param attrs Attributes for the element
     * @throws Exception
     */
    private void handleGridFtpBandwidthTagStart( SiteInfo pinfo,
        Attributes attrs ) throws
        Exception {
        GridFTPBandwidth gridftp_bandwidth = new
            GridFTPBandwidth();
        String dest_id = new String( attrs.getValue( "", "dest-subnet" ) );
        gridftp_bandwidth.setInfo( GridFTPBandwidth.
            DEST_ID, dest_id );

        String avg_bw_range1 = new String( attrs.getValue( "",
            "avg-bandwidth-range1" ) );
        if ( avg_bw_range1.length() != 0 ) {
            gridftp_bandwidth.setInfo( GridFTPBandwidth.
                AVG_BW_RANGE1, avg_bw_range1 );
        }

        String avg_bw_range2 = attrs.getValue( "", "avg-bandwidth-range2" );
        if ( avg_bw_range2 != null ) {
            gridftp_bandwidth.setInfo( GridFTPBandwidth.
                AVG_BW_RANGE1, avg_bw_range2 );
        }

        String avg_bw_range3 = attrs.getValue( "", "avg-bandwidth-range3" );
        if ( avg_bw_range3 != null ) {
            gridftp_bandwidth.setInfo( GridFTPBandwidth.
                AVG_BW_RANGE1, avg_bw_range3 );
        }

        String avg_bw_range4 = attrs.getValue( "", "avg-bandwidth-range4" );
        if ( avg_bw_range4 != null ) {
            gridftp_bandwidth.setInfo( GridFTPBandwidth.
                AVG_BW_RANGE1, avg_bw_range4 );
        }

        gridftp_bandwidth.setInfo( GridFTPBandwidth.
            AVG_BW,
            new String( attrs.getValue( "", "avg-bandwidth" ) ) );
        gridftp_bandwidth.setInfo( GridFTPBandwidth.
            MAX_BW,
            new String( attrs.getValue( "", "max-bandwidth" ) ) );
        gridftp_bandwidth.setInfo( GridFTPBandwidth.
            MIN_BW,
            new String( attrs.getValue( "", "min-bandwidth" ) ) );

        gftp.setGridFTPBandwidthInfo( gridftp_bandwidth );

    }

    /**
     * This method handles the start of a jobmanager tag.
     *
     * @param pinfo The <code>PoolInfo</code> object which will hold the jobmanager information
     * @param attrs The attributes about the jobmanager tag returned from the XML.
     *
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.SiteInfo
     */
    private  void handleJobManagerTagStart( SiteInfo pinfo,
        Attributes attrs ) throws
        Exception {
        JobManager jbinfo = new JobManager();

        jbinfo.setInfo( JobManager.UNIVERSE,
            new String( attrs.getValue( "", "universe" ) ) );
        jbinfo.setInfo( JobManager.URL,
            new String( attrs.getValue( "", "url" ) ) );
        GlobusVersion globusver = new GlobusVersion( new
            Integer(
            attrs.getValue( "", "major" ) ).intValue(),
            new Integer( attrs.getValue( "", "minor" ) ).intValue(),
            new Integer( attrs.getValue( "", "patch" ) ).intValue() );
        jbinfo.setInfo( JobManager.GLOBUS_VERSION,
            globusver.getGlobusVersion() );
        if ( attrs.getValue( "", "free-mem" ) != null ) {
            jbinfo.setInfo( JobManager.FREE_MEM,
                new String( attrs.getValue( "", "free-mem" ) ) );

        }
        if ( attrs.getValue( "", "total-mem" ) != null ) {
            jbinfo.setInfo( JobManager.TOTAL_MEM,
                new String( attrs.getValue( "", "total-mem" ) ) );

        }
        if ( attrs.getValue( "", "max-count" ) != null ) {
            jbinfo.setInfo( JobManager.MAX_COUNT,
                new String( attrs.getValue( "", "max-count" ) ) );

        }
        if ( attrs.getValue( "", "max-cpu-time" ) != null ) {
            jbinfo.setInfo( JobManager.MAX_CPU_TIME,
                new
                String( attrs.getValue( "", "max-cpu-time" ) ) );

        }
        if ( attrs.getValue( "", "running-jobs" ) != null ) {
            jbinfo.setInfo( JobManager.RUNNING_JOBS,
                new
                String( attrs.getValue( "", "running-jobs" ) ) );

        }
        if ( attrs.getValue( "", "jobs-in-queue" ) != null ) {
            jbinfo.setInfo( JobManager.JOBS_IN_QUEUE,
                new
                String( attrs.getValue( "", "jobs-in-queue" ) ) );

        }
        if ( attrs.getValue( "", "max-cpu-time" ) != null ) {
            jbinfo.setInfo( JobManager.MAX_CPU_TIME,
                new
                String( attrs.getValue( "", "max-cpu-time" ) ) );
        }
        if ( attrs.getValue( "", "idle-nodes" ) != null ) {
            jbinfo.setInfo( JobManager.IDLE_NODES,
                new String( attrs.getValue( "", "idle-nodes" ) ) );

        }
        if ( attrs.getValue( "", "total-nodes" ) != null ) {
            jbinfo.setInfo( JobManager.TOTAL_NODES,
                new
                String( attrs.getValue( "", "total-nodes" ) ) );
        }
        if ( attrs.getValue( "", "os" ) != null ) {
            jbinfo.setInfo( JobManager.OS_TYPE,
                new String( attrs.getValue( "", "os" ) ) );
        }
        if ( attrs.getValue( "", "arch" ) != null ) {
            jbinfo.setInfo( JobManager.ARCH_TYPE,
                new String( attrs.getValue( "", "arch" ) ) );
        }
        if ( attrs.getValue( "", "type" ) != null ) {
            jbinfo.setInfo( JobManager.JOBMANAGER_TYPE,
                new String( attrs.getValue( "", "type" ) ) );
        }
        pinfo.setInfo( SiteInfo.JOBMANAGER, jbinfo );
    }

    /**
     * Handles the WorkDirectory Tag Start.
     * @param pinfo Takes a SiteInfo object for which the work directory is.
     * @param attrs Takes the attributes returned from the XML by the parser.
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.SiteInfo
     */
    private  void handleWorkDirectoryTagStart( SiteInfo pinfo,
        Attributes attrs ) throws Exception {
        WorkDir gwd = new WorkDir();
        if ( attrs.getValue( "", "total-size" ) != null ) {
            gwd.setInfo( WorkDir.TOTAL_SIZE,
                new String( attrs.getValue( "", "total-size" ) ) );
        }
        if ( attrs.getValue( "", "free-size" ) != null ) {
            gwd.setInfo( WorkDir.FREE_SIZE,
                new String( attrs.getValue( "", "free-size" ) ) );
        }
        //pinfo.setInfo( WorkDir.WORKDIR, gwd );
        pinfo.setInfo(SiteInfo.WORKDIR,gwd);
    }

    /**
     * Handles the end of the Xml files.
     *
     */
    private  void handleConfigTagEnd() {
        // System.out.println(m_pconfig.toXml());
    }

    /**
     *  Handles the end of the pool tag.
     */
    private  void handlePoolTagEnd() {
        mSiteStore.addEntry( SiteInfo2SiteCatalogEntry.convert( this.m_pool_info ) );
    }

    /**
     * Handles the end of the Profile tag.
     * @param pinfo <code>PoolInfo</code> object for which the
     *              profiles are collected.
     *
     * @throws java.lang.Exception
     * @see org.griphyn.cPlanner.classes.SiteInfo
     */
    private void handleProfileTagEnd( SiteInfo pinfo ) throws RuntimeException {
        if ( mTextContent != null && m_namespace != null && m_key != null ) {

            //check if namespace is valid
            m_namespace = m_namespace.toLowerCase();
            if( !Namespace.isNamespaceValid( m_namespace ) ){
                mTextContent.setLength( 0 );
                mLogger.log("Namespace specified in Site Catalog not supported. ignoring "+ m_namespace,
                            LogManager.WARNING_MESSAGE_LEVEL);
                return;
            }



            Profile profile = new Profile( m_namespace, m_key,
                mTextContent.toString().trim() );
            pinfo.setInfo( SiteInfo.PROFILE, profile );
            mTextContent.setLength( 0 );
        }
    }

    /**
     * Handles the end of the LRC Tag
     */
    private static void handleLRCTagEnd() {
    }

    /**
     * sk made changes to the following function to set GRIDFTPServer instead of
     * setting it in fn handleGridFtpTagStart()
     * @throws java.lang.RuntimeException
     */
    private void handleGridFtpTagEnd() throws      RuntimeException {
        m_pool_info.setInfo( SiteInfo.GRIDFTP, gftp );
    }

    private static void handleGridFtpBandwidthTagEnd() {
    }

    /**
     * Handles the end of the JobManager Tag
     */
    private static void handleJobManagerTagEnd() {
    }

    /**
     * This method handles the Workdirectory tg end.
     * @param pinfo Takes the PoolInfo object.
     * @throws java.lang.Exception
     * @see org.griphyn.cPlanner.classes.SiteInfo
     */
    private void handleWorkDirectoryTagEnd( SiteInfo pinfo ) throws RuntimeException {
        if ( mTextContent != null ) {
            WorkDir gdw = ( WorkDir ) pinfo.getInfo(
                SiteInfo.WORKDIR );
            gdw.setInfo( WorkDir.WORKDIR,
                mTextContent.toString().trim() );
        }
        mTextContent.setLength( 0 );
    }

    /**
     * This class returns  the reference to the <code>PooConfig</code> object
     * containing information about all the pools.
     *
     * @return returns a reference to the <code>PoolConfig</code> object which
     *         contains all the pools.
     *
     * @see org.griphyn.cPlanner.classes.PoolConfig
     */
    public PoolConfig getPoolConfig() {
        return m_pconfig;
    }

    
}
