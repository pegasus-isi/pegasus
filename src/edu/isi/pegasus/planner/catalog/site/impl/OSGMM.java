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

import edu.clemson.SiteCatalogGenerator;

import edu.isi.pegasus.common.logging.LogManagerFactory;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.catalog.SiteCatalog;

import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;

import edu.isi.pegasus.planner.catalog.site.classes.LocalSiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import edu.isi.pegasus.planner.catalog.site.classes.SiteInfo2SiteCatalogEntry;

import org.griphyn.cPlanner.common.StreamGobbler;
import org.griphyn.cPlanner.common.StreamGobblerCallback;


import org.griphyn.cPlanner.classes.GridFTPServer;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.JobManager;
import org.griphyn.cPlanner.classes.LRC;
import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.classes.WorkDir;

import edu.isi.pegasus.planner.catalog.transformation.classes.SysInfo;

import org.griphyn.common.util.Boolean;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;


/**
 * The OSGMM implementation of the Site Catalog interface.
 * This implementation also has a method to generate the SRM property mappings
 * to be used by Pegasus.
 *
 * The following pegasus properties are created for the sites that have the SRM
 * information available.
 *
 * <pre>
 * pegasus.transfer.srm.[sitename].service.url
 * pegasus.transfer.srm.[sitename].service.mountpoint
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class OSGMM implements SiteCatalog {
    
    /**
     * The property key without the pegasus prefix'es to get the condor collector host.
     */
    public static final String DEFAULT_CONDOR_COLLECTOR_PROPERTY_KEY = "osgmm.collector.host";
    
    /**
     * The pegasus property prefix.
     */
    public static final String PEGASUS_PROPERTY_PREFIX = "pegasus.transfer.srm";

    /**
     * The name of the ENGAGE VO
     */
    public static final String ENGAGE_VO = "engage";

    /**
     * The default condor collector to query to for non LIGO VO's
     */
    public static final String DEFAULT_CONDOR_COLLECTOR = "engage-central.renci.org";

    /**
     * The name of the LIGO VO
     */
    public static final String LIGO_VO = "ligo";

    /**
     * The default condor collector to query to for LIGO VO
     */
    public static final String DEFAULT_LIGO_CONDOR_COLLECTOR = "ligo-osgmm.renci.org";

    
    /**
     * The property key without the pegasus prefix'es to get the VO.
     */
    public static final String DEFAULT_VO_PROPERTY_KEY = "osgmm.vo";
    
    /**
     * The default VO to use to query the condor collector.
     */
    public static final String DEFAULT_VO = "ligo";
    
    /**
     * The property key without the pegasus prefix'es to get the grid.
     */
    public static final String DEFAULT_GRID_PROPERTY_KEY = "osgmm.grid";
    
    /**
     * The default Grid to retreive the sites for.
     */
    public static final String DEFAULT_GRID = "osg";
    
    
    /**
     * The property key without the pegasus prefix'es to get the VO.
     */
    public static final String DEFAULT_RETRIEVE_VALIDATED_SITES_PROPERTY_KEY = "osgmm.retrieve.validated.sites";
    
    /**
     * The default VO to use to query the condor collector.
     */
    public static final boolean DEFAULT_RETRIEVE_VALIDATED_SITES = true;
            
    /**
     * An adapter method that converts the Site object to the SiteInfo object
     * corresponding to the site catalog schema version 2.
     * 
     * @param s  the Site object to convert.
     * 
     * @return  the coverted SiteInfo object
     */
    private static SiteInfo convertToSiteInfo( SiteCatalogGenerator.Site s ) throws Exception{
        SiteInfo site = new SiteInfo();
        site.setInfo( SiteInfo.HANDLE, s.siteName );
        site.setInfo( SiteInfo.GRIDLAUNCH, s.gridlaunch );
        site.setInfo( SiteInfo.SYSINFO, new SysInfo( s.sysinfo ));
        site.setInfo( SiteInfo.LRC, new LRC(s.lrcUrl) );
        
        //fork jobmanager
        JobManager forkJM = new JobManager( );
        forkJM.setInfo( JobManager.UNIVERSE, JobManager.FORK_JOBMANAGER_TYPE );
        forkJM.setInfo( JobManager.URL, s.transferUniverseJobManager );
        site.setInfo( SiteInfo.JOBMANAGER,  forkJM );
        
        //compute jobmanager
        JobManager computeJM = new JobManager( );
        computeJM.setInfo( JobManager.UNIVERSE, JobManager.VANILLA_JOBMANAGER_TYPE );
        computeJM.setInfo( JobManager.URL, s.VanillaUniverseJobManager );
        site.setInfo( SiteInfo.JOBMANAGER,  computeJM );
        
        
        //set the gridftp server
        GridFTPServer server = new GridFTPServer();
        server.setInfo( GridFTPServer.GRIDFTP_URL, s.gridFtpUrl );
        server.setInfo( GridFTPServer.STORAGE_DIR, s.gridFtpStorage );
        site.setInfo( SiteInfo.GRIDFTP, server );
        
        //set the environment profiles
        if( s.app != null ){
            site.setInfo( SiteInfo.PROFILE, new Profile( Profile.ENV, "app", s.app ) );
        }
        if( s.data != null ){
            site.setInfo( SiteInfo.PROFILE, new Profile( Profile.ENV, "data", s.data ) );
        }
        if( s.tmp != null ){
            site.setInfo( SiteInfo.PROFILE, new Profile( Profile.ENV, "tmp", s.tmp ) );
        }
        if( s.wntmp != null ){
            site.setInfo( SiteInfo.PROFILE, new Profile( Profile.ENV, "wntmp", s.wntmp ) );
        }
        if( s.globusLocation != null ){
            site.setInfo( SiteInfo.PROFILE, new Profile( Profile.ENV, "GLOBUS_LOCATION", s.globusLocation ) );
            site.setInfo( SiteInfo.PROFILE, new Profile( Profile.ENV, 
                                                         "LD_LIBRARY_PATH", 
                                                         s.globusLocation + File.separator + "lib" ) );
        }
        
        //set the working directory
        WorkDir dir = new WorkDir();
        dir.setInfo( WorkDir.WORKDIR, s.workingDirectory );
        site.setInfo( SiteInfo.WORKDIR, dir );
        return site;
    }
    
    /**
     * The List storing the output of condor-status.
     */
    List<String> mCondorStatusOutput;
    
    
    /**
     * The List storing the stderr of condor-status.
     */
    List<String> mCondorStatusError;
    
    /**
     * The SiteStore object where information about the sites is stored.
     */
    private SiteStore mSiteStore;
    
    /**
     * The handle to the log manager.
     */
    private LogManager mLogger;
    
    /**
     * The VO to which the user belongs to.
     */
    private String mVO;

    /**
     * The collector host to query to.
     */
    private String mCollectorHost;
    
    /**
     * The grid to which the user belongs to.
     */
    private String mGrid;


    /**
     * The default constructor.
     */
    public OSGMM() {
        mLogger = LogManagerFactory.loadSingletonInstance();
        mSiteStore = new SiteStore();
        mVO        = OSGMM.DEFAULT_VO;
        mGrid      = OSGMM.DEFAULT_GRID;
    }

    /* (non-Javadoc)
     * @see edu.isi.pegasus.planner.catalog.SiteCatalog#insert(edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry)
     */
    public int insert(SiteCatalogEntry entry) throws SiteCatalogException {
        mSiteStore.addEntry(entry);
        return 1;
    }

    /* (non-Javadoc)
     * @see edu.isi.pegasus.planner.catalog.SiteCatalog#list()
     */
    public Set<String> list() throws SiteCatalogException {
        return mSiteStore.list();
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
        
        // TODO: these should come from either the command line or a config file
        SiteCatalogGenerator sg = new SiteCatalogGenerator( (ArrayList)mCondorStatusOutput );
        List<SiteCatalogGenerator.Site> sgSites = sg.loadSites( sites, mVO );
        
        int result = 0;
        for( SiteCatalogGenerator.Site s : sgSites ){
            SiteInfo site;
            try {
                //some sanity check before attempting to convert
                if ( s.globusLocation == null || s.VanillaUniverseJobManager == null) {
                    mLogger.log( "Skipping site " + s.siteName, LogManager.INFO_MESSAGE_LEVEL );
                    continue;
                }
                
                mLogger.log( "Adding site " + s.siteName, LogManager.INFO_MESSAGE_LEVEL );
                site = OSGMM.convertToSiteInfo(s);
                mSiteStore.addEntry( SiteInfo2SiteCatalogEntry.convert( site, mLogger ) );
                result++;
            } catch (Exception ex) {
                mLogger.log( " While converting Site object for site " + s.siteName, 
                              ex,
                              LogManager.ERROR_MESSAGE_LEVEL );
            }
        }
        
        //always add local site.
        mLogger.log( "Site LOCAL . Creating default entry" , LogManager.INFO_MESSAGE_LEVEL );
        mSiteStore.addEntry( LocalSiteCatalogEntry.create( mVO, mGrid ) );
        result++;
        
        return result;
    }
    
    /* (non-Javadoc)
     * @see edu.isi.pegasus.planner.catalog.SiteCatalog#lookup(java.lang.String)
     */
    public SiteCatalogEntry lookup(String handle) throws SiteCatalogException {
        return mSiteStore.lookup(handle);
    }

    /* (non-Javadoc)
     * @see edu.isi.pegasus.planner.catalog.SiteCatalog#remove(java.lang.String)
     */
    public int remove(String handle) throws SiteCatalogException {
        throw new UnsupportedOperationException("Method remove( String , String ) not yet implmented");
    }

    
    /**
     * Closes the connection. It resets the internal buffers that contain output
     * of the condor_status command.
     */
    public void close() {
        mCondorStatusOutput = null;
        mCondorStatusError  = null;
    }

    /**
     * Issues the condor status command, and stores the results retrieved back
     * into a List.
     * 
     * @param props is the property table with sufficient settings to
     *              to connect to the implementation.
     * 
     * @return true if connected, false if failed to connect.
     *
     * @throws SiteCatalogException
     */
    public boolean connect(Properties props) throws SiteCatalogException {
        Runtime r = Runtime.getRuntime();
        ListCallback ic = new ListCallback();
        ListCallback ec = new ListCallback();

        mLogger.log( "Properties passed at connection " + props , LogManager.DEBUG_MESSAGE_LEVEL );
        
        // TODO: these should come from either the command line or a config file
        //String collectorHost = "engage-central.renci.org";
        mCollectorHost = props.getProperty( OSGMM.DEFAULT_CONDOR_COLLECTOR_PROPERTY_KEY );

        mVO = props.getProperty( OSGMM.DEFAULT_VO_PROPERTY_KEY,
                                       OSGMM.DEFAULT_VO  ).toLowerCase();

        if( mCollectorHost == null){
            //user did not specify in the properties.
            //assign a collector host on basis of VO.
            if( mVO.equals( OSGMM.LIGO_VO ) ){
                mCollectorHost = OSGMM.DEFAULT_LIGO_CONDOR_COLLECTOR;
            }
            else{
                 mCollectorHost =  OSGMM.DEFAULT_CONDOR_COLLECTOR;
            }
        }
        mGrid = props.getProperty( OSGMM.DEFAULT_GRID_PROPERTY_KEY, OSGMM.DEFAULT_GRID );
        boolean onlyOSGMMValidatedSites = Boolean.parse( props.getProperty( OSGMM.DEFAULT_RETRIEVE_VALIDATED_SITES_PROPERTY_KEY),
                                                         OSGMM.DEFAULT_RETRIEVE_VALIDATED_SITES  );
        
        mLogger.log( "The Condor Collector Host is " + mCollectorHost,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.log( "The User specified VO is " + mVO,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.log( "Retrieve only validated sites " + onlyOSGMMValidatedSites,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        String voToQueryFor = mVO;
        //if the collector is the default collector
        //then vo to query for is always engage 
        if( mCollectorHost.equals( OSGMM.DEFAULT_CONDOR_COLLECTOR )  ){
            voToQueryFor = OSGMM.ENGAGE_VO;
        }
        mLogger.log( "The condor collector will be queried for VO " + voToQueryFor, LogManager.DEBUG_MESSAGE_LEVEL );

        String constraint = "StringlistIMember(\"VO:" + voToQueryFor + "\";GlueCEAccessControlBaseRule)";
        if (onlyOSGMMValidatedSites) {
            constraint += " && SiteVerified==True";
        }
        
        String condorStatusCmd[] = {"condor_status", "-any",  "-pool", mCollectorHost,
                                    "-constraint", constraint,
                                    "-format", "%s", "GlueSiteName",
                                    "-format", ";", "1",  // to force a semicolon, even if the attribute was not found
                                    "-format", "%s", "GlueClusterUniqueID",
                                    "-format", ";", "1",
                                    "-format", "%s", "OSGMM_Globus_Location_Fork",
                                    "-format", ";", "1",
                                    "-format", "%s", "GlueCEInfoContactString",
                                    "-format", ";", "1",
                                    "-format", "%s", "GlueClusterTmpDir",
                                    "-format", ";", "1",
                                    "-format", "%s", "GlueCEInfoHostName",
                                    "-format", ";", "1",
                                    "-format", "%s", "GlueCEInfoApplicationDir",
                                    "-format", ";", "1",
                                    "-format", "%s", "GlueCEInfoDataDir",
                                    "-format", ";", "1",
                                    "-format", "%s", "GlueClusterTmpDir",
                                    "-format", ";", "1",
                                    "-format", "%s", "GlueClusterWNTmpDir",
                                    "-format", ";\\n", "1"};

        String cmdPretty = "";
        for(int i=0; i < condorStatusCmd.length; i++)
        {
            cmdPretty += condorStatusCmd[i] + " ";
        }
        
        try{
            mLogger.log( "condor_status command  is \n " + cmdPretty,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            Process p = r.exec( condorStatusCmd );

            //spawn off the gobblers
            StreamGobbler ips = new StreamGobbler( p.getInputStream(), ic );
            StreamGobbler eps = new StreamGobbler( p.getErrorStream(), ec );
            
            ips.start();
            eps.start();

            //wait for the threads to finish off
            ips.join();
            mCondorStatusOutput = ic.getContents();
            eps.join();
            mCondorStatusError  = ec.getContents();

            //get the status
            int status = p.waitFor();
            if( status != 0){
                mLogger.log("condor_status command  exited with status " + status,
                            LogManager.WARNING_MESSAGE_LEVEL);
                //also dump the stderr
                mLogger.log( "stderr for command invocation " + mCondorStatusError,
                            LogManager.ERROR_MESSAGE_LEVEL );
            }
        }
        catch(IOException ioe){
            mLogger.log( "IOException while calling out to condor_status. Probably" +
                         " condor-status not in path.", ioe,
                        LogManager.ERROR_MESSAGE_LEVEL);
            //also dump the stderr
            mLogger.log( "stderr for command invocation " + mCondorStatusError,
                         LogManager.ERROR_MESSAGE_LEVEL );
            return false;
        }
        catch( InterruptedException ie){
            //ignore
        }
        return true;
    }

    /**
     * Generates SRM properties that can be used by Pegasus to do SRM URL
     * substitution for the case where all the data is accessible on the
     * worker nodes locally.
     *
     * @return Properties object containing the relevant Properties.
     */
    public Properties generateSRMProperties( ){
        Properties result = new Properties( );

        if( this.isClosed() ){
            throw new SiteCatalogException( "Need to connect to site catalog before properties can be generated" );
        }


        String constraint = "regexp(\"file://\",GlueSEAccessProtocolEndpoint) && GlueSAPath=!=UNDEFINED && GlueSEControlProtocolEndpoint=!=UNDEFINED";


        //condor_status -l -pool ligo-osgmm.renci.org -constraint 'regexp("file://", GlueSEAccessProtocolEndpoint) && GlueSAPath =!= UNDEFINED && GlueSEControlProtocolEndpoint =!= UNDEFINED' -format %s GlueSiteName -format ";" 1  -format "srm://%s?SFN=" 'substr(GlueSEControlProtocolEndpoint, 8)' -format "%s"  'ifThenElse(GlueVOInfoPath =!= UNDEFINED, GlueVOInfoPath, GlueSAPath)' -format ";" 1 -format "%s" GlueSAPath -format ";" 1   -format "%s" GlueVOInfoPath -format ";" 1   -format "%s" GlueCESEBindMountInfo   -format ";\n" 1
        String condorStatusCmd[] = { "condor_status", "-pool", mCollectorHost,
                                     "-constraint", constraint,
                                     "-format", "%s", "GlueSiteName", //retrieve the site name
                                     "-format", ";", "1", // to force a semicolon, even if the attribute was not found
                                     "-format", "srm://%s?SFN=", "substr(GlueSEControlProtocolEndpoint, 8)",
                                     //"-format", "\";\"", "1", this is incorrect. java trips badly on this style
                                     "-format", "%s" , "ifThenElse(GlueVOInfoPath =!= UNDEFINED, GlueVOInfoPath, GlueSAPath)",
                                     "-format", ";", "1",
                                     "-format", "%s", "GlueSAPath",
                                     "-format", ";", "1",
                                     "-format", "%s", "GlueVOInfoPath",
                                     "-format", ";", "1",
                                     "-format", "%s", "GlueCESEBindMountInfo",
                                     "-format", ";\\n", "1" };

        String cmdPretty = "";
        for(int i=0; i < condorStatusCmd.length; i++)
        {
            cmdPretty += condorStatusCmd[i] + " ";
        }
        Runtime r = Runtime.getRuntime();
        ListCallback ic = new ListCallback();
        ListCallback ec = new ListCallback();

        try{
            mLogger.log( "condor_status command issued to retrieve SRM mappings is \n " + cmdPretty,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            Process p = r.exec( condorStatusCmd );

            //spawn off the gobblers
            StreamGobbler ips = new StreamGobbler( p.getInputStream(), ic );
            StreamGobbler eps = new StreamGobbler( p.getErrorStream(), ec );

            ips.start();
            eps.start();

            //wait for the threads to finish off
            ips.join();
            List<String> stdout = ic.getContents();
            eps.join();
            List<String> stderr = ec.getContents();

            //get the status
            int status = p.waitFor();

            if( status != 0){
                mLogger.log("condor_status command  exited with status " + status,
                            LogManager.WARNING_MESSAGE_LEVEL);
                //also dump the stderr
                mLogger.log( "stderr for command invocation " + ec.getContents(),
                            LogManager.ERROR_MESSAGE_LEVEL );
                throw new RuntimeException( "condor-status command exited with non zero status "  + status );
            }

            //System.out.println( "The Stdout is " + stdout );
            for( Iterator it = stdout.iterator(); it.hasNext(); ){
                //create properties for each site
                result.putAll( generateSRMProperties( (String)it.next() ) );
            }


        }
        catch(IOException ioe){
            mLogger.log( "IOException while calling out to condor_status. Probably" +
                         " condor-status not in path.", ioe,
                        LogManager.ERROR_MESSAGE_LEVEL);
            //also dump the stderr
            mLogger.log( "stderr for command invocation " + ec.getContents(),
                         LogManager.ERROR_MESSAGE_LEVEL );
            throw new RuntimeException( "IOException while invoking condor-status command", ioe );
        }
        catch( InterruptedException ie){
            //ignore
        }

        return result;
    }

    /**
     * Generates SRM properties that can be used by Pegasus to do SRM URL
     * substitution for the case where all the data is accessible on the
     * worker nodes locally for a particular site.
     * The condor status output for a single site site is passed as input.
     *
     * Example condor_status output for a site
     * <pre>
     * CIT_CMS_T2;srm://cit-se.ultralight.org:8443/srm/v2/server?SFN=/mnt/hadoop/osg;/mnt/hadoop/osg;/mnt/hadoop/osg;/mnt/hadoop,/mnt/hadoop;
     * </pre>
     *
     * The properties created have the following keys
     * <pre>
     * pegasus.transfer.srm.[sitename].service.url
     * pegasus.transfer.srm.[sitename].service.mountpoint
     * </pre>
     * where [sitename] is replaced by the name of the site.
     *
     * @param line   the line from condor_status output for a site.
     *
     * @return Properties object containing the relevant Properties.
     */
    public Properties generateSRMProperties( String line ){
        Properties result = new Properties();
        mLogger.log( "Line being worked on is " + line,
                     LogManager.DEBUG_MESSAGE_LEVEL );


        //split the line first
        String contents[]  = line.split(";");

        // do we have a valid site name?
        if (contents[0] == null || contents[0].equals("")) {
                return result;
        }
        String site = contents[0];

        
        //do another sanity check
        if( contents.length < 4 ){
            //ignore the length
            mLogger.log( "Ignoring line " + line,
                         LogManager.WARNING_MESSAGE_LEVEL );
            return result;
        }

        String srmURLPrefix = contents[ 1 ]; //the srm url prefix
        String glueSAPath = contents[ 2 ]; //the storage access path
        String glueVOPath = contents[ 3 ]; //the vo specific path . it is a subset of sa path
        //figure out the mount point
        String mountPoint = glueVOPath;
        
        if( contents.length == 5 ){
           String bindMountInfo = contents[ 4 ]; //tells how to get to the path on file system.

           //check if any replacement needs to be done
            contents = bindMountInfo.split( "," );
            if( contents.length == 2 ){
                //we have to do replacement
                //However we dont do any replacement for time being as it is incorrect.
                StringBuffer message = new StringBuffer();
                message.append( "Replacing " ).append( contents[0] ).append( " with ").
                        append( contents[1] ).append( " for site " ).append( site ).
                        append( " to get to the local filesystem path ");
                mLogger.log( message.toString(),  LogManager.DEBUG_MESSAGE_LEVEL );
                mountPoint = mountPoint.replace( contents[0], contents[1]);

            }
        }

        //some sanity check on the srmURLPrefix
        contents = srmURLPrefix.split( "," );
        if( contents.length > 1 ){
            //handling the following case
            //UFlorida-PG;srm://srmb.ihepa.ufl.edu:8443/srm/v2/server,httpg://srmb.ihepa.ufl.edu:8443/srm/v2/server?SFN=/lustre/raidl/user/ligo;/lustre/raidl/user/;/lustre/raidl/user/ligo;;
            mLogger.log( "Ignoring line " + line,
                         LogManager.WARNING_MESSAGE_LEVEL );
            return result;
        }

        //create the properties
        String key = createPropertyKey( site, "service.url");
        result.setProperty( key , srmURLPrefix );
        mLogger.log( "Created property " + key + " -> " + srmURLPrefix,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        key = createPropertyKey( site, "service.mountpoint");
        result.setProperty( key , mountPoint );
        mLogger.log( "Created property " + key + " -> " + mountPoint,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        return result;
    }

    /**
     * Creates the property key
     * 
     * @param site    the name of site
     * @param suffix  the suffix to be added to site.
     *
     * @return the property key.
     */
    private String createPropertyKey( String site, String suffix ){
        StringBuffer key = new StringBuffer();

        key.append( OSGMM.PEGASUS_PROPERTY_PREFIX ).append( "." ).append( site ).
            append( "." ).append( suffix );

        return key.toString();
    }

    /**
     * Returns if the connection is closed or not.
     * 
     * @return  boolean indicating connection is closed.
     */
    public boolean isClosed() {
        return ( mCondorStatusOutput == null );
    }

    /**
     * An inner class, that implements the StreamGobblerCallback to store all
     * the lines in a List
     *
     */
    private class ListCallback implements StreamGobblerCallback{

        
        /**
         * The ArrayList where the lines are stored.
         */
        List <String> mList;
        
        /**
         * Default Constructor.
         *
         */
        public ListCallback( ){
            mList = new ArrayList<String>();
        }

        /**
         * Callback whenever a line is read from the stream by the StreamGobbler.
         * Adds the line to the list.
         * 
         * @param line   the line that is read.
         */
        public void work( String line ){
            mList.add( line );
            
        }

        /**
         * Returns the contents captured.
         *
         * @return  List<String>
         */
        public List<String> getContents(){
            return mList;
        }

    }

}
