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

import org.griphyn.common.classes.SysInfo;

import org.griphyn.common.util.Boolean;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;


/**
 * The OSGMM implementation of the Site Catalog interface.
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
     * The default condor collector to query to
     */
    public static final String DEFAULT_CONDOR_COLLECTOR = "ligo-osgmm.renci.org";

    
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
     * The grid to which the user belongs to.
     */
    private String mGrid;

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
        String collectorHost = props.getProperty( OSGMM.DEFAULT_CONDOR_COLLECTOR_PROPERTY_KEY,
                                                  OSGMM.DEFAULT_CONDOR_COLLECTOR);
        mVO = props.getProperty( OSGMM.DEFAULT_VO_PROPERTY_KEY,
                                       OSGMM.DEFAULT_VO  ).toLowerCase();
        mGrid = props.getProperty( OSGMM.DEFAULT_GRID_PROPERTY_KEY, OSGMM.DEFAULT_GRID );
        boolean onlyOSGMMValidatedSites = Boolean.parse( props.getProperty( OSGMM.DEFAULT_RETRIEVE_VALIDATED_SITES_PROPERTY_KEY),
                                                         OSGMM.DEFAULT_RETRIEVE_VALIDATED_SITES  );
        
        mLogger.log( "The Condor Collector Host is " + collectorHost, 
                     LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.log( "The VO is " + mVO, 
                     LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.log( "Retrieve only validated sites " + onlyOSGMMValidatedSites,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        String constraint = "StringlistIMember(\"VO:" + mVO + "\";GlueCEAccessControlBaseRule)";
        if (onlyOSGMMValidatedSites) {
            constraint += " && SiteVerified==True";
        }
        
        String condorStatusCmd[] = {"condor_status", "-any",  "-pool", collectorHost,
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
            mLogger.log( "condor_status command is \n " + cmdPretty,
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
     * Returns if the connection is closed or not.
     * 
     * @return
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
