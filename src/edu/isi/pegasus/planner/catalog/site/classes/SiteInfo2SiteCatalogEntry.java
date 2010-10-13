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

package edu.isi.pegasus.planner.catalog.site.classes;

import edu.isi.pegasus.planner.catalog.site.impl.old.classes.GridFTPServer;
import edu.isi.pegasus.planner.catalog.site.impl.old.classes.SiteInfo;
import edu.isi.pegasus.planner.catalog.site.impl.old.classes.JobManager;
import edu.isi.pegasus.planner.catalog.site.impl.old.classes.LRC;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LogManager;

import java.util.Iterator;
import java.util.List;


/**
 * 
 * An adapter class that converts SiteInfo object to SiteCatalogEntry object.
 *
 * @author Karan Vahi
 * @version  $Revision$
 */
public class SiteInfo2SiteCatalogEntry {
    
     
    /**
     * An adapter method that converts the <code>SiteInfo</code> object to 
     * <code>SiteCatalogEntry</code> object.
     * 
     * @param s  <code>SiteInfo</code> to be converted.
     * 
     * @return the converted <code>SiteCatalogEntry</code> object.
     */
    public static SiteCatalogEntry convert( SiteInfo s ) {
        return SiteInfo2SiteCatalogEntry.convert( s,  LogManagerFactory.loadSingletonInstance() );
    }

    /**
     * An adapter method that converts the <code>SiteInfo</code> object to 
     * <code>SiteCatalogEntry</code> object.
     * 
     * @param s  <code>SiteInfo</code> to be converted.
     * @param logger  the hande to the LogManager
     * 
     * @return the converted <code>SiteCatalogEntry</code> object.
     */
    public static SiteCatalogEntry convert( SiteInfo s, LogManager logger ) {
        SiteCatalogEntry site = new SiteCatalogEntry();
        
        /* set the handle */
        site.setSiteHandle( (String)s.getInfo( SiteInfo.HANDLE ) );
        
        VDSSysInfo sysinfo = ( VDSSysInfo )s.getInfo( SiteInfo.SYSINFO ) ;
        if( sysinfo !=null) {
            site.setVDSSysInfo( sysinfo );
        }
        
        // describe the head node filesystem
        HeadNodeFS hfs = new HeadNodeFS();
        
        /* set the work directory as shared scratch */       
        HeadNodeScratch hscratch = new HeadNodeScratch();    
        SharedDirectory hscratchShared = new SharedDirectory();            
        String workDir = s.getExecMountPoint();
        for ( Iterator it = ((List)s.getInfo( SiteInfo.GRIDFTP )).iterator(); it.hasNext(); ) {
            GridFTPServer g = (GridFTPServer) it.next();            
            hscratchShared.addFileServer( new FileServer( "gsiftp" , (String)g.getInfo( GridFTPServer.GRIDFTP_URL ), workDir ) );
        }
        hscratchShared.setInternalMountPoint( new InternalMountPoint( workDir ));        
        hscratch.setSharedDirectory( hscratchShared );
        hfs.setScratch( hscratch );
        
        /* set the storage directory as shared storage */       
        HeadNodeStorage hstorage = new HeadNodeStorage();    
        SharedDirectory hstorageShared = new SharedDirectory(); 
        String storageDir = null;
        for ( Iterator it = ((List)s.getInfo( SiteInfo.GRIDFTP )).iterator(); it.hasNext(); ) {
            GridFTPServer g = (GridFTPServer) it.next();            
            storageDir      = ( String )g.getInfo( GridFTPServer.STORAGE_DIR ) ;
            hstorageShared.addFileServer( new FileServer( "gsiftp" , 
                                                          (String)g.getInfo( GridFTPServer.GRIDFTP_URL ),
                                                          storageDir ) );
        }
        hstorageShared.setInternalMountPoint( new InternalMountPoint( storageDir ) );        
        hstorage.setSharedDirectory( hstorageShared );
        hfs.setStorage( hstorage );
        
        site.setHeadNodeFS( hfs );
        
        /* set the storage directory as GridGateways */
        for ( Iterator it = ((List)s.getInfo( SiteInfo.JOBMANAGER )).iterator(); it.hasNext(); ) {
            JobManager jm = (JobManager) it.next();
            GridGateway gw = new GridGateway();
            
            String universe = (String)jm.getInfo( JobManager.UNIVERSE );
            if( universe.equals( "vanilla" ) ){
                gw.setJobType( GridGateway.JOB_TYPE.compute );
            } 
            else if( universe.equals( "transfer" ) ){
                gw.setJobType( GridGateway.JOB_TYPE.auxillary );
            } 
            else{
                throw new RuntimeException( "Unknown universe type " + universe + " for site " + site.getSiteHandle() );
            }
            
            String url = (String)jm.getInfo( JobManager.URL );
            gw.setContact( url );
           
            if( url.endsWith( "condor" ) ){
                gw.setScheduler( GridGateway.SCHEDULER_TYPE.Condor );
            }
            else if( url.endsWith( "fork" ) ){
                gw.setScheduler( GridGateway.SCHEDULER_TYPE.Fork );
            }
            else if( url.endsWith( "pbs" ) ){
                gw.setScheduler( GridGateway.SCHEDULER_TYPE.PBS );
            }
            else if( url.endsWith( "lsf" ) ){
                gw.setScheduler( GridGateway.SCHEDULER_TYPE.LSF );
            }
            else if( url.endsWith( "sge" ) ){
                gw.setScheduler( GridGateway.SCHEDULER_TYPE.SGE);
            }

            gw.setIdleNodes( (String)jm.getInfo( JobManager.IDLE_NODES ) );            
            gw.setTotalNodes( (String)jm.getInfo( JobManager.TOTAL_NODES ) );
            
            site.addGridGateway( gw );
        }
        
        /* set the LRC as Replica Catalog */
        for( Iterator it = ((List)s.getInfo( SiteInfo.LRC )).iterator(); it.hasNext(); ) {
            LRC lrc =  (LRC) it.next();            
            ReplicaCatalog rc = new ReplicaCatalog( lrc.getURL() , "LRC" );
            site.addReplicaCatalog( rc );
        }
        
        /* add Profiles */
        for( Iterator it = ((List)s.getInfo( SiteInfo.PROFILE )).iterator(); it.hasNext(); ) {
            site.addProfile( (Profile) it.next() );
        }
        
        logger.log( "SiteCatalogEntry object created is " + site,
                    LogManager.DEBUG_MESSAGE_LEVEL );
        return site;
    }
    
}
