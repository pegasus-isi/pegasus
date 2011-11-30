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
package edu.isi.pegasus.planner.catalog.site.impl.myosg.util;


import edu.isi.pegasus.planner.classes.Profile;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.HeadNodeFS;
import edu.isi.pegasus.planner.catalog.site.classes.HeadNodeScratch;
import edu.isi.pegasus.planner.catalog.site.classes.HeadNodeStorage;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.LocalDirectory;
import edu.isi.pegasus.planner.catalog.site.classes.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SharedDirectory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.WorkerNodeFS;
import edu.isi.pegasus.planner.catalog.site.classes.WorkerNodeScratch;
import edu.isi.pegasus.planner.catalog.site.classes.WorkerNodeStorage;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway.SCHEDULER_TYPE;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.MYOSGSiteInfoFacade;

/**
 * Utility class for converting the Facade object to SiteCatalogEntry object
 * @author Pt
 *
 */
public class MYOSGSiteCatalogUtil {
	 private static LogManager mLogger = LogManagerFactory.loadSingletonInstance();	 
	
       
            
	   
	    public static String getGsiftp(MYOSGSiteInfoFacade sitInfo) {
	    	String host = sitInfo.getGsiftp_server();
	    	String port = sitInfo.getGsiftp_port();
	    	if(host == null || host.equals("")){
	    		mLogger.log( "Gridftp hostname missing in site "+ sitInfo.getShortname() +" .Using gatekeeper entry.",
	                     LogManager.CONFIG_MESSAGE_LEVEL);
	    		host = (sitInfo.getGatekeeper().split("/"))[0];
	    	}
	    	if(port == null || port.equals("")){
	    		mLogger.log( "Gridftp hostname missing in site " + sitInfo.getShortname() + " .Using default 2811.",
	                     LogManager.CONFIG_MESSAGE_LEVEL);
	    		port = "2811";
	    	}    	
	    	if(port.equals("2811")){
	    		return "gsiftp://" + host;
	    	}
	    	else{
	    		return "gsiftp://" + host + ":" + port;
	    	}        
	    }
            
            
            /**
             * Creates a Pegasus <code>SiteCatalogEntry</code> object from the information 
             * in VORS.
             * 
             * The following coventions are followed for determining the worker node
             * and storage node directories.
             * 
             * <pre>
             * head node shared -> data_loc
             * head node local -> tmp_loc
             * worker node shared ->data_loc
             * worker node local -> wntmp_loc
             * </pre>
             * 
             * @param sitInfo
             * @return SiteCatalogEntry object.
             */
	    public static SiteCatalogEntry createSiteCatalogEntry(MYOSGSiteInfoFacade sitInfo){
	    	mLogger.logEventStart(LoggingKeys.EVENT_PEGASUS_PARSE_SITE_CATALOG,
					"site-catalog.id", sitInfo.getShortname(), LogManager.DEBUG_MESSAGE_LEVEL);
	        SiteCatalogEntry entry = new SiteCatalogEntry( sitInfo.getShortname());
	        entry.setHeadNodeFS( createHeadNodeFS(sitInfo) );
	        entry.setWorkerNodeFS( createWorkerNodeFS(sitInfo) );
	        
	        //associate a replica catalog with the site.
	        ReplicaCatalog rc = new ReplicaCatalog( "rls://replica.isi.edu", "RLS" );
	        
	        rc.addAlias( sitInfo.getShortname());
	        //rc.addConnection( new Connection("ignore.lrc", "rls://replica.caltech.edu" ));            
	        entry.addReplicaCatalog( rc );
	        
	        //associate some profiles
	        
	        entry.addProfile( new Profile( Profile.ENV, "PEGASUS_HOME", ((sitInfo.getOsg_grid() != null)?sitInfo.getOsg_grid():"") +"/pegasus")) ;
	        entry.addProfile( new Profile( Profile.ENV, "app_loc",((sitInfo.getApp_loc() != null)?sitInfo.getApp_loc():"/")) );
	        entry.addProfile( new Profile( Profile.ENV, "data_loc", ((sitInfo.getData_loc() != null)?sitInfo.getData_loc():"/")) );
	        entry.addProfile( new Profile( Profile.ENV, "osg_grid", ((sitInfo.getOsg_grid() != null)?sitInfo.getOsg_grid():"/")) );
	        entry.addProfile( new Profile( Profile.ENV, "tmp_loc", ((sitInfo.getTmp_loc() != null)?sitInfo.getTmp_loc():"/")) );
	        entry.addProfile( new Profile( Profile.ENV, "wntmp_loc", ((sitInfo.getWntmp_loc() != null)?sitInfo.getWntmp_loc():"/")) );
	        //entry.addProfile( new Profile( Profile.VDS, "grid", ((sitInfo.getVoInfo().getGrid() != null)?sitInfo.getVoInfo().getGrid():"")) );
	        // TODO Check Pt
//	        entry.addProfile( new Profile( Profile.VDS, "grid", ((sitInfo.getGrid() != null)?sitInfo.getGrid():"")) );
	        
	        //associate grid gateway for auxillary and compute jobs
	      /*  GridGateway gw = new GridGateway( GridGateway.TYPE.gt2,
                                                  ((sitInfo.getUtil_jm() != null)?
                                                       sitInfo.getUtil_jm():
	        	        		      (sitInfo.getVoInfo().getGatekeeper().split(":"))[0] + "/jobmanager-fork"),
	        				  getSchedulerType(sitInfo.getUtil_jm()) );*/
	        GridGateway gw = new GridGateway( GridGateway.TYPE.gt2,
                    sitInfo.getUtil_jm(),
	      		getSchedulerType(sitInfo.getUtil_jm()) );
	        gw.setJobType( GridGateway.JOB_TYPE.auxillary );        
	        
	        entry.addGridGateway( gw );
                
                if( gw.getScheduler() == GridGateway.SCHEDULER_TYPE.Fork ){
                    //add the headnode globus location
                    entry.addProfile( new Profile( Profile.ENV, "GLOBUS_LOCATION", ((sitInfo.getGlobus_loc() != null)?sitInfo.getGlobus_loc():"/") ) );
                    entry.addProfile( new Profile( Profile.ENV, "LD_LIBRARY_PATH", ((sitInfo.getGlobus_loc() != null)?sitInfo.getGlobus_loc():"") + "/lib") );
                }
                else{
                    mLogger.log( "Creating globus location on basis of OSG_GRID for site " + entry.getSiteHandle() ,
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                    String wn = sitInfo.getOsg_grid();
                    String globus = ( wn == null )? "/globus" : wn + "/globus";
                    entry.addProfile( new Profile( Profile.ENV, "GLOBUS_LOCATION", globus ) );
                    entry.addProfile( new Profile( Profile.ENV, "LD_LIBRARY_PATH", globus + "/lib" ));
                }
	        
	       /* gw = new GridGateway( GridGateway.TYPE.gt2,        									
	        		      ((sitInfo.getExec_jm() != null)?
                                             sitInfo.getExec_jm():
                                             (sitInfo.getVoInfo().getGatekeeper().split(":"))[0] + "/jobmanager-fork"),
	        		      getSchedulerType(sitInfo.getExec_jm()) );*/
                gw = new GridGateway( GridGateway.TYPE.gt2,        									
	        		     sitInfo.getExec_jm(),
	        		      getSchedulerType(sitInfo.getExec_jm()) );
	        gw.setJobType( GridGateway.JOB_TYPE.compute );
	        entry.addGridGateway( gw );
			mLogger.logEventCompletion(LogManager.DEBUG_MESSAGE_LEVEL);
	        return entry;
	    }
            
            
            
	    private static SCHEDULER_TYPE getSchedulerType(String url) {
	    	if(url == null){
	    		return GridGateway.SCHEDULER_TYPE.Fork ;
	    	}
	        if( url.endsWith( "condor" ) ){
	            return  GridGateway.SCHEDULER_TYPE.Condor ;
	        }
	        else if( url.endsWith( "fork" ) ){
	            return GridGateway.SCHEDULER_TYPE.Fork ;
	        }
	        else if( url.endsWith( "pbs" ) ){
	            return GridGateway.SCHEDULER_TYPE.PBS ;
	        }
	        else if( url.endsWith( "lsf" ) ){
	            return GridGateway.SCHEDULER_TYPE.LSF ;
	        }
                else if( url.endsWith( "sge" ) ){
                    return GridGateway.SCHEDULER_TYPE.SGE;
                }
	        //if nothing is there than return fork
	        return GridGateway.SCHEDULER_TYPE.Fork ;
		}
            
            
            /**
             * Creates an object describing the head node filesystem.
             * 
             * The following conventions are followed.
             * <pre>
	     *	shared:
	     *	    scratch data_loc
	     *	    storage data_loc
	     *	local:   
	     *	    scratch tmp_loc
	     *	    storage tmp_loc
             * 
             * </pre>
             * 
	     * @return the HeadNodeFS
	     */
	    public static HeadNodeFS createHeadNodeFS(MYOSGSiteInfoFacade sitInfo){
	        // describe the head node filesystem
	        HeadNodeFS hfs = new HeadNodeFS();
	            
	        //head node scratch description start
	        HeadNodeScratch hscratch = new HeadNodeScratch();            
	        
	        //head node local scratch description
	        LocalDirectory hscratchLocal = new LocalDirectory();
                String directory = (sitInfo.getTmp_loc() != null)?sitInfo.getTmp_loc():"/";
	        FileServer f = new FileServer( "gsiftp", getGsiftp(sitInfo), directory );
	        hscratchLocal.addFileServer( f );      
                //no distinction between internal and external view
	        hscratchLocal.setInternalMountPoint( new InternalMountPoint( directory, "50G", "100G") );
	        
	        //head node shared scratch description
	        SharedDirectory hscratchShared = new SharedDirectory();            
                directory = (sitInfo.getData_loc() != null)?sitInfo.getData_loc():"/";
	        f = new FileServer( "gsiftp", getGsiftp(sitInfo), directory );                    
	        hscratchShared.addFileServer( f );
                //no distinction between internal and external view
	        hscratchShared.setInternalMountPoint( new InternalMountPoint( directory, "50G", "100G") );
	        hscratch.setLocalDirectory( hscratchLocal );
	        hscratch.setSharedDirectory( hscratchShared );
	        //head node scratch description ends
	            
	        //head node storage description start
	        HeadNodeStorage hstorage = new HeadNodeStorage();
	            
	        //head node local storage description            
	        LocalDirectory hstorageLocal = new LocalDirectory();
                directory =  (sitInfo.getTmp_loc() != null)?sitInfo.getTmp_loc():"/"  ;
	        f = new FileServer( "gsiftp", getGsiftp(sitInfo), directory );
	        hstorageLocal.addFileServer( f );
                //internal and external view is same
	        hstorageLocal.setInternalMountPoint( new InternalMountPoint( directory, "30G", "100G") );
	        
                //head node shared storage description
	        SharedDirectory hstorageShared = new SharedDirectory();    
                directory = (sitInfo.getData_loc() != null)?sitInfo.getData_loc():"/";
	        f = new FileServer( "gsiftp", getGsiftp(sitInfo), directory  );                 
	        hstorageShared.addFileServer( f );
                //no distinction between internal and external view
	        hstorageShared.setInternalMountPoint( new InternalMountPoint( directory, "50G", "100G") );
	        hstorage.setLocalDirectory( hstorageLocal );
	        hstorage.setSharedDirectory( hstorageShared );            
	        //head node storage description ends
	            
	        hfs.setScratch( hscratch );
	        hfs.setStorage( hstorage );
	            
	        return hfs;
	    }
	    
	    /**
	     * Creates an object describing the worker node filesystem.
	     * 
             * The following conventions are followed.
             * <pre>
             *  shared:
	     *	    scratch data
	     *	    storage data
	     *	local:   
	     *	    scratch wntmp
	     *	    storage wntmp
             * </pre>
             * 
	     * @return the WorkerNodeFS
	     *
             */
	    public static WorkerNodeFS createWorkerNodeFS(MYOSGSiteInfoFacade sitInfo){
	        // describe the head node filesystem
	        WorkerNodeFS wfs = new WorkerNodeFS();
	            
	        //worker node scratch description start
	        WorkerNodeScratch wscratch = new WorkerNodeScratch();            
	        //worker node local scratch description
	        LocalDirectory wscratchLocal = new LocalDirectory();
                String directory = (sitInfo.getWntmp_loc() != null)?sitInfo.getWntmp_loc():"/";
	        FileServer f = new FileServer( "file", "file:///", directory );
	        wscratchLocal.addFileServer( f );
                //no distinction between internal and external view
	        wscratchLocal.setInternalMountPoint( new InternalMountPoint( directory, "50G", "100G") );
                
	        //worker node shared scratch description
	        SharedDirectory wscratchShared = new SharedDirectory();
                directory = (sitInfo.getData_loc() != null)?sitInfo.getData_loc():"/";
	        f = new FileServer( "file", "file:///", directory );
                //no distinction between internal and external view
	        wscratchShared.setInternalMountPoint( new InternalMountPoint( directory, "50G", "100G") );
	        wscratch.setLocalDirectory( wscratchLocal );
	        wscratch.setSharedDirectory( wscratchShared );
	        //head node scratch description ends                    
	        
	        //worker node storage description start
	        WorkerNodeStorage wstorage = new WorkerNodeStorage();            
	        //worker node local scratch description
	        LocalDirectory wstorageLocal = new LocalDirectory();
                directory = (sitInfo.getWntmp_loc() != null)?sitInfo.getWntmp_loc():"/";
	        f = new FileServer( "file", "file:///", directory );
	        wstorageLocal.addFileServer( f );
                //no distinction between internal and external view
	        wstorageLocal.setInternalMountPoint( new InternalMountPoint( directory, "50G", "100G") );
	        
	        //worker node shared scratch description
	        SharedDirectory wstorageShared = new SharedDirectory();            
                directory = (sitInfo.getData_loc() != null)?sitInfo.getData_loc():"/" ;
	        f = new FileServer( "file", "file:///", directory );
                //no distinction between internal and external view
	        wstorageShared.setInternalMountPoint( new InternalMountPoint( directory, "50G", "100G") );
	        wstorage.setLocalDirectory( wstorageLocal );
	        wstorage.setSharedDirectory( wstorageShared );
	        //worker node scratch description ends
	        //worker node storage description ends
	            
	        wfs.setScratch( wscratch );
	        wfs.setStorage( wstorage );
	            
	        return wfs;
	    }	 	   
}
