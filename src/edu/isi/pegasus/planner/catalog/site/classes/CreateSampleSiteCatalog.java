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

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;

import java.io.FileWriter;
import edu.isi.pegasus.planner.classes.Profile;

import java.io.IOException;
        
/**
 * Generates a sample site catalog in XML.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CreateSampleSiteCatalog {

    /**
     * Generates a sample site catalog.
     * 
     * @param file   the path to the file to be written out.
     */
    public void constructSiteCatalog( String file ) {
        try{
            SiteStore store = new SiteStore();
            SiteCatalogEntry3 entry = new SiteCatalogEntry3( "isi_viz" );
            entry.setHeadNodeFS( createHeadNodeFS() );
            entry.setWorkerNodeFS( createWorkerNodeFS() );
            
            //associate a replica catalog with the site.
            ReplicaCatalog rc = new ReplicaCatalog( "rls://replica.isi.edu", "RLS" );
            rc.addAlias( "isi_skynet" );
            rc.addConnection( new Connection("ignore.lrc", "rls://replica.caltech.edu" ));            
            entry.addReplicaCatalog( rc );
            
            //associate some profiles
            entry.addProfile( new Profile( Profile.ENV, "JAVA_HOME", "/java/") );
            entry.addProfile( new Profile( Profile.VDS, "style", "gt2"));
            
            //associate grid gateway for auxillary and compute jobs
            GridGateway gw = new GridGateway( GridGateway.TYPE.gt2,
                                              "cluster.isi.edu/jobmanager-fork",
                                              GridGateway.SCHEDULER_TYPE.fork );
            gw.setJobType( GridGateway.JOB_TYPE.auxillary );
            entry.addGridGateway( gw );
            
            gw = new GridGateway( GridGateway.TYPE.gt2,
                                              "cluster.isi.edu/jobmanager-pbs",
                                              GridGateway.SCHEDULER_TYPE.pbs );
            gw.setJobType( GridGateway.JOB_TYPE.compute );
            entry.addGridGateway( gw );
            
            //add entry to site store
            store.addEntry( Adapter.convert(entry) );
            
            //write DAX to file
            FileWriter scFw = new FileWriter( file );
            System.out.println( "Writing out sample site catalog to " + file );
            store.toXML( scFw, "" );
            scFw.close();

            //test the clone method also
            System.out.println( store.clone() );
            
        }
        catch( IOException ioe ){
            ioe.printStackTrace();
        }
    }
    
    /**
     * Creates an object describing the head node filesystem.
     * 
     * @return the HeadNodeFS
     */
    public HeadNodeFS createHeadNodeFS(){
        // describe the head node filesystem
        HeadNodeFS hfs = new HeadNodeFS();
            
        //head node scratch description start
        HeadNodeScratch hscratch = new HeadNodeScratch();            
        
        //head node local scratch description
        LocalDirectory hscratchLocal = new LocalDirectory();
        FileServer f = new FileServer( "gsiftp", "gsiftp://hserver1.isi.edu", "/external/local" );
        hscratchLocal.addFileServer( f );
        f = new FileServer( "gsiftp", "gsiftp://hserver2.isi.edu", "/external/h2-local" );
        hscratchLocal.addFileServer( f );
        hscratchLocal.setInternalMountPoint( new InternalMountPoint( "/local", "50G", "100G") );
        
        //head node shared scratch description
        SharedDirectory hscratchShared = new SharedDirectory();            
        f = new FileServer( "gsiftp", "gsiftp://hserver1.isi.edu", "/external/shared-scratch" );
        f.addProfile( new Profile( Profile.VDS, "transfer.arguments", "-s -a"));                    
        hscratchShared.addFileServer( f );
        hscratchShared.setInternalMountPoint( new InternalMountPoint( "/shared-scratch", "50G", "100G") );
        hscratch.setLocalDirectory( hscratchLocal );
        hscratch.setSharedDirectory( hscratchShared );
        //head node scratch description ends
            
        //head node storage description start
        HeadNodeStorage hstorage = new HeadNodeStorage();
            
        //head node local storage description            
        LocalDirectory hstorageLocal = new LocalDirectory();
        f = new FileServer( "gsiftp", "gsiftp://hserver1.isi.edu", "/external/local-storage" );
        hstorageLocal.addFileServer( f );
        hstorageLocal.setInternalMountPoint( new InternalMountPoint( "/local-storage", "30G", "100G") );
        //head node shared storage description
        SharedDirectory hstorageShared = new SharedDirectory();            
        f = new FileServer( "gsiftp", "gsiftp://hserver1.isi.edu", "/external/shared-storage" );
        f.addProfile( new Profile( Profile.VDS, "transfer.arguments", "-s -a"));                    
        hstorageShared.addFileServer( f );
        hstorageShared.setInternalMountPoint( new InternalMountPoint( "/shared-storage", "50G", "100G") );
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
     * @return the HeadNodeFS
     */
    public WorkerNodeFS createWorkerNodeFS(){
        // describe the head node filesystem
        WorkerNodeFS wfs = new WorkerNodeFS();
            
        //worker node scratch description start
        WorkerNodeScratch wscratch = new WorkerNodeScratch();            
        //worker node local scratch description
        LocalDirectory wscratchLocal = new LocalDirectory();
        FileServer f = new FileServer( "file", "file:///", "/tmp" );
        wscratchLocal.addFileServer( f );
        wscratchLocal.setInternalMountPoint( new InternalMountPoint( "/tmp", "50G", "100G") );
        //worker node shared scratch description
        SharedDirectory wscratchShared = new SharedDirectory();            
        f = new FileServer( "file", "file:///", "/external/shared-scratch" );
        wscratchShared.setInternalMountPoint( new InternalMountPoint( "/shared-scratch", "50G", "100G") );
        wscratch.setLocalDirectory( wscratchLocal );
        wscratch.setSharedDirectory( wscratchShared );
        //head node scratch description ends
            
        
        
        //worker node storage description start
        WorkerNodeStorage wstorage = new WorkerNodeStorage();            
        //worker node local scratch description
        LocalDirectory wstorageLocal = new LocalDirectory();
        f = new FileServer( "file", "file:///", "/tmp" );
        wstorageLocal.addFileServer( f );
        wstorageLocal.setInternalMountPoint( new InternalMountPoint( "/tmp", "50G", "100G") );
        
        //worker node shared scratch description
        SharedDirectory wstorageShared = new SharedDirectory();            
        f = new FileServer( "file", "file:///", "/external/shared-storage" );
        wstorageShared.setInternalMountPoint( new InternalMountPoint( "/shared-storage", "50G", "100G") );
        wstorage.setLocalDirectory( wstorageLocal );
        wstorage.setSharedDirectory( wstorageShared );
        //worker node scratch description ends
        //worker node storage description ends
            
        wfs.setScratch( wscratch );
        wfs.setStorage( wstorage );
            
        return wfs;
    }
    
    /**
     * The main program
     * 
     * @param args
     */
    public static void main( String[] args ){
       CreateSampleSiteCatalog csc = new CreateSampleSiteCatalog();
        if (args.length == 1) {
            csc.constructSiteCatalog(args[0]);
 
        } else {
            System.out.println("Usage: CreateSampleSiteCatalog <output site catalog file>");
        }
 
    }

    
}
