/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.common.util.Separator;

import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;

import java.io.File;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


/**
 * Ends up creating a cleanup dag that deletes the remote directories that
 * were created by the create dir jobs. The cleanup dag is generated in a
 * sub directory from the main directory containing the submit files of the
 * dag. The dag consists of independant jobs, with each job responsible for
 * deleting directory for a execution pool. The current way of generating the
 * dag is tied to the fact, that the directories in which a job are executed
 * is tied to the pool not the job itself.
 *
 * @author Karan Vahi
 * @version $Revision$
 * @see CreateDirectory
 */
public class RemoveDirectory extends Engine {

    /**
     * The scheme name for file url.
     */
    public static final String FILE_URL_SCHEME = "file:";


    /**
     * The prefix that is attached to the name of the dag for which the
     * cleanup Dag is being generated, to generate the name of the cleanup
     * Dag.
     */
    public static final String CLEANUP_DAG_PREFIX = "del_";

    /**
     * Constant suffix for the names of the remote directory nodes.
     */
    public static final String REMOVE_DIR_SUFFIX = "_rdir";

    /**
     * The logical name of the transformation that removes directories on the
     * remote execution pools.
     */
    public static final String TRANSFORMATION_NAME = "cleanup";
    
    
    /**
     * The basename of the pegasus dirmanager  executable.
     */
    public static final String REMOVE_DIR_EXECUTABLE_BASENAME = "pegasus-cleanup";

    /**
     * The transformation namespace for the create dir jobs.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for the create dir  jobs.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The logical name of the transformation that removes directories on the
     * remote execution pools.
     */
    public static final String DERIVATION_NAME = "cleanup";


    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String DERIVATION_VERSION = "1.0";


    /**
     * The concrete dag so far, for which the clean up dag needs to be generated.
     */
    private ADag mConcDag;


    /**
     *  Boolean indicating whether we need to transfer dirmanager from the submit
     * host.
     */
    private boolean mTransferFromSubmitHost;
    
    /**
     * A convenience method to return the complete transformation name being
     * used to construct jobs in this class.
     *
     * @return the complete transformation name
     */
    public static String getCompleteTranformationName(){
        return Separator.combine( TRANSFORMATION_NAMESPACE,
                                  TRANSFORMATION_NAME,
                                  TRANSFORMATION_VERSION );
    }

    /**
     * The submit directory for the workflow.
     */
    private  String mSubmitDirectory;


    /**
     * The overloaded constructor that sets the dag for which we have to
     * generated the cleanup dag for.
     *
     * @param concDag  the concrete dag for which cleanup is reqd.
     * @param bag      the bag of initialization objects
     * @param submitDirectory   the submit directory for the cleanup workflow
     */
    public RemoveDirectory( ADag concDag, PegasusBag bag, String submitDirectory ) {
        super( bag );
        mConcDag = concDag;
        mTransferFromSubmitHost = bag.getPegasusProperties().transferWorkerPackage();
        mSubmitDirectory = submitDirectory;

        //check to see if it exists
        File dir = new File( submitDirectory );
        if( !dir.exists() ){
            // does not exist, try to make it
            if ( ! dir.mkdirs() ) {

                //try to get around JVM bug. JIRA PM-91
                if( dir.getPath().endsWith( "." ) ){
                    //just try to create the parent directory
                    if( !dir.getParentFile().mkdirs() ){
                        throw new RuntimeException( "Unable to create  directory " +
                                       dir.getPath() );
                    }
                    return;
                }
                throw new RuntimeException( "Unable to create  directory " +
                                       dir.getPath() );
            }

        }
    }

    /**
     * Generates a cleanup DAG for the dag associated with the class. Creates a
     * cleanup node per remote pool. It looks at the ADAG, to determine the
     * sites at which the jobs in the dag have been scheduled.
     *
     * @return the cleanup DAG.
     */
    public ADag generateCleanUPDAG(  ){
        return this.generateCleanUPDAG( mConcDag );
    }


    /**
     * Generates a cleanup DAG for the dag object passed. Creates a cleanup
     * node per remote pool. It looks at the ADAG, to determine the sites at
     * which the jobs in the dag have been scheduled.
     *
     * @param dag  the dag for which cleanup dag needs to be generated.
     *
     * @return the cleanup DAG.
     * @see org.griphyn.cPlanner.classes.ADag#getExecutionSites()
     */
    public ADag generateCleanUPDAG(ADag dag  ){
        ADag cDAG = new ADag();
        cDAG.dagInfo.nameOfADag = this.CLEANUP_DAG_PREFIX + dag.dagInfo.nameOfADag;
        cDAG.dagInfo.index      = dag.dagInfo.index;
        cDAG.dagInfo.setDAXMTime( dag.getMTime() );

        Set pools = this.getCreateDirSites(dag);
        String pool    = null;
        String jobName = null;

        //remove the entry for the local pool
        //pools.remove("local");

        for(Iterator it = pools.iterator();it.hasNext();){
            pool    = (String)it.next();
            jobName = getRemoveDirJobName(dag,pool);
            cDAG.add(makeRemoveDirJob( pool,jobName ));
        }

        return cDAG;
    }

    /**
     * Retrieves the sites for which the create dir jobs need to be created.
     * It returns all the sites where the compute jobs have been scheduled.
     *
     * @param dag the workflow for which the sites have to be computed.
     *
     * @return  a Set containing a list of siteID's of the sites where the
     *          dag has to be run.
     */
    public Set getCreateDirSites(ADag dag){
        Set set = new HashSet();

        for(Iterator it = dag.vJobSubInfos.iterator();it.hasNext();){
            Job job = (Job)it.next();
            //add to the set only if the job is
            //being run in the work directory
            //this takes care of local site create dir
            if( job.runInWorkDirectory()){
                set.add( job.getStagingSiteHandle() );
            }
        }

        //remove the stork pool
        set.remove("stork");

        return set;
    }


    /**
     * It returns the name of the remove directory job, that is to be assigned.
     * The name takes into account the workflow name while constructing it, as
     * that is thing that can guarentee uniqueness of name in case of deferred
     * planning.
     *
     * @param dag   the dag for which the cleanup DAG is being generated.
     * @param pool  the execution pool for which the remove directory job
     *              is responsible.
     *
     * @return String corresponding to the name of the job.
     */
    private String getRemoveDirJobName(ADag dag,String pool){
        StringBuffer sb = new StringBuffer();
        sb.append(dag.dagInfo.nameOfADag).append("_").
           append(dag.dagInfo.index).append("_").
           append(pool).append(this.REMOVE_DIR_SUFFIX);

       return sb.toString();
    }


    /**
     * It creates a remove directory job that creates a directory on the remote pool
     * using the perl executable that Gaurang wrote. It access mkdir underneath.
     * It gets the name of the random directory from the Pool handle.
     *
     * @param site  the execution pool for which the create dir job is to be
     *                  created.
     * @param jobName   the name that is to be assigned to the job.
     *
     * @return the remove dir job.
     */
    public Job makeRemoveDirJob( String site, String jobName ) {
        
        List<String> l = new LinkedList<String>();

        //the externally accessible url to the directory/ workspace for the workflow
        l.add( mSiteStore.getExternalWorkDirectoryURL( site )  );
        return makeRemoveDirJob( site, jobName, l  );
    }
    
    /**
     * It creates a remove directory job that creates a directory on the remote pool
     * using the perl executable that Gaurang wrote. It access mkdir underneath.
     * It gets the name of the random directory from the Pool handle.
     *
     * @param site      the site from where the directory need to be removed.
     * @param jobName   the name that is to be assigned to the job.
     * @param files  the list of files to be cleaned up.
     *
     * @return the remove dir job.
     */
    public Job makeRemoveDirJob( String site, String jobName, List<String> files ) {
        Job newJob  = new Job();
        List entries    = null;
        String execPath = null;
        TransformationCatalogEntry entry   = null;



        //the site where the cleanup job will run
        String eSite = "local";

        for( String file: files ){
            if( file.startsWith( this.FILE_URL_SCHEME ) ){
                //means the cleanup job should run on the staging site
                mLogger.log( "Directory URL is a file url for site " + site + "  " +  files,
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                eSite = site;
            }
        }

        SiteCatalogEntry ePool = mSiteStore.lookup( eSite );

        try {
            entries = mTCHandle.lookup( RemoveDirectory.TRANSFORMATION_NAMESPACE,
                                              RemoveDirectory.TRANSFORMATION_NAME,
                                              RemoveDirectory.TRANSFORMATION_VERSION,
                                              eSite,
                                              TCType.INSTALLED);
        }
        catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entry from TC " + e.getMessage(),
                        LogManager.DEBUG_MESSAGE_LEVEL );
        }
        entry = ( entries == null ) ?
                     this.defaultTCEntry( ePool ): //try using a default one
                     (TransformationCatalogEntry) entries.get(0);


        if( entry == null ){
            //NOW THROWN AN EXCEPTION

            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ").
                  append( this.getCompleteTranformationName() ).
                  append(" at site ").append( eSite );

            mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( error.toString() );
        }
        if( mTransferFromSubmitHost ){
            /*
            //we are using mkdir directly
            argString = " -p " + mPoolHandle.getExecPoolWorkDir( execPool );
            execPath  = "mkdir";
            //path variable needs to be set
            newJob.envVariables.construct( "PATH", CreateDirectory.PATH_VALUE );
            */
            newJob.vdsNS.construct( Pegasus.GRIDSTART_KEY, "None" );

            StringBuffer sb = new StringBuffer();
            sb.append( mProps.getBinDir() ).
               append( File.separator ).append( RemoveDirectory.REMOVE_DIR_EXECUTABLE_BASENAME );
            execPath = sb.toString();
            newJob.condorVariables.construct( "transfer_executable", "true" );
        }
        else{
            execPath = entry.getPhysicalTransformation();
        }


        //prepare the stdin for the cleanup job
        String stdIn = jobName + ".in";
        try{
            BufferedWriter writer;
            writer = new BufferedWriter( new FileWriter(
                                        new File( mSubmitDirectory, stdIn ) ));

            for( String file: files ){
                writer.write( file );
                writer.write( "\n" );
            }

            //closing the handle to the writer
            writer.close();
        }
        catch(IOException e){
            mLogger.log( "While writing the stdIn file " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( "While writing the stdIn file " + stdIn, e );
        }

        //set the stdin file for the job
        newJob.setStdIn( stdIn );
        
        newJob.jobName = jobName;
        newJob.setTransformation( RemoveDirectory.TRANSFORMATION_NAMESPACE,
                                  RemoveDirectory.TRANSFORMATION_NAME,
                                  RemoveDirectory.TRANSFORMATION_VERSION );

        newJob.setDerivation( RemoveDirectory.DERIVATION_NAMESPACE,
                              RemoveDirectory.DERIVATION_NAME,
                              RemoveDirectory.DERIVATION_VERSION  );


        newJob.executable = execPath;
        newJob.setSiteHandle( eSite );
        newJob.jobClass = Job.CLEANUP_JOB;
        newJob.jobID = jobName;

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        newJob.updateProfiles( mSiteStore.lookup( newJob.getSiteHandle() ).getProfiles() );

        //add any notifications specified in the transformation
        //catalog for the job. JIRA PM-391
        newJob.addNotifications( entry );

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        newJob.updateProfiles(entry);

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        newJob.updateProfiles(mProps);


        return newJob;

    }



    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog.
     *
     * @param site   the SiteCatalogEntry for the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    private  TransformationCatalogEntry defaultTCEntry( SiteCatalogEntry site ){
        TransformationCatalogEntry defaultTCEntry = null;
        //check if PEGASUS_HOME is set
        String home = site.getPegasusHome();
        //if PEGASUS_HOME is not set, use VDS_HOME
        home = ( home == null )? site.getVDSHome( ): home;

        mLogger.log( "Creating a default TC entry for " +
                     RemoveDirectory.getCompleteTranformationName() +
                     " at site " + site.getSiteHandle(),
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //if home is still null
        if ( home == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for " +
                         RemoveDirectory.getCompleteTranformationName(),
                         LogManager.DEBUG_MESSAGE_LEVEL );
            //set the flag back to true
            return defaultTCEntry;
        }

        //remove trailing / if specified
        home = ( home.charAt( home.length() - 1 ) == File.separatorChar )?
            home.substring( 0, home.length() - 1 ):
            home;

        //construct the path to it
        StringBuffer path = new StringBuffer();
        path.append( home ).append( File.separator ).
            append( "bin" ).append( File.separator ).
            append( RemoveDirectory.REMOVE_DIR_EXECUTABLE_BASENAME );


        defaultTCEntry = new TransformationCatalogEntry( RemoveDirectory.TRANSFORMATION_NAMESPACE,
                                                         RemoveDirectory.TRANSFORMATION_NAME,
                                                         RemoveDirectory.TRANSFORMATION_VERSION );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site.getSiteHandle() );
        defaultTCEntry.setType( TCType.INSTALLED );
        defaultTCEntry.setSysInfo( site.getSysInfo() );

        //register back into the transformation catalog
        //so that we do not need to worry about creating it again
        try{
            mTCHandle.insert( defaultTCEntry , false );
        }
        catch( Exception e ){
            //just log as debug. as this is more of a performance improvement
            //than anything else
            mLogger.log( "Unable to register in the TC the default entry " +
                          defaultTCEntry.getLogicalTransformation() +
                          " for site " + site, e,
                          LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return defaultTCEntry;
    }



}
