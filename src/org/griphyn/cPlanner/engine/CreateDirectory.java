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

package org.griphyn.cPlanner.engine;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.JobManager;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.UserOptions;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.util.Separator;
import org.griphyn.common.util.DynamicLoader;
import org.griphyn.common.util.FactoryException;

import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

import java.io.File;

/**
 * This common interface that identifies the basic functions that need to be
 * implemented to introduce random directories in which the jobs are executed on
 * the remote execution pools. The implementing classes are invoked when the user
 * gives the --randomdir option. The implementing classes determine where in the
 * graph the nodes creating the random directories are placed and their
 * dependencies with the rest of the nodes in the graph.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 */
public abstract class CreateDirectory
    extends Engine {

    /**
     * Constant suffix for the names of the create directory nodes.
     */
    public static final String CREATE_DIR_SUFFIX = "_cdir";

    /**
     * The name of the package in which all the implementing classes are.
     */
    public static final String PACKAGE_NAME = "org.griphyn.cPlanner.engine.";

    /**
     * The transformation namespace for the create dir jobs.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The logical name of the transformation that creates directories on the
     * remote execution pools.
     */
    public static final String TRANSFORMATION_NAME = "dirmanager";

    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The path to be set for create dir jobs.
     */
    public static final String PATH_VALUE = ".:/bin:/usr/bin:/usr/ucb/bin";

    /**
     * The complete TC name for kickstart.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                 TRANSFORMATION_NAMESPACE,
                                                                 TRANSFORMATION_NAME,
                                                                 TRANSFORMATION_VERSION  );


    /**
     * The derivation namespace for the create dir  jobs.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The logical name of the transformation that creates directories on the
     * remote execution pools.
     */
    public static final String DERIVATION_NAME = "dirmanager";


    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * It is a reference to the Concrete Dag so far.
     */
    protected ADag mCurrentDag;

    /**
     * The handle to the options specified by the user at runtime. The name of
     * the random directory is picked up from here.
     */
    protected UserOptions mUserOpts;

    /**
     * The handle to the logging object, that is used to log the messages.
     */
    protected LogManager mLogger;

    /**
     * The job prefix that needs to be applied to the job file basenames.
     */
    protected String mJobPrefix;

    /**
     * Whether we want to use dirmanager or mkdir directly.
     */
    protected boolean mUseMkdir;


    /**
     * A convenience method to return the complete transformation name being
     * used to construct jobs in this class.
     *
     * @return the complete transformation name
     */
//    public static String getCompleteTranformationName(){
//        return Separator.combine( TRANSFORMATION_NAMESPACE,
//                                  TRANSFORMATION_NAME,
//                                  TRANSFORMATION_VERSION );
//    }


    /**
     * Loads the implementing class corresponding to the mode specified by the
     * user at runtime.
     *
     * @param className  The name of the class that implements the mode. It is the
     *                   name of the class, not the complete name with package.
     *                   That is added by itself.
     * @param concDag        the workflow.
     * @param bag      bag of initialization objects
     *
     * @return instance of a CreateDirecctory implementation
     *
     * @throws FactoryException that nests any error that
     *         might occur during the instantiation of the implementation.
     */
    public static CreateDirectory loadCreateDirectoryInstance(
                                                              String className,
                                                              ADag concDag,
                                                              PegasusBag bag ) throws FactoryException {

        //prepend the package name
        className = PACKAGE_NAME + className;

        //try loading the class dynamically
        CreateDirectory cd = null;
        DynamicLoader dl = new DynamicLoader(className);
        try {
            Object argList[] = new Object[ 2 ];
            argList[0] = concDag;
            argList[1] = bag;
            cd = (CreateDirectory) dl.instantiate(argList);
        } catch (Exception e) {
            throw new FactoryException( "Instantiating Create Directory",
                                        className,
                                        e );
        }

        return cd;
    }

    /**
     * A pratically nothing constructor !
     *
     *
     * @param bag      bag of initialization objects
     */
    protected CreateDirectory( PegasusBag bag ) {
        super( bag.getPegasusProperties() );
        mJobPrefix  = bag.getPlannerOptions().getJobnamePrefix();
        mCurrentDag = null;
        mUserOpts = UserOptions.getInstance();
        mTCHandle = bag.getHandleToTransformationCatalog();
        mLogger   = bag.getLogger();
        //in case of staging of executables/worker package
        //we use mkdir directly
        mUseMkdir = bag.getHandleToTransformationMapper().isStageableMapper();
    }


    /**
     * Default constructor.
     *
     * @param concDag  The concrete dag so far.
     * @param bag      bag of initialization objects
     */
    protected CreateDirectory( ADag concDag, PegasusBag bag ) {
        this( bag );
        mCurrentDag = concDag;
    }

    /**
     * It modifies the concrete dag passed in the constructor and adds the create
     * random directory nodes to it at the root level. These directory nodes have
     * a common child that acts as a concatenating job and ensures that Condor
     * does not start staging in the data before the directories have been added.
     * The root nodes in the unmodified dag are now chidren of this concatenating
     * dummy job.
     */
    public abstract void addCreateDirectoryNodes();

    /**
     * It returns the name of the create directory job, that is to be assigned.
     * The name takes into account the workflow name while constructing it, as
     * that is thing that can guarentee uniqueness of name in case of deferred
     * planning.
     *
     * @param pool  the execution pool for which the create directory job
     *                  is responsible.
     *
     * @return String corresponding to the name of the job.
     */
    protected String getCreateDirJobName(String pool){
        StringBuffer sb = new StringBuffer();


        sb.append(mCurrentDag.dagInfo.nameOfADag).append("_").
           append(mCurrentDag.dagInfo.index).append("_");

       //append the job prefix if specified in options at runtime
       if ( mJobPrefix != null ) { sb.append( mJobPrefix ); }

       sb.append(pool).append(this.CREATE_DIR_SUFFIX);

       return sb.toString();
    }

    /**
     * Retrieves the sites for which the create dir jobs need to be created.
     * It returns all the sites where the compute jobs have been scheduled.
     *
     *
     * @return  a Set containing a list of siteID's of the sites where the
     *          dag has to be run.
     */
    protected Set getCreateDirSites(){
        Set set = new HashSet();

        for(Iterator it = mCurrentDag.vJobSubInfos.iterator();it.hasNext();){
            SubInfo job = (SubInfo)it.next();
            //add to the set only if the job is
            //being run in the work directory
            //this takes care of local site create dir
            if(job.runInWorkDirectory()){
                set.add(job.executionPool);
            }
        }

        //remove the stork pool
        set.remove("stork");

        return set;
    }



    /**
     * It creates a make directory job that creates a directory on the remote pool
     * using the perl executable that Gaurang wrote. It access mkdir underneath.
     * It gets the name of the random directory from the Pool handle. This method
     * does not update the internal graph structure of the workflow to add the
     * node. That is done separately.
     *
     * @param execPool  the execution pool for which the create dir job is to be
     *                  created.
     * @param jobName   the name that is to be assigned to the job.
     *
     * @return create dir job.
     */
    protected SubInfo makeCreateDirJob(String execPool, String jobName) {
        SubInfo newJob  = new SubInfo();
        List entries    = null;
        String execPath = null;
        TransformationCatalogEntry entry   = null;
        JobManager jobManager = null;

        try {
            entries = mTCHandle.getTCEntries( CreateDirectory.TRANSFORMATION_NAMESPACE,
                                              CreateDirectory.TRANSFORMATION_NAME,
                                              CreateDirectory.TRANSFORMATION_VERSION,
                                              execPool, TCType.INSTALLED);
        }
        catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entries from TC " +
                        e.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL );
        }

        entry = ( entries == null ) ?
            this.defaultTCEntry( execPool ): //try using a default one
            (TransformationCatalogEntry) entries.get(0);

        if( entry == null ){
            //NOW THROWN AN EXCEPTION

            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ").
                append( COMPLETE_TRANSFORMATION_NAME ).
                append(" at site ").append( execPool );

            mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( error.toString() );
        }



        SiteInfo ePool = mPoolHandle.getPoolEntry(execPool, "transfer");
        jobManager = ePool.selectJobManager("transfer",true);

        String argString = null;
        if( mUseMkdir ){
            /*
            //we are using mkdir directly
            argString = " -p " + mPoolHandle.getExecPoolWorkDir( execPool );
            execPath  = "mkdir";
            //path variable needs to be set
            newJob.envVariables.construct( "PATH", CreateDirectory.PATH_VALUE );
            */
            newJob.vdsNS.construct( VDS.GRIDSTART_KEY, "None" );

            StringBuffer sb = new StringBuffer();
            sb.append( mProps.getPegasusHome() ).append( File.separator ).append( "bin" ).
               append( File.separator ).append( "dirmanager" );
            execPath = sb.toString();
            argString = "--create --dir " +
                        mPoolHandle.getExecPoolWorkDir( execPool );
            newJob.condorVariables.construct( "transfer_executable", "true" );
        }
        else{
            execPath = entry.getPhysicalTransformation();
            argString = "--create --dir " +
                        mPoolHandle.getExecPoolWorkDir( execPool );
        }

        newJob.jobName = jobName;
        newJob.setTransformation( CreateDirectory.TRANSFORMATION_NAMESPACE,
                                  CreateDirectory.TRANSFORMATION_NAME,
                                  CreateDirectory.TRANSFORMATION_VERSION );
        newJob.setDerivation( CreateDirectory.DERIVATION_NAMESPACE,
                              CreateDirectory.DERIVATION_NAME,
                              CreateDirectory.DERIVATION_VERSION );
        newJob.condorUniverse = "vanilla";
        newJob.globusScheduler = jobManager.getInfo(JobManager.URL);
        newJob.executable = execPath;
        newJob.executionPool = execPool;
        newJob.strargs = argString;
        newJob.jobClass = SubInfo.CREATE_DIR_JOB;
        newJob.jobID = jobName;

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        newJob.updateProfiles(mPoolHandle.getPoolProfile(newJob.executionPool));

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
     * @param site   the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    private  TransformationCatalogEntry defaultTCEntry( String site ){
        TransformationCatalogEntry defaultTCEntry = null;
        //check if PEGASUS_HOME is set
        String home = mPoolHandle.getPegasusHome( site );
        //if PEGASUS_HOME is not set, use VDS_HOME
        home = ( home == null )? mPoolHandle.getVDS_HOME( site ): home;

        mLogger.log( "Creating a default TC entry for " +
                     COMPLETE_TRANSFORMATION_NAME +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //if home is still null
        if ( home == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for " +
                         COMPLETE_TRANSFORMATION_NAME,
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
            append( this.TRANSFORMATION_NAME );


        defaultTCEntry = new TransformationCatalogEntry( this.TRANSFORMATION_NAMESPACE,
                                                         this.TRANSFORMATION_NAME,
                                                         this.TRANSFORMATION_VERSION );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site );
        defaultTCEntry.setType( TCType.INSTALLED );

        //register back into the transformation catalog
        //so that we do not need to worry about creating it again
        try{
            mTCHandle.addTCEntry( defaultTCEntry , false );
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
