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

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import org.griphyn.common.util.Separator;

import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;

import java.io.File;
import org.griphyn.cPlanner.namespace.VDS;


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
    public static final String TRANSFORMATION_NAME = "dirmanager";

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
    public static final String DERIVATION_NAME = "dirmanager";


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
     * The overloaded constructor that sets the dag for which we have to
     * generated the cleanup dag for.
     *
     * @param concDag  the concrete dag for which cleanup is reqd.
     * @param bag      the bag of initialization objects
     */
    public RemoveDirectory( ADag concDag, PegasusBag bag ) {
        super( bag );
        mConcDag = concDag;
        mTransferFromSubmitHost = bag.getHandleToTransformationMapper().isStageableMapper();
    }

    /**
     * Generates a cleanup DAG for the dag associated with the class. Creates a
     * cleanup node per remote pool. It looks at the ADAG, to determine the
     * sites at which the jobs in the dag have been scheduled.
     *
     * @return the cleanup DAG.
     * @see org.griphyn.cPlanner.classes.ADag#getExecutionSites()
     */
    public ADag generateCleanUPDAG(){
        return this.generateCleanUPDAG(mConcDag);
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
    public ADag generateCleanUPDAG(ADag dag){
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
            cDAG.add(makeRemoveDirJob(pool,jobName));
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
    public SubInfo makeRemoveDirJob( String site, String jobName ) {
        List l = new LinkedList();
        l.add( mSiteStore.getWorkDirectory( site ) );
        
        return makeRemoveDirJob( site, jobName, l );
    }
    
    /**
     * It creates a remove directory job that creates a directory on the remote pool
     * using the perl executable that Gaurang wrote. It access mkdir underneath.
     * It gets the name of the random directory from the Pool handle.
     *
     * @param site      the site from where the files need to be removed.
     * @param jobName   the name that is to be assigned to the job.
     * @param files     the files to be removed
     *
     * @return the remove dir job.
     */
    public SubInfo makeRemoveDirJob( String site, String jobName, List files ) {
        SubInfo newJob  = new SubInfo();
        List entries    = null;
        String execPath = null;
        TransformationCatalogEntry entry   = null;
        GridGateway jm = null;

        try {
            entries = mTCHandle.getTCEntries( RemoveDirectory.TRANSFORMATION_NAMESPACE,
                                              RemoveDirectory.TRANSFORMATION_NAME,
                                              RemoveDirectory.TRANSFORMATION_VERSION,
                                              site,
                                              TCType.INSTALLED);
        }
        catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entry from TC " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
        }
        entry = ( entries == null ) ?
                     this.defaultTCEntry( site ): //try using a default one
                     (TransformationCatalogEntry) entries.get(0);


        if( entry == null ){
            //NOW THROWN AN EXCEPTION

            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ").
                  append( this.getCompleteTranformationName() ).
                  append(" at site ").append( site );

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
            newJob.vdsNS.construct( VDS.GRIDSTART_KEY, "None" );

            StringBuffer sb = new StringBuffer();
            sb.append( mProps.getPegasusHome() ).append( File.separator ).append( "bin" ).
               append( File.separator ).append( "dirmanager" );
            execPath = sb.toString();
            newJob.condorVariables.construct( "transfer_executable", "true" );
        }
        else{
            execPath = entry.getPhysicalTransformation();
        }

        SiteCatalogEntry ePool = mSiteStore.lookup( site );
        jm = ePool.selectGridGateway( GridGateway.JOB_TYPE.cleanup );

        StringBuffer arguments = new StringBuffer();
        arguments.append( "--verbose --remove --dir \"" );       
        for( Iterator it = files.iterator(); it.hasNext(); ){
            arguments.append( it.next() );
            //append only if there is another file
            if( it.hasNext()){
                arguments.append( " " );
            }
        }
        arguments.append( "\"" );
        
        newJob.jobName = jobName;
        newJob.setTransformation( RemoveDirectory.TRANSFORMATION_NAMESPACE,
                                  RemoveDirectory.TRANSFORMATION_NAME,
                                  RemoveDirectory.TRANSFORMATION_VERSION );

        newJob.setDerivation( RemoveDirectory.DERIVATION_NAMESPACE,
                              RemoveDirectory.DERIVATION_NAME,
                              RemoveDirectory.DERIVATION_VERSION  );

//        newJob.condorUniverse = "vanilla";
        
        
        newJob.setUniverse( GridGateway.JOB_TYPE.cleanup.toString() );
        newJob.globusScheduler = jm.getContact();
        newJob.executable = execPath;
        newJob.setSiteHandle( site );
        newJob.setArguments( arguments.toString() );
        newJob.jobClass = SubInfo.CREATE_DIR_JOB;
        newJob.jobID = jobName;

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        newJob.updateProfiles( mSiteStore.lookup( newJob.getSiteHandle() ).getProfiles() );

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
        String home = mSiteStore.getPegasusHome( site );
        //if PEGASUS_HOME is not set, use VDS_HOME
        home = ( home == null )? mSiteStore.getVDSHome( site ): home;

        mLogger.log( "Creating a default TC entry for " +
                     RemoveDirectory.getCompleteTranformationName() +
                     " at site " + site,
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
            append( RemoveDirectory.TRANSFORMATION_NAME );


        defaultTCEntry = new TransformationCatalogEntry( RemoveDirectory.TRANSFORMATION_NAMESPACE,
                                                         RemoveDirectory.TRANSFORMATION_NAME,
                                                         RemoveDirectory.TRANSFORMATION_VERSION );

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
