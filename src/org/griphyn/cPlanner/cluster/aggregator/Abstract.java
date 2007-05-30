/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.cluster.aggregator;


import org.griphyn.cPlanner.code.GridStart;

import org.griphyn.cPlanner.code.gridstart.GridStartFactory;
import org.griphyn.cPlanner.code.gridstart.GridStartFactoryException;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.AggregatedJob;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.SiteInfo;

import org.griphyn.cPlanner.cluster.JobAggregator;

import org.griphyn.cPlanner.namespace.Condor;
import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;
import org.griphyn.cPlanner.poolinfo.PoolMode;


import org.griphyn.common.util.DynamicLoader;

import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.catalog.transformation.TCMode;

import org.griphyn.common.classes.TCType;


import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.List;
import java.util.Set;
import java.util.Iterator;
import org.griphyn.common.util.Separator;

/**
 * An abstract implementation of the JobAggregator interface, which the other
 * implementations can choose to extend.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 *
 */
public abstract class Abstract implements JobAggregator {

    /**
     * The prefix that is assigned to the jobname of the collapsed jobs to
     * get the jobname for the fat job.
     */
    public static final String FAT_JOB_PREFIX  = "merge_";

    /**
     * The transformation namespace for the cluster jobs.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The version number for the derivations for cluster jobs
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for the cluster  jobs.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The version number for the derivations for cluster jobs.
     */
    public static final String DERIVATION_VERSION = "1.0";



    /**
     * The directory, where the stdin file of the fat jobs are created.
     * It should be the submit file directory that the user mentions at
     * runtime.
     */
    protected String mDirectory;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The handle to the LogManager that logs all the messages.
     */
    protected LogManager mLogger;

    /**
     * The handle to the transformation catalog.
     */
    protected TransformationCatalog mTCHandle;

    /**
     * Handle to the site catalog.
     */
    protected PoolInfoProvider mSiteHandle;

    /**
     * The handle to the ADag object that contains the workflow being
     * clustered.
     */
    protected ADag mClusteredADag;

    /**
     * The handle to the GridStart Factory.
     */
    protected GridStartFactory mGridStartFactory;


    /**
     * A convenience method to return the complete transformation name being
     * used to construct jobs in this class.
     *
     * @param name  the name of the transformation
     *
     * @return the complete transformation name
     */
    public static String getCompleteTranformationName( String name ){
        return Separator.combine( TRANSFORMATION_NAMESPACE,
                                  name,
                                  TRANSFORMATION_VERSION );
    }


    /**
     * The overloaded constructor, that is called by load method.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param submitDir  the submit directory where the submit file for the job
     *                   has to be generated.
     * @param dag        the workflow that is being clustered.
     *
     * @see JobAggregatorFactory#loadInstance(String,PegasusProperties,String,ADag)
     *
     */
    public Abstract(PegasusProperties properties, String submitDir, ADag dag){
        mLogger = LogManager.getInstance();
        mProps  = properties;
        setDirectory(submitDir);
        mClusteredADag = dag;
        //load the transformation catalog
        //should have been loaded by passing
        //the PegasusProperties object?? Karan
        mTCHandle = TCMode.loadInstance();

        mGridStartFactory = new GridStartFactory();
        mGridStartFactory.initialize( properties, submitDir, dag );

        //load the SiteHandle
        String poolmode = mProps.getPoolMode();
        String poolClass = PoolMode.getImplementingClass(poolmode);
        mSiteHandle = PoolMode.loadPoolInstance(poolClass,mProps.getPoolFile(),
                                                PoolMode.SINGLETON_LOAD);

    }

    /**
     * Returns the arguments with which the <code>AggregatedJob</code>
     * needs to be invoked with.
     *
     * @param job  the <code>AggregatedJob</code> for which the arguments have
     *             to be constructed.
     *
     * @return argument string
     */
    public abstract String aggregatedJobArguments( AggregatedJob job );


     /**
     * Constructs a new aggregated job that contains all the jobs passed to it.
     * The new aggregated job, appears as a single job in the workflow and
     * replaces the jobs it contains in the workflow.
     *
     * @param jobs the list of <code>SubInfo</code> objects that need to be
     *             collapsed. All the jobs being collapsed should be scheduled
     *             at the same pool, to maintain correct semantics.
     * @param name  the logical name of the jobs in the list passed to this
     *              function.
     * @param id   the id that is given to the new job.
     *
     * @return  the <code>SubInfo</code> object corresponding to the aggregated
     *          job containing the jobs passed as List in the input,
     *          null if the list of jobs is empty
     */
    public AggregatedJob construct(List jobs,String name,String id){
        return construct(jobs,name,id,getCollapserLFN());
    }


    /**
     * Constructs a new aggregated job that contains all the jobs passed to it.
     * The new aggregated job, appears as a single job in the workflow and
     * replaces the jobs it contains in the workflow.
     *
     * @param jobs    the list of <code>SubInfo</code> objects that need to be
     *                collapsed. All the jobs being collapsed should be scheduled
     *                at the same pool, to maintain correct semantics.
     * @param name    the logical name of the jobs in the list passed to this
     *                function.
     * @param id      the id that is given to the new job.
     * @param mergeLFN the logical name for the aggregated job that has to be
     *                 constructed.
     *
     * @return  the <code>AggregatedJob</code> object corresponding to the aggregated
     *          job containing the jobs passed as List in the input,
     *          null if the list of jobs is empty
     */
    protected AggregatedJob construct( List jobs,
                                       String name,
                                       String id,
                                       String mergeLFN){
        //sanity check
        if(jobs == null || jobs.isEmpty()){
            mLogger.log("List of jobs for clustering is empty",
                        LogManager.ERROR_MESSAGE_LEVEL);
            return null;
        }

        //sanity check missing to ensure jobs are of same type
        //Right now done in NodeCollapser. But we do not need this for
        //Vertical Clumping. Karan July 28, 2005

        //To get the gridstart/kickstart path on the remote
        //pool, querying with entry for vanilla universe.
        //In the new format the gridstart is associated with the
        //pool not pool, condor universe
        SubInfo firstJob = (SubInfo)jobs.get(0);
        AggregatedJob mergedJob = new AggregatedJob( /*(SubInfo)jobs.get(0),*/
                                                     jobs.size() );
        SiteInfo site = mSiteHandle.getPoolEntry( firstJob.getSiteHandle(),
                                                  Condor.VANILLA_UNIVERSE);
        String gridStartPath = site.getKickstartPath();
        GridStart gridStart = mGridStartFactory.loadGridStart( firstJob,  gridStartPath );

        SubInfo job    = null;
        String mergedJobName = this.FAT_JOB_PREFIX + name + "_" + id;
        mLogger.log("Constructing clustered job " + mergedJobName,
                    LogManager.DEBUG_MESSAGE_LEVEL);

        String stdIn  = null;

        //containers for the input and output
        //files of fat job. Set insures no duplication
        //The multiple transfer ensures no duplicate transfer of
        //input files. So doing the set thing is redundant.
        //Hashset not used correctly
        Set ipFiles    = new java.util.HashSet();
        Set opFiles    = new java.util.HashSet();

        //enable the jobs that need to be merged
        //before writing out the stdin file
        mergedJob = gridStart.enable( mergedJob, jobs );

        try {
            BufferedWriter writer;
            stdIn = mergedJobName + ".in";
            writer = new BufferedWriter(new FileWriter(
                                                       new File(mDirectory,stdIn)));

            //traverse throught the jobs to determine input/output files
            //and merge the profiles for the jobs
            boolean merge = false;
            for( Iterator it = jobs.iterator(); it.hasNext(); ) {
                job = (SubInfo) it.next();
                ipFiles.addAll( job.getInputFiles() );
                opFiles.addAll( job.getOutputFiles() );

                //merge profiles for all jobs except the first
//                if( merge ) { mergedJob.mergeProfiles( job ); }
                //merge profiles for all jobs
                mergedJob.mergeProfiles( job );

                merge = true;

                //handle stdin
                if( job instanceof AggregatedJob ){
                    //slurp in contents of it's stdin
                    File file = new File ( mDirectory, job.getStdIn() );
                    BufferedReader reader = new BufferedReader(
                                                             new FileReader( file )
                                                               );
                    String line;
                    while( (line = reader.readLine()) != null ){
                        writer.write( line );
                        writer.write( "\n" );
                    }
                    reader.close();
                    //delete the previous stdin file
                    file.delete();
                }
                else{
                    //write out the argument string to the
                    //stdin file for the fat job
                    writer.write( job.condorVariables.get("executable")  + " " +
                                 job.condorVariables.get("arguments") + "\n");
                }
            }

            //closing the handle to the writer
            writer.close();
        }
        catch(IOException e){
            mLogger.log("While writing the stdIn file " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( "While writing the stdIn file " + stdIn, e );
        }

        //inconsistency between job name and logical name for now
        mergedJob.setName( mergedJobName );

        mergedJob.setTransformation( this.TRANSFORMATION_NAMESPACE,
                                     mergeLFN,
                                     this.TRANSFORMATION_VERSION  );
        mergedJob.setDerivation( this.DERIVATION_NAMESPACE,
                                 mergeLFN,
                                 this.DERIVATION_VERSION);

        mergedJob.setLogicalID( id );

        mergedJob.setSiteHandle( firstJob.getSiteHandle() );
        mergedJob.setUniverse( firstJob.getUniverse() );
        mergedJob.setJobManager( firstJob.getJobManager() );
        mergedJob.setJobType( SubInfo.COMPUTE_JOB );

        //explicitly set the gridstart key why? Karan Apr 27 2007
        firstJob.vdsNS.construct( VDS.GRIDSTART_KEY,
                                  gridStart.getVDSKeyValue() );


        //the compute job of the VDS supernode is this job itself
        mergedJob.setVDSSuperNode( mergedJobName );

        //the executable that fat job refers to is collapser
        TransformationCatalogEntry entry = this.getTCEntry(mergedJob);

        mergedJob.setRemoteExecutable( entry.getPhysicalTransformation() );


        //overriding the input files, output files, id
        mergedJob.setInputFiles( ipFiles );
        mergedJob.setOutputFiles( opFiles );

        //stdin file is the file containing the arguments
        //for the jobs being collapsed
        mergedJob.setStdIn( stdIn );

        //explicitly set stdout to null overriding any stdout
        //that might have been inherited in the clone operation.
        //FIX for bug 142 http://bugzilla.globus.org/vds/show_bug.cgi?id=142
        mergedJob.setStdOut( "" );
        mergedJob.setStdErr( "" );

        //set the arguments for the clustered job
        mergedJob.setArguments( this.aggregatedJobArguments( mergedJob ) );

        //get hold of one of the jobs and suck init's globus namespace
        //info into the the map.

        /* Not needed, as the clone method would have taken care of it.
           Karan Sept 09, 2004
        entry = getTCEntry(job);
        mergedJob.globusRSL.checkKeyInNS(entry.getProfiles(Profile.GLOBUS));
        */

        //also put in jobType as mpi
        //mergedJob.globusRSL.checkKeyinNS("jobtype","mpi");

        //the profile information from the pool catalog does not need to be
        //assimilated into the job. As the collapsed job is run on the
        //same pool as the job is run
        // mergedJob.updateProfiles(mPoolHandle.getPoolProfile(mergedJob.executionPool));

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        mergedJob.updateProfiles( entry );

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        mergedJob.updateProfiles( mProps );

        return mergedJob;

    }

    /**
     * Helper method to get an entry from the transformation catalog for an
     * installed executable. It does the traversal from the list of entries
     * to return a single TransformationCatalogEntry object, and dies with
     * an appropriate error message if the object is not found.
     * The pool and the name are retrieved from job object.
     *
     * @param job  the job whose corresponding TransformationCatalogEntry you want.
     *
     * @return  the TransformationCatalogEntry corresponding to the entry in the
     *          TC.
     */
    protected TransformationCatalogEntry getTCEntry(SubInfo job){
       List tcentries = null;
       TransformationCatalogEntry entry  = null;
       try {
           tcentries = mTCHandle.getTCEntries(job.namespace,
                                              job.logicalName,
                                              job.version,
                                              job.executionPool,
                                              TCType.INSTALLED);
       } catch (Exception e) {
           mLogger.log(
                "Unable to retrieve entry from TC for transformation " +
                job.getCompleteTCName() + " " +
                e.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL );
        }

        entry = ( tcentries == null ) ?
                 this.defaultTCEntry( job.getTXName(), job.getSiteHandle() ): //try using a default one
                 (TransformationCatalogEntry) tcentries.get(0);

        if( entry == null ){
            //NOW THROWN AN EXCEPTION

            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ").
                  append( job.getCompleteTCName() ).
                  append(" at site ").append( job.getSiteHandle() );

              mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
              throw new RuntimeException( error.toString() );

          }


        return entry;

    }

    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog.
     *
     * @param name  the logical name of the clustering executable.
     * @param site  the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    private  TransformationCatalogEntry defaultTCEntry( String name, String site ){
        TransformationCatalogEntry defaultTCEntry = null;
        //check if PEGASUS_HOME is set
        String home = mSiteHandle.getPegasusHome( site );
        //if PEGASUS_HOME is not set, use VDS_HOME
        home = ( home == null )? mSiteHandle.getVDS_HOME( site ): home;

        mLogger.log( "Creating a default TC entry for " +
                     this.getCompleteTranformationName( name ) +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //if home is still null
        if ( home == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for " +
                         this.getCompleteTranformationName( name ),
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
            append( name );


        defaultTCEntry = new TransformationCatalogEntry( this.TRANSFORMATION_NAMESPACE,
                                                         name,
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



    /**
     * Determines whether there is NOT an entry in the transformation catalog
     * for a particular transformation on a particular site.
     *
     * @param namespace  the logical namespace of the transformation.
     * @param name       the logical name of the transformation.
     * @param version    the version of the transformation.
     * @param site       the site at which existence check is required.
     *
     * @return boolean  true if an entry does not exists, false otherwise.
     */
    protected boolean entryNotInTC(String namespace,String name,
                                   String version,String site){

        //check on for pfn for existence. gmehta says lesser queries
        //underneath
        List l = null;
        try{
            l = mTCHandle.getTCPhysicalNames(namespace, name, version, site,
                                             TCType.INSTALLED);
        }
        catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC " + e.getMessage(),
                LogManager.ERROR_MESSAGE_LEVEL);
        }

        //a double negative
        return !( ( l == null || l.isEmpty() ) ?
                  (( this.defaultTCEntry( name,  site ) ) == null ) ://construct a default tc entry
                  true
                );
    }


    /**
     * Sets the directory where the stdin files are to be generated.
     *
     * @param directory  the path to the directory to which it needs to be set.
     */
    protected void setDirectory(String directory){
        mDirectory = (directory == null)?
                      //user did not specify a submit file dir
                      //use the default i.e current directory
                      ".":
                      //user specified directory picked up
                      directory;

    }

}
