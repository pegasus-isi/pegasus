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


package edu.isi.pegasus.planner.cluster.aggregator;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;


import edu.isi.pegasus.planner.code.GridStartFactory;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.planner.cluster.JobAggregator;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;


import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;


import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.List;
import java.util.Set;
import java.util.Iterator;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.Arrays;
import java.util.HashSet;

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
    public static final String CLUSTERED_JOB_PREFIX  = "merge_";

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
     * The marker to designate a line in the input file reserved for 
     * monitord purposes.
     */
    public static final String MONITORD_COMMENT_MARKER = "#@";

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
     * Handle to the site catalog store
     */
    protected SiteStore mSiteStore;
    //protected PoolInfoProvider mSiteHandle;

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
     * Bag of initialization objects.
     */
    protected PegasusBag mBag;


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
     * The default constructor.
     */
    public Abstract(){

    }


    /**
     *Initializes the JobAggregator impelementation
     *
     * @param dag  the workflow that is being clustered.
     * @param bag   the bag of objects that is useful for initialization.
     *
     */
    public void initialize( ADag dag , PegasusBag bag  ){
        mBag    = bag;
        mClusteredADag = dag;

        mLogger = bag.getLogger();
        mProps  = bag.getPegasusProperties();

        mTCHandle = bag.getHandleToTransformationCatalog();
        mSiteStore = bag.getHandleToSiteStore();

        setDirectory( bag.getPlannerOptions().getSubmitDirectory() );

        mGridStartFactory = new GridStartFactory();
        mGridStartFactory.initialize( mBag, dag, null );


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
     * @param jobs the list of <code>Job</code> objects that need to be
     *             collapsed. All the jobs being collapsed should be scheduled
     *             at the same pool, to maintain correct semantics.
     * @param name  the logical name of the jobs in the list passed to this
     *              function.
     * @param id   the id that is given to the new job.
     *
     * @return  the <code>Job</code> object corresponding to the aggregated
     *          job containing the jobs passed as List in the input,
     *          null if the list of jobs is empty
     */
    public AggregatedJob constructAbstractAggregatedJob(List jobs,String name,String id){
        return constructAbstractAggregatedJob(jobs,name,id,getClusterExecutableLFN());
    }

    /**
     * Constructs an abstract aggregated job that has a handle to the appropriate
     * JobAggregator that will be used to aggregate the jobs.
     *
     * @param jobs the list of <code>SubInfo</code> objects that need to be
     *             collapsed. All the jobs being collapsed should be scheduled
     *             at the same pool, to maintain correct semantics.
     * @param name  the logical name of the jobs in the list passed to this
     *              function.
     * @param id   the id that is given to the new job.
     * @param mergeLFN the logical name for the aggregated job that has to be
     *                 constructed.
     *
     * @return  the <code>SubInfo</code> object corresponding to the aggregated
     *          job containing the jobs passed as List in the input,
     *          null if the list of jobs is empty
     */
    public AggregatedJob constructAbstractAggregatedJob( List jobs,
                                                         String name,
                                                         String id,
                                                         String mergeLFN ){
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
        Job firstJob = (Job)jobs.get(0);
        AggregatedJob mergedJob = new AggregatedJob( /*(Job)jobs.get(0),*/
                                                     jobs.size() );

        mergedJob.setJobAggregator( this );


        mergedJob.setJobType( Job.COMPUTE_JOB );

        Job job    = null;
        StringBuffer sb = new StringBuffer();
        sb.append( Abstract.CLUSTERED_JOB_PREFIX );
        if( name != null && name.length() > 0 ){
            sb.append( name ).append( "_" );
        }
        sb.append( id );
        String mergedJobName = sb.toString();

        mLogger.log("Constructing Abstract clustered job " + mergedJobName,
                    LogManager.DEBUG_MESSAGE_LEVEL);

        //inconsistency between job name and logical name for now
        mergedJob.setName( mergedJobName );

        //fix for JIRA bug 83
        //the site handle needs to be set for the aggregated job
        //before it is enabled.
        mergedJob.setSiteHandle( firstJob.getSiteHandle() );
        mergedJob.setStagingSiteHandle( firstJob.getStagingSiteHandle() );


        Set ipFiles = new HashSet();
        Set opFiles = new HashSet();
        boolean userExecutablesStaged = false;
        for( Iterator it = jobs.iterator(); it.hasNext(); ) {
                job = (Job) it.next();
                ipFiles.addAll( job.getInputFiles() );
                opFiles.addAll( job.getOutputFiles() );
                mergedJob.add(job);
                
                //update user executable staging.
                userExecutablesStaged = userExecutablesStaged || job.userExecutablesStagedForJob();

                //we need to merge the profiles from the constituent
                //jobs now, rather in function makeAbstractAggreagatedJobConcrete
                //JIRA PM-368
                //merge profiles for all jobs
                mergedJob.mergeProfiles( job );

        }

        mergedJob.setExecutableStagingForJob(userExecutablesStaged);
        
        //overriding the input files, output files, id
        mergedJob.setInputFiles( ipFiles );
        mergedJob.setOutputFiles( opFiles );


        mergedJob.setTransformation( Abstract.TRANSFORMATION_NAMESPACE,
                                     mergeLFN,
                                     Abstract.TRANSFORMATION_VERSION  );
        mergedJob.setDerivation( Abstract.DERIVATION_NAMESPACE,
                                 mergeLFN,
                                 Abstract.DERIVATION_VERSION);

        mergedJob.setLogicalID( id );

        
        //the compute job of the VDS supernode is this job itself
        mergedJob.setVDSSuperNode( mergedJobName );


        //explicitly set stdout to null overriding any stdout
        //that might have been inherited in the clone operation.
        //FIX for bug 142 http://bugzilla.globus.org/vds/show_bug.cgi?id=142
        mergedJob.setStdOut( "" );
        mergedJob.setStdErr( "" );

        return mergedJob;
    }

    /**
     * Enables the abstract clustered job for execution and converts it to it's 
     * executable form
     * 
     * @param job          the abstract clustered job
     */
    public void makeAbstractAggregatedJobConcrete( AggregatedJob job ){

        
        //containers for the input and output
        //files of fat job. Set insures no duplication
        //The multiple transfer ensures no duplicate transfer of
        //input files. So doing the set thing is redundant.
        //Hashset not used correctly

        File stdIn = writeOutInputFileForJobAggregator( job );
        
        //the executable that fat job refers to is collapser
        TransformationCatalogEntry entry = this.getTCEntry( job );

        job.setRemoteExecutable( entry.getPhysicalTransformation() );

        //stdin file is the file containing the arguments
        //for the jobs being collapsed
        job.setStdIn( stdIn.getName() );

        //explicitly set stdout to null overriding any stdout
        //that might have been inherited in the clone operation.
        //FIX for bug 142 http://bugzilla.globus.org/vds/show_bug.cgi?id=142
        job.setStdOut( "" );
        job.setStdErr( "" );

        
        //get hold of one of the jobs and suck init's globus namespace
        //info into the the map.

        //add any notifications specified in the transformation
        //catalog for the job. JIRA PM-391
        job.addNotifications( entry );


        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        job.updateProfiles( entry );

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        job.updateProfiles( mProps );

        //set the arguments for the clustered job
        //they are set in the end to ensure that profiles can
        //be used to specify the argumetns
        job.setArguments( this.aggregatedJobArguments( job ) );


        return ;

    }


    /**
     * Generates the comment string for the job . It generates a comment of the 
     * format # task_id transformation derivation. 
     * 
     * @param job       the job for which 
     * @param taskid    the task id to put in.
     * 
     * @return the comment invocation
     */
    protected String getCommentString( Job job, int taskid ){
        return this.getCommentString(taskid, job.getCompleteTCName(), job.getDAXID());
    }
    
    /**
     * Generates the comment string for the job . It generates a comment of the 
     * format # task_id transformation derivation. 
     * 
     * @param taskid    the task id to put in.
     * @param transformationName
     * @param daxID     the id of the job from the DAX
     * 
     * @return the comment invocation
     */
    protected String getCommentString( int taskid, String transformationName, String daxID ){
        StringBuffer sb = new StringBuffer();
        sb.append( MONITORD_COMMENT_MARKER ).append( " " ).
           append( taskid ).append( " " ).
           append( transformationName ).append( " " ).
           append( daxID ).append( " " );
           
        return sb.toString();
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
    protected TransformationCatalogEntry getTCEntry(Job job){
       List tcentries = null;
       TransformationCatalogEntry entry  = null;
       try {
           tcentries = mTCHandle.lookup(job.namespace,
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
                 this.defaultTCEntry( this.getClusterExecutableLFN(),
                                      this.getClusterExecutableBasename(),
                                      job.getSiteHandle() ): //try using a default one
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
     * @param name                the logical name for the clustering transformation.
     * @param executableBasename  the basename for the executable in the bin directory
     *                            of a Pegasus installation
     * @param site  the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    private  TransformationCatalogEntry defaultTCEntry( String name,
                                                        String executableBasename,
                                                        String site ){
        TransformationCatalogEntry defaultTCEntry = null;
        //check if PEGASUS_HOME is set
        String home = mSiteStore.getPegasusHome( site );
        //if PEGASUS_HOME is not set, use VDS_HOME
        home = ( home == null )? mSiteStore.getVDSHome( site ): home;

        mLogger.log( "Creating a default TC entry for " +
                     Abstract.getCompleteTranformationName( name ) +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //if home is still null
        if ( home == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for " +
                         Abstract.getCompleteTranformationName( name ),
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
            append( executableBasename );


        defaultTCEntry = new TransformationCatalogEntry( Abstract.TRANSFORMATION_NAMESPACE,
                                                         name,
                                                         Abstract.TRANSFORMATION_VERSION );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site );
        defaultTCEntry.setType( TCType.INSTALLED );
        defaultTCEntry.setSysInfo( this.mSiteStore.getSysInfo( site ) );

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



    /**
     * Determines whether there is NOT an entry in the transformation catalog
     * for a particular transformation on a particular site.
     *
     * @param namespace  the logical namespace of the transformation.
     * @param name       the logical name of the transformation.
     * @param version    the version of the transformation.
     * @param executableBasename  basename of the executable that does the clustering.
     * @param site       the site at which existence check is required.
     *
     * @return boolean  true if an entry does not exists, false otherwise.
     */
    protected boolean entryNotInTC(String namespace,
                                   String name,
                                   String version,
                                   String executableBasename,
                                   String site){

        //check on for pfn for existence. gmehta says lesser queries
        //underneath
        List l = null;
        try{
            l = mTCHandle.lookupNoProfiles(namespace, name, version, site,
                                             TCType.INSTALLED);
        }
        catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC " + e.getMessage(),
                LogManager.ERROR_MESSAGE_LEVEL);
        }

        //a double negative
        return  ( l == null || l.isEmpty() ) ?
                  (( this.defaultTCEntry( name, executableBasename, site ) ) == null ) ://construct a default tc entry
                  false ;
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

    
    /**
     * Writes out the input file for the aggregated job
     *
     * @param job   the aggregated job
     *
     * @return path to the input file
     */
    protected File writeOutInputFileForJobAggregator(AggregatedJob job ) {
        return this.writeOutInputFileForJobAggregator(job, 1);
    }
    
    
    /**
     * Writes out the input file for the aggregated job
     *
     * @param job   the aggregated job
     *
     * @return path to the input file
     */
    protected File writeOutInputFileForJobAggregator(AggregatedJob job, Integer taskid) {
        File stdin = null;
        try {
            BufferedWriter writer;
            String name = job.getID() + ".in";
            
            //PM-833 the .in file should be in the same directory where all job submit files go
            File directory = new File( this.mDirectory, job.getRelativeSubmitDirectory() );
            stdin = new File( directory, name );
            writer = new BufferedWriter(new FileWriter( stdin ) );

            //traverse throught the jobs to determine input/output files
            //and merge the profiles for the jobs
            //int taskid = 1;
            
            for(  Iterator it = this.topologicalOrderingRequired() ?
                            job.topologicalSortIterator()://we care about order
                            job.nodeIterator();//dont care about order
                                                it.hasNext();  ) {
                GraphNode node = ( GraphNode )it.next();
                Job constitutentJob = (Job) node.getContent();

                //handle stdin
                if( constitutentJob instanceof AggregatedJob ){
                    //PM-817 recursive clustering case, we need to 
                    //write out merge_XXXX.in file for constitutent job
                    //that is a clustered job itself
                    File file = this.writeOutInputFileForJobAggregator( (AggregatedJob)constitutentJob, taskid );
                    //slurp in contents of it's stdin
                    //taking care of the taskid increments across recursion
                    BufferedReader reader = new BufferedReader(
                                                             new FileReader( file )
                                                               );
                    String line;
                    while( (line = reader.readLine()) != null ){
                        //ignore comment out lines
                        if( line.startsWith( MONITORD_COMMENT_MARKER) ){
                            String[] split = line.split( "\\s+" );
                            //System.out.println(Arrays.toString(split));
                            //taskid = Integer.parseInt( split[1] );
                            writer.write( getCommentString(  taskid, split[2], split[3] ) + "\n" );
                            continue;
                        }
                        writer.write( line );
                        writer.write( "\n" );
                        taskid++;
                    }
                    reader.close();
                    //delete the previous stdin file
                    file.delete();
                }
                else{
                    //write out the argument string to the
                    //stdin file for the fat job

                    //genereate the comment string that has the
                    //taskid transformation derivation
                    writer.write( getCommentString( constitutentJob, taskid ) + "\n" );

                    // the arguments are no longer set as condor profiles
                    // they are now set to the corresponding profiles in
                    // the Condor Code Generator only.
                    writer.write( constitutentJob.getRemoteExecutable()  + " " +
                                   constitutentJob.getArguments() + "\n");
                    taskid++;
                }
            }

            //closing the handle to the writer
            writer.close();
        }
        catch(IOException e){
            mLogger.log("While writing the stdIn file " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( "While writing the stdIn file " + stdin, e );
        }

        return stdin;

    }

}
