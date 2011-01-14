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

package edu.isi.pegasus.planner.code.generator.condor;



import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.CondorVersion;
import edu.isi.pegasus.common.util.FindExecutable;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Job;

import edu.isi.pegasus.planner.namespace.Dagman;

import edu.isi.pegasus.planner.parser.DAXParserFactory;
import edu.isi.pegasus.planner.parser.dax.Callback;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.RunDirectoryFilenameFilter;

import edu.isi.pegasus.planner.client.CPlanner;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;


import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.code.GridStartFactory;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.parser.Parser;

import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import java.text.NumberFormat;
import java.text.DecimalFormat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * The class that takes in a dax job specified in the DAX and renders it into
 * a SUBDAG with pegasus-plan as the appropriate prescript.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SUBDAXGenerator{

    /**
     * Whether to generate the SUBDAG keyword or not.
     */
    public static final boolean GENERATE_SUBDAG_KEYWORD = false;
    
    /**
     * Suffix to be applied for cache file generation.
     */
    private static final String CACHE_FILE_SUFFIX = ".cache";
            
    
    /**
     * The logical name with which to query the transformation catalog for
     * cPlanner executable.
     */
    public static final String CPLANNER_LOGICAL_NAME = "pegasus-plan";

    /**
     * The namespace to use for condor dagman.
     */
    public static final String CONDOR_DAGMAN_NAMESPACE = "condor";

    /**
     * The logical name with which to query the transformation catalog for the
     * condor_dagman executable, that ends up running the mini dag as one
     * job.
     */
    public static final String CONDOR_DAGMAN_LOGICAL_NAME = "dagman";

    /**
     * The namespace to which the job in the MEGA DAG being created refer to.
     */
    public static final String NAMESPACE = "pegasus";

    /**
     * The planner utility that needs to be called as a prescript.
     */
    public static final String RETRY_LOGICAL_NAME = "pegasus-plan";

    /**
     * The dagman knobs controlled through property. They map the property name to
     * the corresponding dagman option.
     */
    public static final String DAGMAN_KNOBS[][]={
        { Dagman.MAXPRE_KEY,  " -MaxPre " },
        { Dagman.MAXPOST_KEY, " -MaxPost " },
        { Dagman.MAXJOBS_KEY, " -MaxJobs " },
        { Dagman.MAXIDLE_KEY, " -MaxIdle " },
    };


    /**
     * The username of the user running the program.
     */
    private String mUser;

    /**
     * The number formatter to format the run submit dir entries.
     */
    private NumberFormat mNumFormatter;

    /**
     * The object containing all the options passed to the Concrete Planner.
     */
    private PlannerOptions mPegasusPlanOptions;

    /**
     * The handle to Pegasus Properties.
     */
    private PegasusProperties mProps;

    /**
     * Handle to the logging manager.
     */
    private LogManager mLogger;

    /**
     * Bag of Pegasus objects
     */
    private PegasusBag mBag;
    
    /**
     * The print writer handle to DAG file being written out.
     */
    private PrintWriter mDAGWriter;
    
    /**
     * The handle to the transformation catalog
     */
    private TransformationCatalog mTCHandle;
    
    /**
     * The cleanup scope for the workflows.
     */
    private PegasusProperties.CLEANUP_SCOPE mCleanupScope;
    
    /**
     * The long value of condor version.
     */
    private long mCondorVersion;
    
    /**
     * Any extra arguments that need to be passed to dagman, as determined
     * from the properties file.
     */
    // String mDAGManKnobs;

    /**
     * Maps a sub dax job id to it's submit directory. The population relies
     * on top down traversal during Code Generation.
     */
    private Map<String,String>mDAXJobIDToSubmitDirectoryCacheFile;
    
    private Graph mWorkflow;
    
    
    /**
     * The default constructor.
     */
    public SUBDAXGenerator() {
        mNumFormatter = new DecimalFormat( "0000" );


    }


    /**
     * Initializes the class.
     *
     * @param bag  the bag of objects required for initialization
     * @param workflow  the dag for which code is being generated
     * @param dagWriter  handle to the dag writer
     */
    public void initialize( PegasusBag bag, Graph workflow, PrintWriter dagWriter ){
        mBag = bag;
        mWorkflow = workflow;
        mDAGWriter = dagWriter;
        mProps  = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        mTCHandle = bag.getHandleToTransformationCatalog();
        this.mPegasusPlanOptions  = bag.getPlannerOptions();
        mCleanupScope = mProps.getCleanupScope();
        mDAXJobIDToSubmitDirectoryCacheFile = new HashMap();
                
        mUser = mProps.getProperty( "user.name" ) ;
        if ( mUser == null ){ mUser = "user"; }

        //hardcoded options for time being.
        mPegasusPlanOptions.setPartitioningType( "Whole" );
        
        mCondorVersion = CondorVersion.getInstance( mLogger ).numericValue();
        if( mCondorVersion == -1 ){
            mLogger.log( "Unable to determine the version of condor " , LogManager.WARNING_MESSAGE_LEVEL );
        }
        else{
            mLogger.log( "Condor Version detected is " + mCondorVersion , LogManager.DEBUG_MESSAGE_LEVEL );
        }
        
    }

    
   
    /**
     * Generates code for a job
     *
     * @param job  the job for which code has to be generated.
     *
     * @return a <code>Job</code> if a submit file needs to be generated
     *         for the job. Else return null.
     * 
     */
    public Job generateCode( Job job ){
        String arguments = job.getArguments();
        String [] args = arguments.split( " " );
        
        mLogger.log( "Generating code for DAX job  " + job.getID(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
        mLogger.log( "Arguments passed to SUBDAX Generator are " + arguments,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        
        //convert the args to pegasus-plan options
        PlannerOptions options = new CPlanner( mLogger ).parseCommandLineArguments( args, false );

        //figure out the label and index for SUBDAX
        String label = null;
        String index = null;
        File dax = new File( options.getDAX() );
        String labelBasedDir = null;
        if( dax.exists() ){
            //retrieve the metadata in the subdax.
            //means the the dax needs to be generated beforehand.
            Map metadata = getDAXMetadata( options.getDAX() );
            label = (String) metadata.get( "name" );
            index = (String) metadata.get( "index" );
            //the label for directory purposes includes the logical id too
            labelBasedDir = label + "_" + job.getLogicalID();
        }
        else{
            //try and construct on basis of basename prefix option
            String basenamePrefix = options.getBasenamePrefix() ;
            if( basenamePrefix == null ){
                StringBuffer error = new StringBuffer();
                error.append( "DAX file for subworkflow does not exist " ).append( dax ).
                      append( " . Either set the --basename option to subworkflow or make sure dax exists" );
                throw new RuntimeException( error.toString() );
            }
            label = options.getBasenamePrefix();
            index = "0";
            labelBasedDir = label;
            mLogger.log( "DAX File for subworkflow does not exist. Set label value to the basename option passed ",
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }



        //check if we want a label based submit directory for the sub workflow
       // if( mProps.labelBasedSubmitDirectoryForSubWorkflows()  ){
        //From 3.0 onwards if a user does not specify a relative submit 
        //we always create a label/job id based directory structure
            String relative = options.getRelativeSubmitDirectoryOption();

            relative = ( relative == null )?
                        labelBasedDir ://no relative-submit-dir option specified. set to label
                        new File( relative, labelBasedDir ).getPath();

            options.setRelativeSubmitDirectory( relative );
        //}
        String submit = options.getSubmitDirectory();

        mLogger.log( "Submit directory in sub dax specified is " + submit,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        if( submit == null || !submit.startsWith( File.separator ) ){
            //then set the submit directory relative to the parent workflow basedir
            String innerBase     = mPegasusPlanOptions.getBaseSubmitDirectory();
            String innerRelative = mPegasusPlanOptions.getRelativeSubmitDirectory();
            innerRelative = ( innerRelative == null && mPegasusPlanOptions.partOfDeferredRun() )?
                             mPegasusPlanOptions.getRandomDir(): //the random dir is the relative submit dir?
                             innerRelative;
            
            //FIX for JIRA bug 65 to ensure innerRelative is resolved correctly
            //in case of innerRelative being ./ . We dont want inner relative
            //to compute to .// Instead we want it to compute to ././
            //innerRelative += File.separator + submit  ;
            innerRelative =  new File( innerRelative, submit ).getPath();
            
            //options.setSubmitDirectory( mPegasusPlanOptions.getSubmitDirectory(), submit );
            options.setSubmitDirectory( innerBase, innerRelative );
            mLogger.log( "Base Submit directory for inner workflow set to " + innerBase,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            mLogger.log( "Relative Submit Directory for inner workflow set to " + innerRelative,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            mLogger.log( "Submit directory for inner workflow set to " + options.getSubmitDirectory(),
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }

        if( options.getExecutionSites().isEmpty() ){
            //for JIRA feature request PM-64
            //no sites are specified. use the execution sites for
            //the parent workflow
            mLogger.log( "Setting list of execution sites to the same as outer workflow",
                         LogManager.DEBUG_MESSAGE_LEVEL );
            options.getExecutionSites().addAll( mPegasusPlanOptions.getExecutionSites() );
        }
        
        //add the parents generated transient rc to the cache files
        //arguments for the sub workflow
        Set cacheFiles = options.getCacheFiles();
        Set parentsTransientRCs = getParentsTransientRC( job );
        if ( !parentsTransientRCs.isEmpty() ){
            mLogger.log( "Parent DAX Jobs Transient RC's are " + parentsTransientRCs,
                         LogManager.DEBUG_MESSAGE_LEVEL  );
            cacheFiles.addAll( parentsTransientRCs  );
        }

        //do some sanitization of the path to the dax file.
        //if it is a relative path, then ???
        options.setSanitizePath( true );
        
        
        String baseDir = options.getBaseSubmitDirectory();
        String relativeDir = null;
        //construct the submit directory structure for subdax
        try{
            relativeDir = (options.getRelativeSubmitDirectory() == null) ?
                                 //create our own relative dir
                                 createSubmitDirectory(label,
                                                       baseDir,
                                                       mUser,
                                                       options.getVOGroup(),
                                                       mProps.useTimestampForDirectoryStructure()) :
                                 options.getRelativeSubmitDirectory();
        }
        catch( IOException ioe ){
            String error = "Unable to write  to directory" ;
            throw new RuntimeException( error + options.getSubmitDirectory() , ioe );

        }

        options.setSubmitDirectory( baseDir, relativeDir  );
        mLogger.log( "Submit Directory for SUB DAX  is " + options.getSubmitDirectory() , LogManager.DEBUG_MESSAGE_LEVEL );
        
     
         
        if( options.getRelativeDirectory() == null || !options.getRelativeDirectory().startsWith( File.separator ) ){
            //then set the relative directory relative to the parent workflow relative dir
            String baseRelativeExecDir = mPegasusPlanOptions.getRelativeDirectory();
            if( baseRelativeExecDir == null ){
                //set the relative execution directory to relative submit directory
                options.setRelativeDirectory( options.getRelativeSubmitDirectory() );
            }
            else{
                //the else look should not be there.
                //construct path from base relative exec dir
                File innerRelativeExecDir = null;
                if( mProps.labelBasedSubmitDirectoryForSubWorkflows() ){
                    innerRelativeExecDir = new File( baseRelativeExecDir, options.getRelativeSubmitDirectory() );
                    //this is temporary till LIGO fixes it's dax
                    //and above property will go away.
                    //we dont want label in the exec dir
                    innerRelativeExecDir = innerRelativeExecDir.getParentFile();
                }
                else{
                    //starting 3.0 onwards we dont want long paths
                    //in execution directories for sub workflows
                    //JIRA PM-260
                    String innerRelative = options.getRelativeDirectory();
                    innerRelative = ( innerRelative == null )?
                                     //construct something on basis of label
                                     labelBasedDir :
                                     innerRelative;
                    innerRelativeExecDir = new File( baseRelativeExecDir, innerRelative);
                }
                options.setRelativeDirectory(innerRelativeExecDir.getPath() );
            }
        }
        mLogger.log( "Relative Execution Directory for SUB DAX is " + options.getRelativeDirectory() , LogManager.DEBUG_MESSAGE_LEVEL );

        //create a symbolic link to dax in the subdax submit directory
        String linkedDAX = createSymbolicLinktoDAX( options.getSubmitDirectory(),
                                                    options.getDAX() );
        
        //update options with the linked dax
        options.setDAX( linkedDAX );
        
        //for time being for LIGO , try and create a symlink for
        //the cache file that is created during sub workflow execution
        //in parent directory of the submit directory
        //JIRA PM-116
        if( mProps.labelBasedSubmitDirectoryForSubWorkflows() ){
            this.createSymbolicLinktoCacheFile( options, label, index);
        }
        
        //write out the properties in the submit directory
        String propertiesFile = null;
        try{
            //we dont want to store the path to sub workflow properties files in the
            //internal variable in PegasusProperties.
            propertiesFile = this.mProps.writeOutProperties( options.getSubmitDirectory(), true, false );
        }
        catch( IOException ioe ){
            throw new RuntimeException( "Unable to write out properties to directory " + options.getSubmitDirectory() );
        }
        
        //construct  the pegasus-plan prescript for the JOB
        //the log file for the prescript should be in the
        //submit directory of the outer level workflow
        StringBuffer log = new StringBuffer();
        log.append( mPegasusPlanOptions.getSubmitDirectory() ).append( File.separator ).
            append( job.getName() ).append( ".pre.log" );
        String prescript = constructPegasusPlanPrescript( job, options, propertiesFile, log.toString() );
        job.setPreScript( prescript );
        
        //determine the path to the dag file that will be constructed
        if( GENERATE_SUBDAG_KEYWORD ){
            StringBuffer dag = new StringBuffer();
            dag.append( options.getSubmitDirectory() ).append( File.separator ).
                append( CondorGenerator.getDAGFilename( options, label, index, ".dag") );
        
            //print out the SUBDAG keyword for the job
            StringBuffer sb = new StringBuffer();
            sb.append( Dagman.SUBDAG_EXTERNAL_KEY ).append( " " ).
               append( job.getName() ).append( " " ).
               append( dag.toString() );
            mDAGWriter.println( sb.toString() );
            return null;
        }
        else{
            StringBuffer basenamePrefix = new StringBuffer();
            if( options.getBasenamePrefix() == null ){
                basenamePrefix.append( label );                
                basenamePrefix.append( "-" ).append( index );
            }else{
                //add the prefix from options
                basenamePrefix.append( options.getBasenamePrefix() );
            }
            
            mLogger.log( "Basename prefix for the sub workflow is " + basenamePrefix,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            String subDAXCache = new File( options.getSubmitDirectory(),
                                           basenamePrefix + CACHE_FILE_SUFFIX ).getAbsolutePath();
            mLogger.log( "Cache File for the sub workflow is " + subDAXCache,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            mDAXJobIDToSubmitDirectoryCacheFile.put( job.getID(), subDAXCache);
            
            Job dagJob = constructDAGJob( job,
                                    new File( mPegasusPlanOptions.getSubmitDirectory() ),
                                    new File( options.getSubmitDirectory()),
                                    basenamePrefix.toString()
                                  );
            //set the prescript
            dagJob.setPreScript( prescript );
            return dagJob;
        }

    }
    
    /**
     * Constructs a job that plans and submits the partitioned workflow,
     * referred to by a Partition. The main job itself is a condor dagman job
     * that submits the concrete workflow. The concrete workflow is generated by
     * running the planner in the prescript for the job.
     *
     * @param subdaxJob         the original subdax job.
     * @param directory   the directory where the submit file for dagman job has
     *                    to be written out to.
     * @param subdaxDirectory   the submit directory where the submit files for the
     *                          subdag reside.
     * @param basenamePrefix    the basename to be assigned to the files associated 
     *                          with DAGMan
     * 
     * @return the constructed DAG job.
     */
    protected Job constructDAGJob( Job subdaxJob,
                                       File directory,
                                       File subdaxDirectory,
                                       String basenamePrefix){
        
    
        //for time being use the old functions.
        Job job = new Job();
        //the parent directory where the submit file for condor dagman has to
        //reside. the submit files for the corresponding partition are one level
        //deeper.
        String parentDir = directory.getAbsolutePath();

        //set the logical transformation
        job.setTransformation(CONDOR_DAGMAN_NAMESPACE,
                              CONDOR_DAGMAN_LOGICAL_NAME,
                              null);

        //set the logical derivation attributes of the job.
        job.setDerivation(CONDOR_DAGMAN_NAMESPACE,
                          CONDOR_DAGMAN_LOGICAL_NAME,
                          null);

        //always runs on the submit host
        job.setSiteHandle("local");
        
        //we want the DAG job to inherit the dagman profile keys
        //cannot inherit condor and pegasus profiles, as 
        //they may get the dag job to run incorrectly 
        //example, pegasus style keys if specified for site local
        job.dagmanVariables.merge( subdaxJob.dagmanVariables );
        


        //set the partition id only as the unique id
        //for the time being.
//        job.setName(partition.getID());

        //set the logical id for the job same as the partition id.
        job.setLogicalID( subdaxJob.getLogicalID() );

        //construct the name of the DAG job as same as subdax job
        job.setName( subdaxJob.getName() );

        List entries;
        TransformationCatalogEntry entry = null;

        //get the path to condor dagman
        try{
            //try to construct the path from the environment
            entry = constructTCEntryFromEnvironment( );
            
            //try to construct from the TC
            if( entry == null ){
                mLogger.log( "Unable to construct path to dagman from the environment. CONDOR_HOME and CONDOR_LOCATION not set.",
                             LogManager.DEBUG_MESSAGE_LEVEL );
                entries = mTCHandle.lookup(job.namespace, job.logicalName,
                                                 job.version, job.getSiteHandle(),
                                                 TCType.INSTALLED);
                entry = (entries == null) ?
                    defaultTCEntry( "local") ://construct from site catalog
                    //Gaurang assures that if no record is found then
                    //TC Mechanism returns null
                    (TransformationCatalogEntry) entries.get(0);
            }
        }
        catch(Exception e){
            throw new RuntimeException( "ERROR: While accessing the Transformation Catalog",e);
        }
        if(entry == null){
            //throw appropriate error
            throw new RuntimeException("ERROR: Entry not found in tc for job " +
                                        job.getCompleteTCName() +
                                        " on site " + job.getSiteHandle());
        }

        //set the path to the executable and environment string
        job.executable = entry.getPhysicalTransformation();
        //the environment variable are set later automatically from the tc
        //job.envVariables = entry.envString;

        //the job itself is the main job of the super node
        //construct the classad specific information
        job.jobID = job.getName();
        job.jobClass = Job.COMPUTE_JOB;


        //directory where all the dagman related files for the nested dagman
        //reside. Same as the directory passed as an input parameter
        String subdaxDir = subdaxDirectory.getAbsolutePath();

        //make the initial dir point to the submit file dir for the subdag
        //we can do this as we are running this job both on local host, and scheduler
        //universe. Hence, no issues of shared filesystem or anything.
        job.condorVariables.construct("initialdir", subdaxDir );


        //construct the argument string, with all the dagman files
        //being generated in the partition directory. Using basenames as
        //initialdir has been specified for the job.
        StringBuffer sb = new StringBuffer();

        sb.append(" -f -l . -Debug 3").
           append(" -Lockfile ").append( getBasename( basenamePrefix, ".dag.lock") ).
           append(" -Dag ").append( getBasename( basenamePrefix, ".dag"));
        
        //specify condor log for condor version less than 7.1.2
        if( mCondorVersion < CondorVersion.v_7_1_2 ){
           sb.append(" -Condorlog ").append(getBasename( basenamePrefix, ".log"));
        }
        
        //allow for version mismatch as after 7.1.3 condor does tight 
        //checking on dag.condor.sub file and the condor version used
        if( mCondorVersion >= CondorVersion.v_7_1_3 ){
            sb.append( " -AllowVersionMismatch " );
        }
        
        //we append the Rescue DAG option only if old version
        //of Condor is used < 7.1.0.  To detect we check for a non
        //zero value of --rescue option to pegasus-plan
        //Karan June 27, 2007
        mLogger.log( "Number of Resuce retries " + mPegasusPlanOptions.getNumberOfRescueTries() ,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        if( mCondorVersion >= CondorVersion.v_7_1_0 || mPegasusPlanOptions.getNumberOfRescueTries() > 0 ){            
            mLogger.log( "Constructing arguments to dagman in 7.1.0 and later style",
                         LogManager.DEBUG_MESSAGE_LEVEL );
            sb.append( " -AutoRescue 1 -DoRescueFrom 0 ");
        }
        else{
            mLogger.log( "Constructing arguments to dagman in pre 7.1.0 style",
                         LogManager.DEBUG_MESSAGE_LEVEL );
            sb.append(" -Rescue ").append(getBasename( basenamePrefix, ".dag.rescue"));
        }
        
       //pass any dagman knobs that were specified in properties file
       sb.append( this.constructDAGManKnobs( job ) );

       //put in the environment variables that are required
       job.envVariables.construct("_CONDOR_DAGMAN_LOG",
                                  new File( subdaxDir, 
                                            getBasename( basenamePrefix, ".dag.dagman.out")).getAbsolutePath()
                                            );
       job.envVariables.construct("_CONDOR_MAX_DAGMAN_LOG","0");

       //set the arguments for the job
       job.setArguments(sb.toString());

       //the environment need to be propogated for exitcode to be picked up
       job.condorVariables.construct("getenv","TRUE");

       job.condorVariables.construct("remove_kill_sig","SIGUSR1");


       //the log file for condor dagman for the dagman also needs to be created
       //it is different from the log file that is shared by jobs of
       //the partition. That is referred to by Condorlog

//       keep the log file common for all jobs and dagman albeit without
//       dag.dagman.log suffix
//       job.condorVariables.construct("log", getAbsolutePath( partition, dir,".dag.dagman.log"));

//       String dagName = mMegaDAG.dagInfo.nameOfADag;
//       String dagIndex= mMegaDAG.dagInfo.index;
//       job.condorVariables.construct("log", dir + mSeparator +
//                                     dagName + "_" + dagIndex + ".log");

       
       //the job needs to be explicitly launched in 
       //scheduler universe instead of local universe
       job.condorVariables.construct( Condor.UNIVERSE_KEY, Condor.SCHEDULER_UNIVERSE );
       

       //incorporate profiles from the transformation catalog
       //and properties for the time being. Not from the site catalog.

       //the profile information from the transformation
       //catalog needs to be assimilated into the job
       //overriding the one from pool catalog.
       job.updateProfiles( entry );

       //the profile information from the properties file
       //is assimilated overidding the one from transformation
       //catalog.
       job.updateProfiles(mProps);
       
       //we do not want the job to be launched via kickstart
       //Fix for Pegasus bug number 143
       //http://bugzilla.globus.org/vds/show_bug.cgi?id=143
       job.vdsNS.construct( Pegasus.GRIDSTART_KEY,
                            GridStartFactory.GRIDSTART_SHORT_NAMES[GridStartFactory.NO_GRIDSTART_INDEX] );

       return job;
    }
    
    /**
     * Constructs Any extra arguments that need to be passed to dagman, as determined
     * from the properties file.
     *
     * @param job  the job 
     *
     * @return any arguments to be added, else empty string
     */
    public  String constructDAGManKnobs( Job job ){
        StringBuffer sb = new StringBuffer();

        //get all the values for the dagman knows
        int value;
        for( int i = 0; i < SUBDAXGenerator.DAGMAN_KNOBS.length; i++ ){
            value = parseInt( (String)job.dagmanVariables.get( SUBDAXGenerator.DAGMAN_KNOBS[i][0] ) );
            if ( value > 0 ){
                //add the option
                sb.append( SUBDAXGenerator.DAGMAN_KNOBS[i][1] );
                sb.append( value );
            }
        }
        return sb.toString();

    }
    
    /**
     * Parses a string into an integer. Non valid values returned as -1
     *
     * @param s  the String to be parsed as integer
     *
     * @return the int value if valid, else -1
     */
    protected static int parseInt( String s ){
        int value = -1;
        try{
            value = Integer.parseInt( s );
        }
        catch( Exception e ){
            //ignore
        }
        return value;
    }


    
    /**
     * Returns the basename of a dagman (usually) related file for a particular
     * partition.
     *
     * @param prefix  the prefix.
     * @param suffix     the suffix for the file basename.
     *
     * @return the basename.
     */
    protected String getBasename( String prefix, String suffix ){
        StringBuffer sb = new StringBuffer( 16 );
        //add a prefix P
        sb.append( prefix ).append( suffix );
        return sb.toString();
    }

    /**
     * Constructs the basename to the cache file that is to be used
     * to log the transient files. The basename is dependant on whether the
     * basename prefix has been specified at runtime or not.
     *
     * @param options   the options for the sub workflow.
     * @param label     the label for the workflow.
     * @param index     the index for the workflow.
     *
     * @return the name of the cache file
     */
    protected String getCacheFileName( PlannerOptions options, String label , String index ){
        StringBuffer sb = new StringBuffer();
        String bprefix = options.getBasenamePrefix();

        if(bprefix != null){
            //the prefix is not null using it
            sb.append(bprefix);
        }
        else{
            //generate the prefix from the name of the dag
            sb.append( label ).append("-").
           append( index );
        }
        //append the suffix
        sb.append( SUBDAXGenerator.CACHE_FILE_SUFFIX );

        return sb.toString();

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
        //not implemented as we dont have handle to site catalog in this class
        return null;
    }

    /**
     * Returns a tranformation catalog entry object constructed from the environment
     * 
     * An entry is constructed if either of the following environment variables
     * are defined
     * 1) CONDOR_HOME
     * 2) CONDOR_LOCATION
     * 
     * CONDOR_HOME takes precedence over CONDOR_LOCATION
     *
     *
     * @return  the constructed entry else null.
     */
    private  TransformationCatalogEntry constructTCEntryFromEnvironment( ){
        //construct environment profiles 
        Map<String,String> m = System.getenv();
        ENV env = new ENV();
        String key = "CONDOR_HOME";
        if( m.containsKey( key ) ){
            env.construct( key, m.get( key ) );
        }
        
        key = "CONDOR_LOCATION";
        if( m.containsKey( key ) ){
            env.construct( key, m.get( key ) );
        }


        return ( env.isEmpty() ) ?
                constructTCEntryFromPath( )://construct entry from PATH environment variable
                constructTCEntryFromEnvProfiles( env ); //construct entry from environment
    }

    /**
     * Returns a tranformation catalog entry object constructed from the path
     * environment variable
     *
     * @param env  the environment profiles.
     *
     * @return  the entry constructed else null if environment variables not defined.
     */
    private TransformationCatalogEntry constructTCEntryFromPath( ) {
        //find path to condor_dagman
        TransformationCatalogEntry entry = null;
        File condorDagMan = FindExecutable.findExec( "condor_dagman" );

        if( condorDagMan == null ){
            mLogger.log( "Unable to determine path to condor_dagman using PATH environment variable ",
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return entry;
        }

        String dagManPath = condorDagMan.getAbsolutePath();
        mLogger.log( "Constructing path to dagman on basis of env variable PATH " + dagManPath ,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        return this.constructTransformationCatalogEntryForDAGMan( dagManPath );
    }
    
    /**
     * Returns a tranformation catalog entry object constructed from the environment
     * 
     * An entry is constructed if either of the following environment variables
     * are defined
     * 1) CONDOR_HOME
     * 2) CONDOR_LOCATION
     * 
     * CONDOR_HOME takes precedence over CONDOR_LOCATION
     * 
     * @param env  the environment profiles.
     *
     *
     * @return  the entry constructed else null if environment variables not defined.
     */
    private TransformationCatalogEntry constructTCEntryFromEnvProfiles( ENV env ) {
        
        //check if either CONDOR_HOME or CONDOR_LOCATION is defined
        String key = null;
        if( env.containsKey( "CONDOR_HOME") ){
            key = "CONDOR_HOME";
        }
        else if( env.containsKey( "CONDOR_LOCATION") ){
            key = "CONDOR_LOCATION";
        }
        
        if( key == null ){
            //environment variables are not defined.
            return null;
        }
        
        mLogger.log( "Constructing path to dagman on basis of env variable " + key,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        
        //construct path to condor dagman
        StringBuffer path = new StringBuffer();
        path.append( env.get( key ) ).append( File.separator ).
             append( "bin" ).append( File.separator).
             append( "condor_dagman" );
        
        return this.constructTransformationCatalogEntryForDAGMan( path.toString() );
    }

    /**
     * Constructs TransformationCatalogEntry for DAGMan.
     *
     * @param path   path to dagman
     *
     * @return TransformationCatalogEntry for dagman if path is not null, else null.
     */
    private TransformationCatalogEntry constructTransformationCatalogEntryForDAGMan( String path ){
        if( path == null ){
            return null;
        }

        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        entry = new TransformationCatalogEntry();
        entry.setLogicalTransformation( CONDOR_DAGMAN_NAMESPACE,
                                        CONDOR_DAGMAN_LOGICAL_NAME,
                                        null );
        entry.setType( TCType.INSTALLED );
        entry.setResourceId( "local" );


        entry.setPhysicalTransformation( path.toString() );

        return entry;

    }



    /**
     * Constructs the pegasus plan prescript for the subdax
     * 
     * @param job        the subdax job
     * @param options    the planner options with which subdax has to be invoked
     * @param properties the properties file.
     * @param log        the log for the prescript output
     * 
     * @return  the prescript
     */
    public String constructPegasusPlanPrescript( Job job,
                                                 PlannerOptions options,
                                                 String properties,
                                                 String log ){
        StringBuffer prescript = new StringBuffer();

    
        String site = job.getSiteHandle();
        TransformationCatalogEntry entry = null;

        //get the path to script wrapper from the
        try{
            List entries = mTCHandle.lookup( "pegasus",
                                                   "pegasus-plan",
                                                   null,
                                                   "local",
                                                   TCType.INSTALLED);

            //get the first entry from the list returned
            entry = ( entries == null ) ?
                     null :
                     //Gaurang assures that if no record is found then
                     //TC Mechanism returns null
                     ((TransformationCatalogEntry) entries.get(0));
        }
        catch(Exception e){
            throw new RuntimeException( "ERROR: While accessing the Transformation Catalog",e);
        }

        //construct the prescript path
        StringBuffer script = new StringBuffer();
        if(entry == null){
            //log to debug
            mLogger.log("Constructing the default path to the pegasus-plan",
                        LogManager.DEBUG_MESSAGE_LEVEL);

            //construct the default path to the executable
            script.append( mProps.getPegasusHome()).append( File.separator ).
                   append( "bin" ).append( File.separator ).
                   append( "pegasus-plan" );
        }
        else{
            script.append(entry.getPhysicalTransformation());
        }


        //set the flag designating that the planning invocation is part
        //of a deferred planning run
        options.setPartOfDeferredRun( true );

        //in case of deferred planning cleanup wont work
        //explicitly turn it off if the file cleanup scope if fullahead
        if( mCleanupScope.equals( PegasusProperties.CLEANUP_SCOPE.fullahead ) ){
            options.setCleanup( false );
        }

        //we want monitoring to happen
        options.setMonitoring( true );

        //construct the argument string.
        //add the jvm options and the pegasus options if any
        StringBuffer arguments = new StringBuffer();
        arguments./*append( mPOptions.toJVMOptions())*/
                  append(" -Dpegasus.user.properties=").append( properties ).
                  append( " -Dpegasus.log.*=").append(log).
                  //add other jvm options that user may have specified
                  append( options.toJVMOptions() ).
                  //put in all the other options.
                  append( options.toOptions());
        
        //add the --dax option explicitly in the end
        arguments.append( " --dax " ).append( options.getDAX() );

        prescript.append( script ).append( " " ).append( arguments );
        
        return prescript.toString();
    }


    /**
     * Creates a symbolic link to the DAX file in a dax sub directory in the
     * submit directory
     *
     * @param options   the options for the sub workflow.
     * @param label     the label for the workflow.
     * @param index     the index for the workflow.
     *
     * @return  boolean whether symlink is created or not
     */
    public boolean createSymbolicLinktoCacheFile( PlannerOptions options , String label, String index ){
        File f = new File( options.getSubmitDirectory() );
        String cache  = this.getCacheFileName(options, label, index);
        File source =  new File( f, cache );
        File  dest  =  new File ( f.getParent(), cache );

        StringBuffer sb = new StringBuffer();
        sb.append( "Creating symlink " ).append( source.getAbsolutePath() ).
           append( " -> ").append( dest.getAbsolutePath() );
        mLogger.log( sb.toString(), LogManager.DEBUG_MESSAGE_LEVEL );

        return this.createSymbolicLink( source.getAbsolutePath(), dest.getAbsolutePath(), true );
    }



    /**
     * Creates a symbolic link to the DAX file in a dax sub directory in the 
     * submit directory 
     * 
     * 
     * @param submitDirectory    the submit directory for the sub workflow.
     * @param dax                the dax file to which the symbolic link has
     *                           to be created.
     * 
     * @return  the symbolic link created.
     */
    public String createSymbolicLinktoDAX( String submitDirectory , String dax ){
        File dir = new File( submitDirectory, "dax" );
        
        //create a symbolic in the dax subdirectory to the
        //dax file specified in the sub dax
                
        //create the dir if it does not exist
        try{
            sanityCheck( dir );
        }
        catch( IOException ioe ){
            throw new RuntimeException( "Unable to create the submit directory for sub dax " + dir );
        }
                
        //we have the partition written out
        //now create a symlink to the DAX file
        StringBuffer destinationDAX = new StringBuffer();
        destinationDAX.append( dir ).append( File.separator ).
                       append( new File(dax).getName() );
        
        if ( !createSymbolicLink( dax , destinationDAX.toString() ) ){
                throw new RuntimeException( "Unable to create symbolic link between " +
                                                dax + " and " + destinationDAX.toString() );
        }    
        
        return destinationDAX.toString();
    }
    
    
    
    /**
     * Returns the metadata stored in the root adag element in the DAX
     * 
     * @param dax   the dax file.
     * 
     * @return Map containing the metadata.
     */
    public Map getDAXMetadata( String dax ){
        Callback cb =  DAXParserFactory.loadDAXParserCallback( mProps, dax, "DAX2Metadata" );

        try{
//            DAXParser2 daxParser = new DAXParser2( dax, mBag, cb);
            Parser p = DAXParserFactory.loadDAXParser( mBag, cb );
            p.startParser( dax );
        }
        catch( RuntimeException e ){
            //check explicity for file not found exception
            if( e.getCause() != null && e.getCause() instanceof java.io.IOException){
                //rethrow 
                throw e;
            }
            
            //ignore only if the parsing is completed
            mLogger.log( e.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return ( Map ) cb.getConstructedObject();
        
    }
    
    
    


    /**
     * Creates the submit directory for the workflow. This is not thread safe.
     *
     * @param dag     the workflow being worked upon.
     * @param dir     the base directory specified by the user.
     * @param user    the username of the user.
     * @param vogroup the vogroup to which the user belongs to.
     * @param timestampBased boolean indicating whether to have a timestamp based dir or not
     *
     * @return  the directory name created relative to the base directory passed
     *          as input.
     *
     * @throws IOException in case of unable to create submit directory.
     */
    protected String createSubmitDirectory( ADag dag,
                                            String dir,
                                            String user,
                                            String vogroup,
                                            boolean timestampBased ) throws IOException {

        return createSubmitDirectory( dag.getLabel(), dir, user, vogroup, timestampBased );
    }

    /**
     * Creates the submit directory for the workflow. This is not thread safe.
     *
     * @param label   the label of the workflow
     * @param dir     the base directory specified by the user.
     * @param user    the username of the user.
     * @param vogroup the vogroup to which the user belongs to.
     * @param timestampBased boolean indicating whether to have a timestamp based dir or not
     *
     * @return  the directory name created relative to the base directory passed
     *          as input.
     *
     * @throws IOException in case of unable to create submit directory.
     */
    protected String createSubmitDirectory( String label,
                                            String dir,
                                            String user,
                                            String vogroup,
                                            boolean timestampBased ) throws IOException {
        File base = new File( dir );
        StringBuffer result = new StringBuffer();

        //do a sanity check on the base
        sanityCheck( base );

        //add the user name if possible
        base = new File( base, user );
        result.append( user ).append( File.separator );

        //add the vogroup
        base = new File( base, vogroup );
        sanityCheck( base );
        result.append( vogroup ).append( File.separator );

        //add the label of the DAX
        base = new File( base, label );
        sanityCheck( base );
        result.append( label ).append( File.separator );

        //create the directory name
        StringBuffer leaf = new StringBuffer();
        if( timestampBased ){
            leaf.append( mPegasusPlanOptions.getDateTime( mProps.useExtendedTimeStamp() ) );
        }
        else{
            //get all the files in this directory
            String[] files = base.list( new RunDirectoryFilenameFilter() );
            //find the maximum run directory
            int num, max = 1;
            for( int i = 0; i < files.length ; i++ ){
                num = Integer.parseInt( files[i].substring( RunDirectoryFilenameFilter.SUBMIT_DIRECTORY_PREFIX.length() ) );
                if ( num + 1 > max ){ max = num + 1; }
            }

            //create the directory name
            leaf.append( RunDirectoryFilenameFilter.SUBMIT_DIRECTORY_PREFIX ).append( mNumFormatter.format( max ) );
        }
        result.append( leaf.toString() );
        base = new File( base, leaf.toString() );
        mLogger.log( "Directory to be created is " + base.getAbsolutePath(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
        sanityCheck( base );

        return result.toString();
    }


    /**
     * Checks the destination location for existence, if it can
     * be created, if it is writable etc.
     *
     * @param dir is the new base directory to optionally create.
     *
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheck( File dir ) throws IOException{
        if ( dir.exists() ) {
            // location exists
            if ( dir.isDirectory() ) {
                // ok, isa directory
                if ( dir.canWrite() ) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to dir
                    throw new IOException( "Cannot write to existing directory " +
                                           dir.getPath() );
                }
            } else {
                // exists but not a directory
                throw new IOException( "Destination " + dir.getPath() + " already " +
                                       "exists, but is not a directory." );
            }
        } else {
            // does not exist, try to make it
            if ( ! dir.mkdirs() ) {
                //try to get around JVM bug. JIRA PM-91
                if( dir.getPath().endsWith( "." ) ){
                    //just try to create the parent directory
                    if( !dir.getParentFile().mkdirs() ){
                        //tried everything and failed
                        throw new IOException( "Unable to create  directory " +
                                       dir.getPath() );
                    }
                    return;
                }

                throw new IOException( "Unable to create  directory " +
                                       dir.getPath() );
            }
        }
    }

    /**
     * This method generates a symlink between two files
     *
     * @param source       the file that has to be symlinked
     * @param destination  the destination of the symlink
     *
     * @return boolean indicating if creation of symlink was successful or not
     *
     */
    protected boolean createSymbolicLink( String source, String destination  ) {
        return this.createSymbolicLink( source, destination, false );
    }

    /**
     * This method generates a symlink between two files
     *
     * @param source       the file that has to be symlinked 
     * @param destination  the destination of the symlink
     * @param logErrorToDebug  whether to log messeage to debug or not
     *
     * @return boolean indicating if creation of symlink was successful or not
     *
     */
    protected boolean createSymbolicLink( String source, String destination , boolean logErrorToDebug ) {
        try{
            Runtime rt = Runtime.getRuntime();
            String command = "ln -sf " + source + " " + destination;
            mLogger.log( "Creating symlink between " + source + " " + destination,
                         LogManager.DEBUG_MESSAGE_LEVEL);
            Process p = rt.exec( command, null );

            // set up to read subprogram output
            InputStream is = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);

            // set up to read subprogram error
            InputStream er = p.getErrorStream();
            InputStreamReader err = new InputStreamReader(er);
            BufferedReader ebr = new BufferedReader(err);

            // read output from subprogram
            // and display it

            String s,se=null;
            while ( ((s = br.readLine()) != null) || ((se = ebr.readLine()) != null ) ) {
               if(s!=null){
                   mLogger.log(s,LogManager.DEBUG_MESSAGE_LEVEL);
               }
               else {
                   if( logErrorToDebug ){
                       mLogger.log( se, LogManager.DEBUG_MESSAGE_LEVEL );
                   }
                   else{
                       mLogger.log(se,LogManager.ERROR_MESSAGE_LEVEL );
                   }
               }
            }

            br.close();
            return true;
        }
        catch(Exception ex){
            if( logErrorToDebug ){
                mLogger.log("Unable to create symlink to the log file" , ex,
                            LogManager.DEBUG_MESSAGE_LEVEL );
            }else{
                mLogger.log("Unable to create symlink to the log file" , ex,
                            LogManager.ERROR_MESSAGE_LEVEL);
            }
            return false;
       }

    }

    /**
     * Returns a set containing the paths to the parent dax jobs
     * transient replica catalogs.
     * 
     * @param  job  the job 
     * 
     * @return  Set of paths
     */
    public Set<String> getParentsTransientRC( Job job ){
        Set<String> s = new HashSet();
        
        //get the graph node corresponding to the jobs
        GraphNode node = this.mWorkflow.getNode( job.getID() );
        
        for( GraphNode parent : node.getParents() ){
            Job p = ( Job )parent.getContent();
            if( p instanceof DAXJob ){
                s.add( this.mDAXJobIDToSubmitDirectoryCacheFile.get( p.getID() ));
            }
        }
        
        return s;
    }


}
