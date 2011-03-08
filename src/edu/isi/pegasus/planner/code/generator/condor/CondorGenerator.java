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

import edu.isi.pegasus.common.logging.LoggingKeys;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;

import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.code.POSTScript;
import edu.isi.pegasus.planner.code.GridStartFactory;
import edu.isi.pegasus.planner.code.generator.Abstract;
import edu.isi.pegasus.planner.code.CodeGeneratorFactory;
import edu.isi.pegasus.planner.code.generator.Braindump;

import edu.isi.pegasus.planner.code.generator.NetloggerJobMapper;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.CondorVersion;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.DagInfo;
import edu.isi.pegasus.planner.classes.PCRelation;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.PlannerOptions;

import edu.isi.pegasus.planner.common.PegasusProperties;


import edu.isi.pegasus.common.util.Boolean;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;

import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.Globus;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.namespace.ENV;

import edu.isi.pegasus.planner.partitioner.graph.Adapter;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import org.griphyn.vdl.euryale.VTorInUseException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.Writer;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.HashMap;
/**
 * This class generates the condor submit files for the DAG which has to
 * be submitted to the Condor DagMan.
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @version $Revision$
 */
public class CondorGenerator extends Abstract {

    /**
     * The nice separator, define once, use often.
     */
    public  static final String mSeparator =
        "######################################################################";
    
    
    

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
     * The prefix for DAGMan specific properties
     */
    public static final String DAGMAN_PROPERTIES_PREFIX = "dagman.";
    
    /**
     * The default priority key associated with the stagein jobs.
     */
    public static final int DEFAULT_STAGE_IN_PRIORITY_KEY = 700;
 
    /**
     * The default priority key associated with the inter site transfer jobs.
     */
    public static final int DEFAULT_INTER_SITE_PRIORITY_KEY = 700;
    
    /**
     * The default priority key associated with the create dir jobs.
     */
    public static final int DEFAULT_CREATE_DIR_PRIORITY_KEY = 800;
    
    /**
     * The default priority key associated with the stage out jobs.
     */
    public static final int DEFAULT_STAGE_OUT_PRIORITY_KEY = 900;
    
    /**
     * The default priority key associated with the cleanup jobs.
     */
    public static final int DEFAULT_CLEANUP_PRIORITY_KEY = 1000;
   
    /**
     * Handle to the Transformation Catalog.
     */
    protected TransformationCatalog mTCHandle;

    /**
     * Handle to the pool provider.
     */
    //private PoolInfoProvider mPoolHandle;
    
    /**
     * The handle to the site catalog store.
     */
    private SiteStore mSiteStore;


    /**
     * Specifies the implementing class for the pool interface. Contains
     * the name of the class that implements the pool interface the user has
     * asked at runtime.
     */
    protected String mPoolClass;

   
    /**
     * The file handle to the .dag file. A part of the dag file is printed
     * as we write the submit files, to insert the appropriate postscripts
     * for handling exit codes.
     */
    protected PrintWriter mDagWriter;


    /**
     * The name of the log file in the /tmp directory
     */
    protected String mTempLogFile;

    /**
     * A boolean indicating whether the files have been generated or not.
     */
    protected boolean mDone;

    /**
     * The workflow for which the code has to be generated.
     */
    protected ADag mConcreteWorkflow;

    /**
     * Handle to the Style factory, that is used for this workflow.
     */
    protected CondorStyleFactory mStyleFactory;

    /**
     * The handle to the GridStart Factory.
     */
    protected GridStartFactory mGridStartFactory;

    /**
     * A boolean indicating whether grid start has been initialized or not.
     */
    protected boolean mInitializeGridStart;

    /**
     * The long value of condor version.
     */
    private long mCondorVersion;

    
    
    /**
     * The default constructor.
     */
    public CondorGenerator(){
        super();
        mInitializeGridStart = true;
        mStyleFactory     = new CondorStyleFactory();
        mGridStartFactory = new GridStartFactory();
    }
    
   
     /**
     * Returns the name of the file on the basis of the metadata associated
     * with the DAG.
     * In case of Condor dagman, it is the name of the .dag file that is
     * written out. The basename of the .dag file is dependant on whether the
     * basename prefix has been specified at runtime or not by command line
     * options.
     *
     * @param options  the options passed to the planner.
     * @param name   the name attribute in dax
     * @param index  the index attribute in dax.
     * @param suffix the suffix to be applied at the end.
     *
     * @return the name of the dagfile.
     */
    public static String getDAGFilename( PlannerOptions options, 
                                         String name, 
                                         String index,
                                         String suffix ){
        //constructing the name of the dagfile
        StringBuffer sb = new StringBuffer();
        String bprefix = options.getBasenamePrefix();
        if( bprefix != null){
            //the prefix is not null using it
            sb.append(bprefix);
        }
        else{
            //generate the prefix from the name of the dag
            sb.append( name ).append("-").
                append( index );
        }
        //append the suffix
        sb.append( suffix );



        return sb.toString();

    }

    /**
     * Initializes the Code Generator implementation. Initializes the various
     * writers.
     *
     * @param bag   the bag of initialization objects.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusBag bag ) throws CodeGeneratorException{

        super.initialize( bag );

        //create the base directory recovery
        File wdir = new File(mSubmitFileDir);
        wdir.mkdirs();

        mTCHandle    = bag.getHandleToTransformationCatalog();
        mSiteStore   = bag.getHandleToSiteStore();

        //instantiate and intialize the style factory
        mStyleFactory.initialize( mProps, mSiteStore );
        
        //determine the condor version
        mCondorVersion = CondorVersion.getInstance( mLogger ).numericValue();
        if( mCondorVersion == -1 ){
            mLogger.log( "Unable to determine the version of condor " ,
                          LogManager.WARNING_MESSAGE_LEVEL );
        }
        else{
            mLogger.log( "Condor Version detected is " + mCondorVersion ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }
    }



    /**
     * Generates the code for the concrete workflow in Condor DAGMAN and CondorG
     * input format. Returns only the File object for the DAG file that is written
     * out.
     *
     * @param dag  the concrete workflow.
     *
     * @return the Collection of <code>File</code> objects for the files written
     *         out.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode( ADag dag ) throws CodeGeneratorException{
        DagInfo ndi        = dag.dagInfo;
        Vector vSubInfo    = dag.vJobSubInfos;

        if ( mInitializeGridStart ){
            mConcreteWorkflow = dag;
            mGridStartFactory.initialize( mBag, dag );
            mInitializeGridStart = false;
        }


        CodeGenerator storkGenerator = CodeGeneratorFactory.loadInstance( mBag, "Stork" );

        String className   = this.getClass().getName();
        String dagFileName = getDAGFilename( dag, ".dag" );
        mDone = false;

        File dagFile = null;
        Collection<File> result = new ArrayList(1);
        if (ndi.dagJobs.isEmpty()) {
            //call the callout before returns
            concreteDagEmpty( dagFileName, dag );
            return result ;
        } else {
            //initialize the file handle to the dag
            //file and print it's header
            dagFile = initializeDagFileWriter( dagFileName, ndi );
            result.add( dagFile );

            //write out any category based dagman knobs to the dagman file
            printDagString( this.getCategoryDAGManKnobs( mProps ) );
        }

        
        if( mProps.symlinkCommonLog() ){
            //figure out the logs directory for condor logs
            String dir = mProps.getSubmitLogsDirectory();
            File directory = null;
            if( dir != null ){
                directory = new File( dir );
            
                //try to create this directory if it does not exist
                if( !directory.exists() && !directory.mkdirs() ){
                    //directory does not exist and cannot be created
                    directory = null;
                }
            }
            mLogger.log( "Condor logs directory to be used is " + directory,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        
        
        
            //Create a file in the submit logs directory for the log 
            //and symlink it to the submit directory.
            try{
          
                File f = File.createTempFile( dag.dagInfo.nameOfADag + "-" +
                                              dag.dagInfo.index,".log", 
                                              directory );
                mTempLogFile=f.getAbsolutePath();
            } catch (IOException ioe) {
                mLogger.log( "Error while creating an empty log file in " +
                             "the local temp directory " + ioe.getMessage(),
                              LogManager.ERROR_MESSAGE_LEVEL);
            }
        }


        mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_CODE_GENERATION,
                               LoggingKeys.DAX_ID,
                               dag.getAbstractWorkflowID(),
                               LogManager.DEBUG_MESSAGE_LEVEL);
        
  
        //convert the dax to a graph representation and walk it
        //in a top down manner
        Graph workflow = Adapter.convert( dag );
        SUBDAXGenerator subdaxGen = new SUBDAXGenerator();
        subdaxGen.initialize( mBag, workflow, mDagWriter );
                    
        for( Iterator it = workflow.iterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();
            Job job = (Job)node.getContent();
            
            //only apply priority if job is not associated with a priority
            //beforehand
            if( !job.condorVariables.containsKey( Condor.PRIORITY_KEY ) ){
                int priority = getJobPriority( job, node.getDepth() );
                
                //apply a priority to the job overwriting any preexisting priority
                job.condorVariables.construct( Condor.PRIORITY_KEY,
                                               new Integer(priority).toString() );
                                               
                //log to debug
                StringBuffer sb = new StringBuffer();
                sb.append( "Applying priority of " ).append( priority ).
                        append( " to " ).append( job.getID() );
                mLogger.log( sb.toString(), LogManager.DEBUG_MESSAGE_LEVEL );
            }

                 
            //write out the submit file for each job
            //in addition makes the corresponding
            //entries in the .dag file corresponding
            //to the job and it's postscript
            if ( job.getSiteHandle().equals( "stork" ) ) {
                //write the job information in the .dag file
                StringBuffer dagString = new StringBuffer();
                dagString.append( "DATA " ).append( job.getName() ).append( " " );
                dagString.append( job.getName() ).append( ".stork" );
                printDagString( dagString.toString() );
                storkGenerator.generateCode( dag, job  );
            }
            //for dag jobs we dont need to generate submit file
            else if( job instanceof DAGJob ){
                //SUBDAG EXTERNAL  B  inner.dag
                DAGJob djob = ( DAGJob )job;
                
                //djob.dagmanVariables.checkKeyInNS( Dagman.SUBDAG_EXTERNAL_KEY,
                //                                  djob.getDAGFile() );
                StringBuffer sb = new StringBuffer();
                sb.append( Dagman.SUBDAG_EXTERNAL_KEY ).append( " " ).append( job.getName() ).
                   append( " " ).append( djob.getDAGFile() );
                
                //check if dag needs to run in a specific directory
                String dagDir = djob.getDirectory();
                if( dagDir != null){
                    sb.append( " " ).append( Dagman.DIRECTORY_EXTERNAL_KEY ).
                       append( " " ).append( dagDir );
                }
                
                printDagString( sb.toString() );
            
                printDagString( job.dagmanVariables.toString( job.getName()) );
            }
            else{ //normal jobs and subdax jobs
                
                if( job.typeRecursive() ){
                    
                    job = subdaxGen.generateCode( job  );
                }
                
                if( job != null ){
                    //the submit file for the job needs to be written out
                    //write out a condor submit file
                    generateCode( dag, job  );
                }
                
                //write out all the dagman profile variables associated
                //with the job to the .dag file.
                printDagString( job.dagmanVariables.toString( job.getName()) );

            }
            
            
            mLogger.log("Written Submit file : " +
                        getFileBaseName(job), LogManager.DEBUG_MESSAGE_LEVEL);
        }
        mLogger.logEventCompletion( LogManager.DEBUG_MESSAGE_LEVEL );

        //writing the tail of .dag file
        //that contains the relation pairs
        this.writeDagFileTail( ndi );
        mLogger.log("Written Dag File : " + dagFileName.toString(),
                    LogManager.DEBUG_MESSAGE_LEVEL);

        //symlink the log file to a file in the temp directory if possible
        if( mProps.symlinkCommonLog() ){
            this.generateLogFileSymlink( this.getCondorLogInTmpDirectory(),
                                         this.getCondorLogInSubmitDirectory( dag ) );
        }

        //write out the DOT file
        mLogger.log( "Writing out the DOT file ", LogManager.DEBUG_MESSAGE_LEVEL );
        this.writeDOTFile( getDAGFilename( dag, ".dot"), dag );

        //write out the netlogger file
        mLogger.log( "Written out job.map file", LogManager.DEBUG_MESSAGE_LEVEL );
        this.writeJobMapFile( getDAGFilename( dag, ".job.map"), dag );
        
        //write out the braindump file
        this.writeOutBraindump( dag );
        
        //we are donedirectory
        mDone = true;

        return result;
    }

    /**
     * Generates the code (condor submit file) for a single job.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job    the <code>Job</code> object holding the information about
     *               that particular job.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag, Job job ) throws CodeGeneratorException{
        String dagname  = dag.dagInfo.nameOfADag;
        String dagindex = dag.dagInfo.index;
        String dagcount = dag.dagInfo.count;
        String subfilename = this.getFileBaseName( job );
        String envStr = null;

        //initialize GridStart if required.
        if ( mInitializeGridStart ){
            mConcreteWorkflow = dag;
            mGridStartFactory.initialize( mBag, dag );
            mInitializeGridStart = false;
        }

        //for recursive dax's trigger partition and plan and exit.
        //Commented out to be replaced with SUBDAG rendering.
        //Karan September 10th 2009
        /*
        if ( job.typeRecursive() ){
            String args = job.getArguments();
            PartitionAndPlan pap = new PartitionAndPlan();
            pap.initialize( mBag );
            Collection<File> files = pap.doPartitionAndPlan( args );
            File dagFile = null;
            for( Iterator it = files.iterator(); it.hasNext(); ){
                File f = (File) it.next();
                if ( f.getName().endsWith( ".dag" ) ){
                    dagFile = f;
                    break;
                }
            }

            mLogger.log( "The DAG for the recursive job created is " + dagFile,
                         LogManager.DEBUG_MESSAGE_LEVEL );

            //translate the current job into DAGMan submit file
            Job dagCondorJob = this.constructDAGJob( job.getName(),
                                                         dagFile.getParent(),
                                                         dagFile.getName() );

            //write out the dagCondorJob for it
            mLogger.log( "Generating submit file for DAG " , LogManager.DEBUG_MESSAGE_LEVEL );
            this.generateCode( dag, dagCondorJob );
            
            //setting the dagman variables of dagCondorJob to original job
            //so that the right information is printed in the .dag file
            job.dagmanVariables = dagCondorJob.dagmanVariables;
            return;
        }
        */
        

        // intialize the print stream to the file
        PrintWriter writer = null;
        try{
            writer = getWriter(job);
        }catch(IOException ioe ){
            throw new CodeGeneratorException( "IOException while writing submit file for job " +
                                              job.getName(), ioe);
        }

        //handle the globus rsl parameters
        //for the job from various resources
        handleGlobusRSLForJob( job );

        writer.println(this.mSeparator);
        writer.println("# PEGASUS WMS GENERATED SUBMIT FILE");
        writer.println("# DAG : " + dagname + ", Index = " + dagindex +
                       ", Count = " + dagcount);
        writer.println("# SUBMIT FILE NAME : " + subfilename);
        writer.println(this.mSeparator);

        //figure out the style to apply for a job
        applyStyle( job, writer );

        // handling of  log file is now done through condor profile
        //bwSubmit.println("log = " + dagname + "_" + dagindex + ".log");

        // handle environment settings
        handleEnvVarForJob( job );
        envStr = job.envVariables.toString();
        if (envStr != null) {
            writer.print( job.envVariables );
        }

        // handle Condor variables
        handleCondorVarForJob( job );
        writer.print( job.condorVariables );

        //write the classad's that have the information regarding
        //which Pegasus super node is a node part of, in addition to the
        //release version of Chimera/Pegasus, the jobClass and the
        //workflow id
        ClassADSGenerator.generate( writer, dag, job );

        // DONE
        writer.println("queue");
        writer.println(this.mSeparator);
        writer.println("# END OF SUBMIT FILE");
        writer.println(this.mSeparator);

        // close the print stream to the file (flush)
        writer.close();
        return;
    }


    /**
     * Starts monitoring of the workflow by invoking a workflow monitor daemon
     * tailstatd. The tailstatd is picked up from the default path of
     * $PEGASUS_HOME/bin/tailstatd.
     *
     * @return boolean indicating whether could successfully start the monitor
     *         daemon or not.
     *
     * @throws VTorInUseException in case the method is called before the
     *         submit files have been generated.
     */
    public boolean startMonitoring() throws VTorInUseException{
        //do nothing.
        //earlier the braindump file was generated when this function
        //was called.
        return true;
    }

    /**
     * Resets the Code Generator implementation.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void reset( )throws CodeGeneratorException{
        super.reset();
        mDone = false;
        mInitializeGridStart = true;

    }


    /**
     * Constructs a map with the numbers/values to be passed in the RSL handle
     * for certain pools. The user ends up specifying these through the
     * properties file. The value of the property is of the form
     * poolname1=value,poolname2=value....
     *
     * @param propValue the value of the property got from the properties file.
     *
     * @return Map
     */
    private Map constructMap(String propValue) {
        Map map = new java.util.TreeMap();

        if (propValue != null) {
            StringTokenizer st = new StringTokenizer(propValue, ",");
            while (st.hasMoreTokens()) {
                String raw = st.nextToken();
                int pos = raw.indexOf('=');
                if (pos > 0) {
                    map.put(raw.substring(0, pos).trim(),
                            raw.substring(pos + 1).trim());
                }
            }
        }

        return map;
    }


    /**
     * Constructs a job that plans and submits the partitioned workflow,
     * referred to by a Partition. The main job itself is a condor dagman job
     * that submits the concrete workflow. The concrete workflow is generated by
     * running the planner in the prescript for the job.
     *
     * @param name        the name to be assigned to the job.
     * @param directory   the submit directory where the submit files for the
     *                    partition should reside. this is where the dag file is
     *                    created
     * @param dagBasename the basename of the dag file created.
     *
     * @return the constructed DAG job.
     */
    protected Job constructDAGJob( String name,
                                       String directory,
                                       String dagBasename){
        //for time being use the old functions.
        Job job = new Job();

        //set the logical transformation
        job.setTransformation( CONDOR_DAGMAN_NAMESPACE,
                               CONDOR_DAGMAN_LOGICAL_NAME,
                               null);

        //set the logical derivation attributes of the job.
        job.setDerivation( CONDOR_DAGMAN_NAMESPACE,
                           CONDOR_DAGMAN_LOGICAL_NAME,
                           null );

        //always runs on the submit host
        job.setSiteHandle( "local" );

        //set the partition id only as the unique id
        //for the time being.
//        job.setName(partition.getID());

        //set the logical id for the job same as the partition id.
        job.setName( name );

        List entries;
        TransformationCatalogEntry entry = null;

        //get the path to condor dagman
        try{
            //try to construct the path from the environment
            entry = constructTCEntryFromEnvironment( );
            
            //try to construct from the TC
            if( entry == null ){
                entries = mTCHandle.lookup(job.namespace, job.logicalName,
                                                 job.version, job.getSiteHandle(),
                                                 TCType.INSTALLED);
                entry = (entries == null) ?
                    defaultTCEntry( "local" ) ://construct from site catalog
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
        job.setRemoteExecutable( entry.getPhysicalTransformation() );

        //the job itself is the main job of the super node
        //construct the classad specific information
        job.jobID = job.getName();
        job.jobClass = Job.COMPUTE_JOB;


        //directory where all the dagman related files for the nested dagman
        //reside. Same as the directory passed as an input parameter
        String dir = directory;

        //make the initial dir point to the submit file dir for the partition
        //we can do this as we are running this job both on local host, and scheduler
        //universe. Hence, no issues of shared filesystem or anything.
        job.condorVariables.construct( "initialdir", dir );


        //construct the argument string, with all the dagman files
        //being generated in the partition directory. Using basenames as
        //initialdir has been specified for the job.
        StringBuffer sb = new StringBuffer();

        sb.append(" -f -l . -Debug 3").
           append(" -Lockfile ").append( getBasename( dagBasename, ".lock") ).
           append(" -Dag ").append(  dagBasename );
           //append(" -Rescue ").append( getBasename( dagBasename, ".rescue")).
        
        //specify condor log for condor version less than 7.1.2
        if( mCondorVersion < CondorVersion.v_7_1_2 ){
           sb.append(" -Condorlog ").append( getBasename( dagBasename, ".log"));
        }
        
        //allow for version mismatch as after 7.1.3 condor does tight 
        //checking on dag.condor.sub file and the condor version used
        if( mCondorVersion >= CondorVersion.v_7_1_3 ){
            sb.append( " -AllowVersionMismatch " );
        }

        //for condor 7.1.0 
       sb.append( " -AutoRescue 1 -DoRescueFrom 0 ");
        
       //pass any dagman knobs that were specified in properties file
//       sb.append( this.mDAGManKnobs );

       //put in the environment variables that are required
       job.envVariables.construct("_CONDOR_DAGMAN_LOG",
                                  directory + File.separator + dagBasename + ".dagman.out" );
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
        
        return constructTCEntryFromEnvProfiles( env );
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
        TransformationCatalogEntry entry = null;
        
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
            return entry;
        }
        
        mLogger.log( "Constructing path to dagman on basis of env variable " + key,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        
        entry = new TransformationCatalogEntry();
        entry.setLogicalTransformation( CONDOR_DAGMAN_NAMESPACE,
                                        CONDOR_DAGMAN_LOGICAL_NAME,
                                        null );
        entry.setType( TCType.INSTALLED );
        entry.setResourceId( "local" );
        
        //construct path to condor dagman
        StringBuffer path = new StringBuffer();
        path.append( env.get( key ) ).append( File.separator ).
             append( "bin" ).append( File.separator).
             append( "condor_dagman" );
        entry.setPhysicalTransformation( path.toString() );
        
        return entry;
    }

    /**
     * A covenience method to construct the basename.
     *
     * @param prefix   the first half of basename
     * @param suffix   the latter half of basename
     *
     * @return basename
     */
    protected String getBasename( String prefix, String suffix ){
        StringBuffer sb = new StringBuffer();
        sb.append( prefix ).append( suffix );
        return sb.toString();

    }


    /**
     * Initializes the file handler to the dag file and writes the header to it.
     *
     * @param filename     basename of dag file to be written.
     * @param dinfo        object containing daginfo of type DagInfo .
     *
     * @return the File object for the DAG file.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    protected File initializeDagFileWriter(String filename, DagInfo dinfo)
                                                       throws CodeGeneratorException{
        // initialize file handler

        filename = mSubmitFileDir + "/" + filename;
        File dag = new File(filename);


        try {

            //initialize the print stream to the file
            mDagWriter = new PrintWriter(new BufferedWriter(new
                FileWriter(dag)));

            printDagString(this.mSeparator);
            printDagString("# PEGASUS WMS GENERATED DAG FILE");
            printDagString("# DAG " + dinfo.nameOfADag);
            printDagString("# Index = " + dinfo.index + ", Count = " +
                           dinfo.count);
            printDagString(this.mSeparator);
        } catch (Exception e) {
            throw new CodeGeneratorException( "While writing to DAG FILE " + filename,
                                              e);
        }
        return dag;
    }


    /**
     * Write out the DAGMan knobs for each category the user mentions in
     * the properties.
     *
     * @param properties  the pegasus properties
     *
     * @return the String
     */
    protected String getCategoryDAGManKnobs( PegasusProperties properties ){

        //get all dagman properties
        Properties dagman = properties.matchingSubset( DAGMAN_PROPERTIES_PREFIX, false );
        StringBuffer result = new StringBuffer();
        String newLine = System.getProperty( "line.separator", "\r\n" );

        //iterate through all the properties
        for( Iterator it = dagman.keySet().iterator(); it.hasNext(); ){
            String name = ( String ) it.next();//like bigjob.maxjobs
            //System.out.println( name );

            //figure out whether it is a category property or not
            //really a short cut way of doing it
            //if( (dotIndex = name.indexOf( "." )) != -1 && dotIndex != name.length() - 1 ){
            if( Dagman.categoryRelatedKey( name.toUpperCase() ) ){
                //we have a category and a key
                int dotIndex = name.indexOf( "." );
                String category = name.substring( 0, dotIndex   );//like bigjob
                String knob     = name.substring( dotIndex + 1 );//like maxjobs
                String value    = dagman.getProperty( name );//the value of the property in the properties

                //System.out.println( category + " " + knob + " " + value);
                result.append( knob.toUpperCase( ) ).append( " " ).append( category ).
                       append( " " ).append( value ).append( newLine );
            }
        }

        return result.toString();
    }

    /**
     * Writes out the DOT file in the submit directory.
     *
     * @param filename  basename of dot file to be written .
     * @param dag       the <code>ADag</code> object.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    protected void writeDOTFile( String filename, ADag dag )
                                                       throws CodeGeneratorException{
        // initialize file handler

        filename = mSubmitFileDir + File.separator + filename;


        try {
            Writer stream = new PrintWriter( new BufferedWriter ( new FileWriter( filename ) ) );
            dag.toDOT( stream, null );
            stream.close();

        } catch (Exception e) {
            throw new CodeGeneratorException( "While writing to DOT FILE " + filename,
                                              e);
        }

    }

    /**
     * Writes out the job map file in the submit directory.
     *
     * @param filename  basename of dot file to be written .
     * @param dag       the <code>ADag</code> object.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    protected void writeJobMapFile( String filename, ADag dag )
                                                       throws CodeGeneratorException{
        // initialize file handler

        filename = mSubmitFileDir + File.separator + filename;


        try {
            Writer stream = new PrintWriter( new BufferedWriter ( new FileWriter( filename ) ) );
            NetloggerJobMapper njm = new NetloggerJobMapper( mLogger );
            njm.writeOutMappings( stream, dag );
            stream.close();

        } catch (Exception e) {
            throw new CodeGeneratorException( "While writing to DOT FILE " + filename,
                                              e);
        }

    }


    /**
     * It writes the relations making up the  DAG in the dag file and and closes
     * the file handle to it.
     *
     * @param dinfo   object containing daginfo of type DagInfo.
     *
     * @throws CodeGeneratorException
     */
    protected void writeDagFileTail(DagInfo dinfo) throws CodeGeneratorException{
        try {

            // read the contents from the Daginfo object and
            //print out the parent child relations.

            for (Enumeration dagrelationsenum = dinfo.relations.elements();
                 dagrelationsenum.hasMoreElements(); ) {
                PCRelation pcrl = (PCRelation) dagrelationsenum.nextElement();
                printDagString("PARENT " + pcrl.parent + " CHILD " + pcrl.child);
            }

            printDagString(this.mSeparator);
            printDagString("# End of DAG");
            printDagString(this.mSeparator);

            // close the print stream to the file
            mDagWriter.close();

        } catch (Exception e) {
            throw new CodeGeneratorException( "Error Writing to Dag file " + e.getMessage(),
                                              e );
        }

    }

    /**
     * Writes a string to the dag file. When calling this function the
     * file handle to file is already initialized.
     *
     * @param  str   The String to be printed to the dag file.
     *
     * @throws CodeGeneratorException
     */
    protected void printDagString(String str) throws CodeGeneratorException{
        try {
            mDagWriter.println(str);
        } catch (Exception e) {
            throw new CodeGeneratorException( "Writing to Dag file " + e.getMessage(),
                                              e );
        }

    }


    /**
     * Returns the name of Condor log file in a tmp directory that is created
     * if generation of symlink for condor logs is turned on.
     * 
     * @return  the name of the log file.
     */
    protected String getCondorLogInTmpDirectory(){
       return this.mTempLogFile;
    }
    
    
    /**
     * Returns the path to the condor log file in the submit directory.
     * It can be a symlink.
     *
     * @param dag  the concrete workflow.
     *
     * @return the path to condor log file in the submit directory.
     */
    protected String getCondorLogInSubmitDirectory( ){
        return this.getCondorLogInSubmitDirectory( this.mConcreteWorkflow );
    }
    
    /**
     * Returns the path to the condor log file in the submit directory.
     * It can be a symlink.
     *
     * @param dag  the concrete workflow.
     *
     * @return the path to condor log file in the submit directory.
     */
    protected String getCondorLogInSubmitDirectory( ADag dag ){
        StringBuffer sb = new StringBuffer();
        sb.append(this.mSubmitFileDir)
           .append(File.separator);

       String bprefix = mPOptions.getBasenamePrefix();
       if( bprefix != null){
           //the prefix is not null using it
           sb.append(bprefix);
       }
       else{
           //generate the prefix from the name of the dag
           sb.append(dag.dagInfo.nameOfADag).append("-").
               append(dag.dagInfo.index);
       }
       //append the suffix
       sb.append(".log");
       return sb.toString();
    }

    
    /**
     * Returns a Map containing additional braindump entries that are specific
     * to a Code Generator. 
     * 
     * @param workflow  the executable workflow
     * 
     * @return Map containing entries for dag and condor_log
     */
    public  Map<String, String> getAdditionalBraindumpEntries( ADag workflow ) {
        Map entries = new HashMap();
        entries.put( Braindump.GENERATOR_TYPE_KEY, "dag" );
        entries.put( "dag", this.getDAGFilename( workflow, ".dag") );
        entries.put( "condor_log", this.getCondorLogInSubmitDirectory( workflow ) );
        return entries;
    }
    

    /**
     * This method generates a symlink to the actual log file written in the
     * local temp directory. The symlink is placed in the dag directory.
     *
     * @param logFile the full path to the log file.
     * @param symlink the full path to the symlink.
     *
     * @return boolean indicating if creation of symlink was successful or not
     */
    protected boolean generateLogFileSymlink(String logFile, String symlink) {
        try{
            Runtime rt = Runtime.getRuntime();
            String command = "ln -s " +logFile + " " + symlink;
            mLogger.log("Creating symlink to the log file in the local temp directory\n"
                        + command ,LogManager.DEBUG_MESSAGE_LEVEL);
            Process p = rt.exec(command,null);

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
                   mLogger.log(se,LogManager.ERROR_MESSAGE_LEVEL );
               }
            }

            br.close();
            return true;
        }
        catch(Exception ex){
            mLogger.log("Unable to create symlink to the log file" , ex,
                        LogManager.ERROR_MESSAGE_LEVEL);
            return false;
       }

    }


    /**
     * Returns the name of the file on the basis of the metadata associated
     * with the DAG.
     * In case of Condor dagman, it is the name of the .dag file that is
     * written out. The basename of the .dag file is dependant on whether the
     * basename prefix has been specified at runtime or not by command line
     * options.
     *
     * @param dag    the dag for which the .dag file has to be created.
     * @param suffix the suffix to be applied at the end.
     *
     * @return the name of the dagfile.
     */
    protected String getDAGFilename( ADag dag, String suffix ){
        return getDAGFilename( mPOptions,
                               dag.dagInfo.nameOfADag,
                               dag.dagInfo.index,
                               suffix );
    }
    
   

    

    /**
     * Returns the basename of the file, that contains the output of the
     * dagman while running the dag generated for the workflow.
     * The basename of the .out file is dependant on whether the
     * basename prefix has been specified at runtime or not by command line
     * options.
     *
     * @param dag  the DAG containing the concrete workflow
     *
     * @return the name of the dagfile.
     */
    protected String getDAGMANOutFilename( ADag dag ){
        //constructing the name of the dagfile
        StringBuffer sb = new StringBuffer();
        String bprefix = mPOptions.getBasenamePrefix();
        if( bprefix != null){
            //the prefix is not null using it
            sb.append(bprefix);
        }
        else{
            //generate the prefix from the name of the dag
            sb.append(dag.dagInfo.nameOfADag).append("-").
                append(dag.dagInfo.index);
        }
        //append the suffix
        sb.append(".dag.dagman.out");

        return sb.toString();

    }



    /**
     * A callout method that dictates what needs to be done in case the concrete
     * plan that is generated is empty.
     * It just logs a message saying the plan is empty.
     *
     * @param filename  Filename of the dag to be written of type String.
     * @param dag       the concrete dag that is empty.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    protected void concreteDagEmpty(String filename, ADag dag)
                                                 throws CodeGeneratorException{
        StringBuffer sb = new StringBuffer();
        sb.append( "The concrete plan generated contains no nodes. ").
           append( "\nIt seems that the output files are already at the output pool" );

        mLogger.log( sb.toString(), LogManager.INFO_MESSAGE_LEVEL );

   }



    /**
     * It updates/adds the condor variables that are  got through the Dax with
     * the values specified in the properties file, pool config file or adds some
     * variables internally. In case of clashes of Condor variables from
     * various sources the following order is followed,property file, pool config
     * file and then dax.
     *
     * @param job  The Job object containing the information about the job.
     *
     *
     * @throws CodeGeneratorException
     */
    protected void handleCondorVarForJob(Job job) throws CodeGeneratorException{
        Condor cvar = job.condorVariables;

        String key = null;
        String value = null;

        //put in the classad expression for the values
        //construct the periodic_release and periodic_remove
        //values only if their final computed values are > 0
        this.populatePeriodicReleaseAndRemoveInJob( job  );

        // have to change this later maybe
        key = "notification";
        value = (String) cvar.removeKey(key);
        if (value == null) {
            cvar.construct(key, "NEVER");
        } else {
            cvar.construct(key, value);

            //check if transfer_executable was set to
            //true by the user at runtime
        }
        key = "transfer_executable";
        if (cvar.containsKey(key)) {
            //we do not put in the default value
        } else {
            // we assume pre-staged executables through the GVDS
            cvar.construct(key, "false");

        }

        key = "copy_to_spool";
        if (cvar.containsKey(key)) {
            //we do not put in the default value
        } else
            // no sense copying files to spool for globus jobs
            // and is mandatory for the archstart stuff to work
            // for local jobs
            cvar.construct(key, "false");

        //construct the log file for the submit job
        key = "log";
        if(!cvar.containsKey(key)){
            //we put in the default value
            //cvar.construct("log",dagname + "_" + dagindex + ".log");
            cvar.construct("log",this.getCondorLogInSubmitDirectory( ) );
        }

        //also add the information as for the submit event trigger
        //for mei retry mechanism
        cvar.construct("submit_event_user_notes","pool:" + job.executionPool);


        //on the basis of the
        //transfer_executable key do some magic
// No longer handled here. Handled in Kickstart.java
// Karan Vahi January 16, 2008
//        handleTransferOfExecutable(job);

        //correctly quote the arguments according to
        //Condor Quoting Rules.
        String args = (String) job.condorVariables.get("arguments");
        if( mProps.useCondorQuotingForArguments() && args != null){
            try {
                mLogger.log("Unquoted arguments are " + args,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                //insert a comment for the old args
                //job.condorVariables.construct("#arguments",args);
                args = CondorQuoteParser.quote(args, true);
                job.condorVariables.construct("arguments", args);
                mLogger.log("Quoted arguments are " + args,
                            LogManager.DEBUG_MESSAGE_LEVEL);
            }
            catch (CondorQuoteParserException e) {
                throw new RuntimeException("CondorQuoting Problem " +
                                           e.getMessage());
            }
        }


        return;

    }

    
    /**
     * Populates the periodic release and remove values in the job.
     * If an integer value is specified it is used to construct the default
     * expression, else the value specified in the profiles is used as is.
     * 
     * The default expression for periodic_release and periodic_remove is
     * <pre>
     *   periodic_release = (NumSystemHolds <= releasevalue)
     *   periodic_remove = (NumSystemHolds > removevalue)
     * </pre>
     * where releasevalue is value of condor profile periodic_release
     * and   removevalue  is value of condor profile periodic_remove
     * 
     * @param job  the job object.
     */
    public void populatePeriodicReleaseAndRemoveInJob( Job job ){

        //Karan Oct 19, 2005. The values in property file
        //should only be treated as default. Need to reverse
        //below.

        //get the periodic release values always a default
        //value is got if not specified.
        String releaseval = (String) job.condorVariables.get( Condor.PERIODIC_RELEASE_KEY );
        releaseval = (releaseval == null) ?
            //put in default value
            "3" :
            //keep the one from profiles or dax
            releaseval;
        

        String removeval = (String) job.condorVariables.get( Condor.PERIODIC_REMOVE_KEY );
        removeval = (removeval == null) ?
            //put in default value
            "3" :
            //keep the one from profiles or dax
            removeval;


        int removeint  = this.getNaturalNumberValue( removeval );
        int releaseint = this.getNaturalNumberValue( releaseval );

        if( removeint > 0 && releaseint > 0 ){
            if( removeint > releaseint ){
                removeval = releaseval;
                //throw a warning down
                mLogger.log(
                    " periodic_remove > periodic_release " +
                    "for job " + 
                     ". Setting periodic_remove=periodic_release",
                    LogManager.WARNING_MESSAGE_LEVEL);
            }
        }

    
        String value = null;
        if( releaseint > 0){
            value = "(NumSystemHolds <= " + releaseint + ")";
            job.condorVariables.construct( Condor.PERIODIC_RELEASE_KEY, value);
        }
        else{
            job.condorVariables.construct( Condor.PERIODIC_RELEASE_KEY, releaseval );
        }

        
        if( removeint > 0){
            value = "(NumSystemHolds > " + removeint + ")";
            job.condorVariables.construct( Condor.PERIODIC_REMOVE_KEY, value );
        }
        else{
            job.condorVariables.construct( Condor.PERIODIC_REMOVE_KEY, removeval );
        }

    
    }
    
    
    /**
     * Returns a natural number value ( > 0 ) if the parameter passed is an integer
     * and greater than zero, else -1
     * 
     * @param value   the String passed
     * 
     * @return
     */
    protected int getNaturalNumberValue( String value ){
        int result = -1;

        try{
            result = Integer.parseInt(value);
        }
        catch( Exception e ){

        }
        return result;
    }

    /**
     * It changes the paths to the executable depending on whether we want to
     * transfer the executable or not. If the transfer_executable is set to true,
     * then the executable needs to be shipped from the submit host meaning the
     * local pool. This function changes the path of the executable to the one on
     * the local pool, so that it can be shipped.
     *
     * @param job the <code>Job</code> containing the job description.
     *
     * @throws CodeGeneratorException
     */
    protected void handleTransferOfExecutable(Job sinfo) throws CodeGeneratorException{
        Condor cvar = sinfo.condorVariables;

        if (!cvar.getBooleanValue("transfer_executable")) {
            //the executable paths are correct and
            //point to the executable on the remote pool
            return;
        }

        SiteCatalogEntry site = mSiteStore.lookup( sinfo.getSiteHandle() );
        String gridStartPath = site.getKickstartPath();

        if (gridStartPath == null) {
            //not using grid start
            //we need to stage in the executable from
            //the local pool. Not yet implemented
            mLogger.log("At present only the transfer of gridstart is supported",
                        LogManager.ERROR_MESSAGE_LEVEL);
            return;
        } else {
            site =  mSiteStore.lookup( "local" );
            gridStartPath = site.getKickstartPath();
            if (gridStartPath == null) {
                mLogger.log(
                    "Gridstart needs to be shipped from the submit host to pool" +
                    sinfo.executionPool + ".\nNo entry for it in pool local",
                    LogManager.ERROR_MESSAGE_LEVEL);
                throw new CodeGeneratorException( "GridStart needs to be shipped from submit host to site " +
                                                  sinfo.getSiteHandle() );

            } else {
                //the jobs path to executable is updated
                //by the path on the submit host
                cvar.construct("executable", gridStartPath);

                //the arguments to gridstart need to be
                //appended with the true remote directory
                String args = (String) cvar.removeKey("arguments");
                args = " -w " +
                    mSiteStore.getWorkDirectory( sinfo ) +
                    " " + args;
                cvar.construct("arguments", args);

                //we have to remove the remote_initial dir for it.
                //as this is required for the LCG sites
                //Actually this should be done thru a LCG flag
                cvar.removeKey("remote_initialdir");

            }

        }
    }

    /**
     * Applies a submit file style to the job, according to the fact whether
     * the job has to be submitted directly to condor or to a remote jobmanager
     * via CondorG and GRAM.
     * If no style is associated with the job, then for the job running on
     * local site, condor style is applied. For a job running on non local sites,
     * globus style is applied if none is associated with the job.
     *
     * @param job  the job on which the style needs to be applied.
     * @param writer the PrintWriter stream to the submit file for the job.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    protected void applyStyle( Job job, PrintWriter writer )
                                                  throws CodeGeneratorException{

        //load  the appropriate style for the job
        CondorStyle cs = mStyleFactory.loadInstance( job );
        String style   = (String)job.vdsNS.get( Pegasus.STYLE_KEY );

        boolean isGlobus =  style.equals( Pegasus.GLOBUS_STYLE ) ? true : false;

        //apply the appropriate style on the job.
        if( job instanceof AggregatedJob ){
            cs.apply( (AggregatedJob)job  );
        }
        else{
            cs.apply( job );
        }

        //handle GLOBUS RSL if required, and stdio appropriately
        String rslString = job.globusRSL.toString();
        rslString += gridstart( writer, job, isGlobus );
        if( isGlobus ){
            //only for CondorG style does RSL make sense
            //instead of writing directly
            //incorporate as condor profile
            //job.condorVariables.construct( "globusrsl", rslString );
            job.condorVariables.construct( "globusrsl", job.globusRSL.toString() );

        }
    }


    /**
     * It updates/adds the environment variables that are got through the Dax with
     * the values specified in the properties file, pool config file or adds some
     * variables internally. In case of clashes of environment variables from
     * various sources the following order is followed,property file,
     * transformation catalog, pool config file and then dax.
     * At present values are not picked from the properties file.
     *
     * @param job  The Job object containing the information about the job.
     */
    protected void handleEnvVarForJob(Job sinfo) {

    }

    /**
     * It updates/adds the the Globus RSL parameters got through the dax that are
     * in Job object. In addition inserts the additional rsl attributes
     * that can be specified in the properties file or the pool config files in
     * the profiles tags. In case of clashes of RSL attributes from various
     * sources the following order is followed,property file, pool config file
     * and then dax.
     *
     * @param job  The Job object containing the information about the job.
     */
    protected void handleGlobusRSLForJob(Job sinfo) {
        Globus rsl = sinfo.globusRSL;

        String key = null;
        String value = null;

        //Getting all the rsl parameters specified
        //in dax
        /*
                 if (job.globusRSL != null) {
            rsl.putAll(job.globusRSL);

            // 19-05 jsv: Need to change to {remote_}initialdir commands
            // allow TR to spec its own directory
                 }
         */

        
        // check job type, unless already specified
        // Note, we may need to adjust this again later
        if (!rsl.containsKey("jobtype")) {
            rsl.construct("jobtype", "single");
        }
        //sanitize jobtype on basis of jobmanager
        //Karan Sept 12,2005
        //This is to overcome specifically Duncan's problem
        //while running condor universe standard jobs.
        //For that the jobtype=condor needs to be set for the compute
        //job. This is set in the site catalog, but ends up
        //breaking transfer jobs that are run on jobmanager-fork
        String jmURL = sinfo.globusScheduler;
        if(jmURL != null && jmURL.endsWith("fork")){
            rsl.construct("jobtype","single");
        }


    }

    /**
     * Computes the priority for a job based on job type and depth in the workflow
     * 
     * @param job       the job whose priority needs to be computed
     * @param depth     the depth in the workflow
     * 
     * @return
     */
    protected int getJobPriority(Job job, int depth) {
        int priority = 0;
        
        int type = job.getJobType();
        switch ( type ){
            case Job.CREATE_DIR_JOB:
                priority = CondorGenerator.DEFAULT_CREATE_DIR_PRIORITY_KEY;
                break;
                
            case Job.CLEANUP_JOB:
                priority = CondorGenerator.DEFAULT_CLEANUP_PRIORITY_KEY;
                break;
                
            case Job.STAGE_IN_JOB:
                priority = CondorGenerator.DEFAULT_STAGE_IN_PRIORITY_KEY;
                break;
                
            case Job.INTER_POOL_JOB:
                priority = CondorGenerator.DEFAULT_INTER_SITE_PRIORITY_KEY;
                break;
                
            case Job.STAGE_OUT_JOB:
                priority = CondorGenerator.DEFAULT_STAGE_OUT_PRIORITY_KEY;
                break;
             
            default:
                //compute on the basis of the depth
                priority = depth * 10;
                break;
        }
        
        return priority;
    }

   
    /**
     * This function creates the stdio handling with and without gridstart.
     * Please note that gridstart will become the default by end 2003, and
     * no gridstart support will be phased out.
     *
     * @param writer is an open stream for the Condor submit file.
     * @param job is the job information structure.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     *
     * @return A possibly empty string which contains things that
     *         need to be added to the "globusrsl" clause. The return
     *         value is only of interest for isGlobusJob==true calls.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    private String gridstart(PrintWriter writer,
                             Job job,
                             boolean isGlobusJob) throws CodeGeneratorException {
        //To get the gridstart/kickstart path on the remote
        //pool, querying with entry for vanilla universe.
        //In the new format the gridstart is associated with the
        //pool not pool, condor universe
//        SiteInfo site = mPoolHandle.getPoolEntry(job.executionPool,
//                                                 Condor.VANILLA_UNIVERSE);
        
        SiteCatalogEntry site = mSiteStore.lookup( job.getSiteHandle() );
        String gridStartPath = site.getKickstartPath();

        StringBuffer rslString = new StringBuffer();
        String jobName = job.jobName;
        String script = null;
        job.dagmanVariables.checkKeyInNS(Dagman.JOB_KEY,
                                         getFileBaseName(job));
        
        
        //remove the prescript arguments key
        //should be already be set to the prescript key
//        //NO NEED TO REMOVE AS WE ARE HANDLING CORRECTLY IN DAGMAN NAMESPACE
//        //NOW. THERE THE ARGUMENTS AND KEY ARE COMBINED. Karan May 11,2006
//        //job.dagmanVariables.removeKey(Dagman.PRE_SCRIPT_ARGUMENTS_KEY);

//        script = (String)job.dagmanVariables.removeKey(Dagman.PRE_SCRIPT_KEY);
//        if(script != null){
//            //put in the new key with the prescript
//            job.dagmanVariables.checkKeyInNS(PRE_SCRIPT_KEY,script);
//        }


        //condor streaming is now for both grid and non grid universe jobs
        // we always put in the streaming keys. they default to false
        boolean stream = Boolean.parse( (String)job.condorVariables.removeKey( Condor.STREAM_STDERR_KEY ),
                                        false );
        if ( stream ) {
            //we want it to be staged
            writer.println("stream_error  = true");
        }
        else {
            writer.println("stream_error  = false");
        }
        
        stream = Boolean.parse( (String)job.condorVariables.removeKey( Condor.STREAM_STDOUT_KEY  ),
                                        false );
        if ( stream ) {
            //we want it to be staged
            writer.println("stream_output = true" );
        }
        else{  //we want it to be staged
            writer.println("stream_output = false" );
        }

        GridStart gridStart = mGridStartFactory.loadGridStart( job, gridStartPath );

        //enable the job
        boolean enable = false;
        if( job instanceof AggregatedJob ){
            enable = gridStart.enable( (AggregatedJob) job, isGlobusJob );
        }
        else{
            enable = gridStart.enable( job,isGlobusJob );
        }
        if( !enable ){
            String msg = "Job " +  jobName + " cannot be enabled by " +
                         gridStart.shortDescribe() + " to run at " +
                         job.getSiteHandle();
            mLogger.log( msg, LogManager.FATAL_MESSAGE_LEVEL );
            throw new CodeGeneratorException( msg );
        }


        //apply the appropriate POSTScript
        POSTScript ps       = mGridStartFactory.loadPOSTScript( job, gridStart );
        boolean constructed = ps.construct( job, Dagman.POST_SCRIPT_KEY );

        //write out all the dagman profile variables associated
        //with the job to the .dag file.
//        printDagString(job.dagmanVariables.toString(jobName));
        
        return rslString.toString();
    }



}
