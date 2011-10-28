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

package edu.isi.pegasus.planner.code.gridstart;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.Proxy;
import edu.isi.pegasus.common.util.S3cfg;

import java.io.BufferedReader;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.code.GridStart;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;


import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Pegasus;

import edu.isi.pegasus.planner.transfer.sls.SLSFactory;
import edu.isi.pegasus.planner.transfer.SLS;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.common.util.Separator;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.List;

/**
 * This class launches all the jobs using seqexec. It wraps all
 * invocations in kickstart. 
 * 
 * The seqexec input file for the compute jobs contains the commands to
 * 
 * <pre>
 * 1) create directory on worker node
 * 2) fetch input data files
 * 3) execute the job
 * 4) transfer the output data files
 * 5) cleanup the directory
 * </pre>
 *
 * 
 * The following property should be set to false to disable the staging of the 
 * SLS files via the first level staging jobs
 * 
 * <pre>
 * pegasus.transfer.stage.sls.file     false
 * </pre>
 *
 * To enable this implementation at runtime set the following property
 * <pre>
 * pegasus.gridstart SeqExec
 * </pre>
 *
 * 
 * @author Karan Vahi
 * @version $Revision$
 */

public class SeqExec implements GridStart {
    private PegasusBag mBag;
    private ADag mDAG;

    /**
     * The basename of the class that is implmenting this. Could have
     * been determined by reflection.
     */
    public static final String CLASSNAME = "SeqExec";

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "seqexec";

    /**
     * The marker to designate a line in the input file reserved for 
     * monitord purposes.
     */
    public static final String MONITORD_COMMENT_MARKER =
            edu.isi.pegasus.planner.cluster.aggregator.Abstract.MONITORD_COMMENT_MARKER;
    
    /**
     * The complete transformation name for pegasus transfer
     */
    public static final String PEGASUS_TRANSFER_COMPLETE_TRANSFORMATION_NAME = "pegasus::pegasus-transfer";

    /**
     * The environment variable that tells seqexec to run a setup job.
     */
    public static final String  SEQEXEC_SETUP_ENV_VARIABLE = " SEQEXEC_SETUP";
    

    /**
     * The environment variable that tells seqexec to run a cleanup job.
     */
    public static final String  SEQEXEC_CLEANUP_ENV_VARIABLE = " SEQEXEC_CLEANUP";


    /**
     * The LogManager object which is used to log all the messages.
     */
    protected LogManager mLogger;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The submit directory where the submit files are being generated for
     * the workflow.
     */
    protected String mSubmitDir;

    /**
     * The argument string containing the arguments with which the exitcode
     * is invoked on kickstart output.
     */
//    protected String mExitParserArguments;

    /**
     * A boolean indicating whether to generate lof files or not.
     */
    protected boolean mGenerateLOF;

    /**
     * A boolean indicating whether to have worker node execution or not.
     */
    protected boolean mWorkerNodeExecution;

    /**
     * The handle to the SLS implementor
     */
    protected SLS mSLS;

    /**
     * The options passed to the planner.
     */
    protected PlannerOptions mPOptions;


    /**
     * Handle to the site catalog store.
     */
    //protected PoolInfoProvider mSiteHandle;
    protected SiteStore mSiteStore;

    /**
     * An instance variable to track if enabling is happening as part of a clustered job.
     * See Bug 21 comments on Pegasus Bugzilla
     */
    protected boolean mEnablingPartOfAggregatedJob;

    /**
     * Handle to kickstart GridStart implementation.
     */
    private Kickstart mKickstartGridStartImpl;

   
     /**
     * Handle to Transformation Catalog.
     */
    private TransformationCatalog mTCHandle;

    /**
     * Boolean to track whether to stage sls file or not
     */
    protected boolean mStageSLSFile;

    /**
     * The path to local user proxy.
     */
    protected String mLocalUserProxy;

    /**
     * The basename of the proxy
     */
    protected String mLocalUserProxyBasename;

    /**
     * The path to local user s3cfg.
     */
    protected String mLocalS3cfg;

    /**
     * The basename of the s3cfg
     */
    protected String mLocalS3cfgBasename;


    /**
     * Initializes the GridStart implementation.
     *
     *  @param bag   the bag of objects that is used for initialization.
     * @param dag   the concrete dag so far.
     */
    public void initialize( PegasusBag bag, ADag dag ){
        mBag       = bag;
        mDAG       = dag;
        mLogger    = bag.getLogger();
        mSiteStore   = bag.getHandleToSiteStore();
        mPOptions  = bag.getPlannerOptions();
        mSubmitDir = mPOptions.getSubmitDirectory();
        mProps     = bag.getPegasusProperties();
        mGenerateLOF  = mProps.generateLOFFiles();
        mTCHandle  = bag.getHandleToTransformationCatalog();

        mWorkerNodeExecution = mProps.executeOnWorkerNode();
        if( mWorkerNodeExecution ){
            //load SLS
            mSLS = SLSFactory.loadInstance( bag );
        }
        mEnablingPartOfAggregatedJob = false;
        mKickstartGridStartImpl = new Kickstart();
        mKickstartGridStartImpl.initialize( bag, dag );

        mStageSLSFile = mProps.stageSLSFilesViaFirstLevelStaging();
        mLocalUserProxy = Proxy.getPathToUserProxy(bag);

        //set the path to user proxy only if the proxy exists
        if( !new File( mLocalUserProxy).exists() ){
            mLogger.log( "The user proxy does not exist - " + mLocalUserProxy,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            mLocalUserProxy = null;
        }
        mLocalUserProxyBasename = (mLocalUserProxy == null) ?
                                  null :
                                  new File(mLocalUserProxy).getName();

        mLocalS3cfg = S3cfg.getPathToS3cfg(bag);
        //set the path to user proxy only if the proxy exists
        if( mLocalS3cfg != null && !new File(mLocalS3cfg).exists() ){
            mLogger.log( "The s3cfg file does not exist - " + mLocalUserProxy,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            mLocalS3cfg = null;
        }

        mLocalS3cfgBasename = (mLocalS3cfg == null) ?
                                  null :
                                  new File(mLocalS3cfg).getName();
    }
    
    /**
     * Enables a job to run on the grid. This also determines how the
     * stdin,stderr and stdout of the job are to be propogated.
     * To grid enable a job, the job may need to be wrapped into another
     * job, that actually launches the job. It usually results in the job
     * description passed being modified modified.
     *
     * @param job  the <code>Job</code> object containing the job description
     *             of the job that has to be enabled on the grid.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     *
     * @return boolean true if enabling was successful,else false.
     */
    public boolean enable( AggregatedJob job,boolean isGlobusJob){
        boolean result = true;
    
        //lets enable the aggregated job via kickstart first
        //to create the SLS files and other things
        result = this.mKickstartGridStartImpl.enable(job, isGlobusJob);
        
         //For JIRA PM-380
        if( mWorkerNodeExecution ){
            //lets enable the AggregatedJob directly for worker node execution
            //create a SLS files for the clustered jobs.
            StringBuffer args = new StringBuffer();
            this.mKickstartGridStartImpl.enableForWorkerNodeExecution( job, args , false );
        }
         
        
        if( job instanceof AggregatedJob && !mSLS.doesCondorModifications()){
            if( job.getJobType() == Job.COMPUTE_JOB /*||
                job.getJobType() == Job.STAGED_COMPUTE_JOB*/ ){

                AggregatedJob clusteredJob = (AggregatedJob) job;
                enableClusteredJobForWorkerNodeExecution( clusteredJob, isGlobusJob );
            }
        }
        else{
            throw new RuntimeException( "Clustered Job is not of type compute " + job.getID() );
        }

        return result;
    }


    /**
     * Enables a job to run on the grid by launching it directly. It ends
     * up running the executable directly without going through any intermediate
     * launcher executable. It connects the stdio, and stderr to underlying
     * condor mechanisms so that they are transported back to the submit host.
     *
     * @param job  the <code>Job</code> object containing the job description
     *             of the job that has to be enabled on the grid.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     *
     * @return boolean true if enabling was successful,else false in case when
     *         the path to kickstart could not be determined on the site where
     *         the job is scheduled.
     */
    public boolean enable(Job job, boolean isGlobusJob) {
        //take care of relative submit directory if specified
        String submitDir = mSubmitDir + mSeparator;
//        String submitDir = getSubmitDirectory( mSubmitDir , job) + mSeparator;

        //the executable path and arguments are put
        //in the Condor namespace and not printed to the
        //file so that they can be overriden if desired
        //later through profiles and key transfer_executable
        //construct(job,"executable", handleTransferOfExecutable( job ) );

        //sanity check for the arguments
        //if(job.strargs != null && job.strargs.length() > 0){
          //  construct(job, "arguments", job.strargs);
        //}

        // handle stdin
        if (job.stdIn.length() > 0) {
            construct(job,"input",submitDir + job.stdIn);
            if (isGlobusJob) {
                //this needs to be true as you want the stdin
                //to be transfered to the remote execution
                //pool, as in case of the transfer script.
                //it needs to be set if the stdin is already
                //prepopulated at the remote side which
                //it is not.
                construct(job,"transfer_input","true");
            }
        }

        if (job.stdOut.length() > 0) {
            //handle stdout
            construct(job,"output",job.stdOut);
            if (isGlobusJob) {
                construct(job,"transfer_output","false");
            }
        } else {
            // transfer output back to submit host, if unused
            construct(job,"output",submitDir + job.jobName + ".out");
            if (isGlobusJob) {
                construct(job,"transfer_output","true");
            }
        }

        if (job.stdErr.length() > 0) {
            //handle stderr
            construct(job,"error",job.stdErr);
            if (isGlobusJob) {
                construct(job,"transfer_error","false");
            }
        } else {
            // transfer error back to submit host, if unused
            construct(job,"error",submitDir + job.jobName + ".err");
            if (isGlobusJob) {
                construct(job,"transfer_error","true");
            }
        }
        

        //we enable the job via kickstart always for now
        //for the jobs that are being enabled as part of
        //a clustered job in the clustering modeul
        if( this.mEnablingPartOfAggregatedJob ){
            return mKickstartGridStartImpl.enable( job, isGlobusJob );
        }


        //consider case for non worker node execution first
        if( !mWorkerNodeExecution ){
            //we enable the job via kickstart always for now
            //for the jobs that are being enabled as part of
            //a clustered job in the clustering modeul
            //if( this.mEnablingPartOfAggregatedJob ){
            //    return mKickstartGridStartImpl.enable( job, isGlobusJob );
            //}
            //else{
                //we need to launch the job via seqexec
                if( job instanceof AggregatedJob ){
                    // job is already clustered.
                    mLogger.log( "Not enabling job " + job.getID() + " as it is already clustered ",
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                    return true;
                }
                else{
                    //for now a single job is launched via kickstart only
                    //no point launching it via seqexec and then kickstart
                    return mKickstartGridStartImpl.enable( job, isGlobusJob );
                }
            //}
        }//end of handling of non worker node execution
        else{
            //handle stuff differently for clustered jobs
            //for non condor modified SLS
            enableForWorkerNodeExecution( job, isGlobusJob );
        }//end of worker node execution



        if( mGenerateLOF ){
            //but generate lof files nevertheless


            //inefficient check here again. just a prototype
            //we need to generate -S option only for non transfer jobs
            //generate the list of filenames file for the input and output files.
            if (! (job instanceof TransferJob)) {
                generateListofFilenamesFile( job.getInputFiles(),
                                             job.getID() + ".in.lof");
            }

            //for cleanup jobs no generation of stats for output files
            if (job.getJobType() != Job.CLEANUP_JOB) {
                generateListofFilenamesFile(job.getOutputFiles(),
                                           job.getID() + ".out.lof");

            }
        }///end of mGenerateLOF

        return true;
    }

    /**
     * Enables jobs for worker node execution.
     *
     *
     *
     * @param job           the job to be enabled.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     */
    private void enableForWorkerNodeExecution(Job job, boolean isGlobusJob ) {

        if( job.getJobType() == Job.COMPUTE_JOB /*||
                 job.getJobType() == Job.STAGED_COMPUTE_JOB */){

            File seqxecIPFile = enableAndGenerateSeqexecInputFile( job, isGlobusJob );
            construct( job,"input", seqxecIPFile.getAbsolutePath() );

            //construct path to seqexec file
            TransformationCatalogEntry entry = getSeqExecTransformationCatalogEntry( job.getSiteHandle() );

            // the arguments are no longer set as condor profiles
            // they are now set to the corresponding profiles in
            // the Condor Code Generator only.
            job.setRemoteExecutable( entry.getPhysicalTransformation() );
            //we want seqexec to fail hard on first error (non-zero exit code or signal death)
            job.setArguments( " -f " );
/*
            construct( job, "executable", entry.getPhysicalTransformation() );
            //we want seqexec to fail hard on first error (non-zero exit code or signal death)
            construct( job, "arguments", " -f " );
 */
        }
        //for all auxillary jobs let kickstart figure what to do
        else{
            mKickstartGridStartImpl.enable( job, isGlobusJob );
        }

    }

    /**
     * Enables a clustered job for worker node execution.  It creates a single
     * input file for the seqexec invocation. The input file contains commands to
     *
     * <pre>
     * 1) create directory on worker node
     * 2) fetch input data files
     * 3) execute the job
     * 4) transfer the output data files
     * 5) cleanup the directory
     * </pre>
     *
     * @param job          the job to be enabled.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     */
    protected void enableClusteredJobForWorkerNodeExecution( AggregatedJob job, boolean isGlobusJob) {
        

        String key = getDirectoryKey(job);

        //always have the remote dir set to /tmp as
        //we are banking on kickstart to change directory
        //for us for compute jobs
        //job.condorVariables.construct(key, "/tmp");

        AggregatedJob clusteredJob = (AggregatedJob) job;
        Job firstJob = clusteredJob.getConstituentJob(0);

        GridStart gs = this.mKickstartGridStartImpl;

        //add the -f option always
        if (!mProps.abortOnFirstJobFailure()) {
            //the clustering module did not add the -f option
            //we add ourselves here
            //we want seqexec to fail hard on first error (non-zero exit code or signal death)


//            construct(job, "arguments", job.getArguments() + " -f ");
            job.setArguments( job.getArguments() + " -f " );
        }

        this.enableAndGenerateSeqexecInputFile( job, isGlobusJob );

    }

    /**
     * It changes the paths to the executable depending on whether we want to
     * transfer the executable or not. Currently, the transfer_executable is only
     * handled for staged compute jobs, where Pegasus is staging the binaries
     * to the remote site.
     * 
     * @param job   the <code>Job</code> containing the job description.
     *
     * @return the path that needs to be set as the executable key. If 
     *         transfer_executable is not set the path to the executable is
     *         returned as is.
     */
    protected String handleTransferOfExecutable( Job job  ) {
        Condor cvar = job.condorVariables;
        String path = job.executable;
        
        if ( cvar.getBooleanValue( "transfer_executable" )) {
            
            //explicitly check for whether the job is a staged compute job or not
            if( job.userExecutablesStagedForJob() ){
                //the executable is being staged to the remote site.
                //all we need to do is unset transfer_executable
                cvar.construct( "transfer_executable", "false" );
            }
            else{
                mLogger.log( "Transfer of Executables in SeqExec GridStart only works for staged computes jobs ",
                             LogManager.ERROR_MESSAGE_LEVEL );
                
            }
        }
        else{
            //the executable paths are correct and
            //point to the executable on the remote pool
        }
        return path;
    }


    
    /**
     * Indicates whether the enabling mechanism can set the X bit
     * on the executable on the remote grid site, in addition to launching
     * it on the remote grid stie
     *
     * @return false, as no wrapper executable is being used.
     */
    public  boolean canSetXBit(){
        return false;
    }

    /**
     * Returns the value of the vds profile with key as Pegasus.GRIDSTART_KEY,
     * that would result in the loading of this particular implementation.
     * It is usually the name of the implementing class without the
     * package name.
     *
     * @return the value of the profile key.
     * @see org.griphyn.cPlanner.namespace.Pegasus#GRIDSTART_KEY
     */
    public  String getVDSKeyValue(){
        return SeqExec.CLASSNAME;
    }


    /**
     * Returns a short textual description in the form of the name of the class.
     *
     * @return  short textual description.
     */
    public String shortDescribe(){
        return SeqExec.SHORT_NAME;
    }

    /**
     * Returns the SHORT_NAME for the POSTScript implementation that is used
     * to be as default with this GridStart implementation.
     *
     * @return  the identifier for the default POSTScript implementation for
     *          kickstart gridstart module.
     *
     * @see Kickstart#defaultPOSTScript() 
     */
    public String defaultPOSTScript(){
        return this.mKickstartGridStartImpl.defaultPOSTScript();
    }

    /**
     * Returns the directory that is associated with the job to specify
     * the directory in which the job needs to run
     * 
     * @param job  the job
     * 
     * @return the condor key . can be initialdir or remote_initialdir
     */
    private String getDirectoryKey(Job job) {
        /*
        String style = (String)job.vdsNS.get( Pegasus.STYLE_KEY );
                    //remove the remote or initial dir's for the compute jobs
                    String key = ( style.equalsIgnoreCase( Pegasus.GLOBUS_STYLE )  )?
                                   "remote_initialdir" :
                                   "initialdir";
         */
        String universe = (String) job.condorVariables.get( Condor.UNIVERSE_KEY );
        return ( universe.equals( Condor.STANDARD_UNIVERSE ) ||
                 universe.equals( Condor.LOCAL_UNIVERSE) ||
                 universe.equals( Condor.SCHEDULER_UNIVERSE ) )?
                "initialdir" :
                "remote_initialdir";
    }


    /**
     * Returns a boolean indicating whether to remove remote directory
     * information or not from the job. This is determined on the basis of the
     * style key that is associated with the job.
     *
     * @param job the job in question.
     *
     * @return boolean
     */
    private boolean removeDirectoryKey(Job job){
        String style = job.vdsNS.containsKey(Pegasus.STYLE_KEY) ?
                       null :
                       (String)job.vdsNS.get(Pegasus.STYLE_KEY);

        //is being run. Remove remote_initialdir if there
        //condor style associated with the job
        //Karan Nov 15,2005
        return (style == null)?
                false:
                style.equalsIgnoreCase(Pegasus.CONDOR_STYLE);

    }

    /**
     * Constructs a condor variable in the condor profile namespace
     * associated with the job. Overrides any preexisting key values.
     *
     * @param job   contains the job description.
     * @param key   the key of the profile.
     * @param value the associated value.
     */
    private void construct(Job job, String key, String value){
        job.condorVariables.construct(key,value);
    }

    /**
     * Writes out the list of filenames file for the job.
     *
     * @param files  the list of <code>PegasusFile</code> objects contains the files
     *               whose stat information is required.
     *
     * @param basename   the basename of the file that is to be created
     *
     * @return the full path to lof file created, else null if no file is written out.
     */
     public String generateListofFilenamesFile( Set files, String basename ){
         //sanity check
         if ( files == null || files.isEmpty() ){
             return null;
         }

         String result = null;
         //writing the stdin file
        try {
            File f = new File( mSubmitDir, basename );
            FileWriter input;
            input = new FileWriter( f );
            PegasusFile pf;
            for( Iterator it = files.iterator(); it.hasNext(); ){
                pf = ( PegasusFile ) it.next();
                input.write( pf.getLFN() );
                input.write( "\n" );
            }
            //close the stream
            input.close();
            result = f.getAbsolutePath();

        } catch ( IOException e) {
            mLogger.log("Unable to write the lof file " + basename, e ,
                        LogManager.ERROR_MESSAGE_LEVEL);
        }

        return result;
     }

     /**
      * Adds contents to an output stream.
      * @param src
      * @param out
      * @throws java.io.IOException
      */
     private void addToFile( File src, PrintWriter out ) throws IOException{

        BufferedReader in = new BufferedReader( new FileReader( src ) );

        String line = null;

        while(( line = in.readLine() ) != null ){
            //System.out.println( line );
            out.println( line );
        }

        in.close();
     }



     /**
      * Adds contents to an output stream.
      * @param src
      * @param out
      * @throws java.io.IOException
      */
     private void addToFile( File src, OutputStream out ) throws IOException{
      
        InputStream in = new FileInputStream(src);
    
        // Transfer bytes from in to out
        byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        in.close();
     }
     
     
     
     /**
     * Returns the directory in which the job executes on the worker node.
     * 
     * @param job
     * 
     * @return  the full path to the directory where the job executes
     */
    public String getWorkerNodeDirectory( Job job ){
        StringBuffer workerNodeDir = new StringBuffer();
        String destDir = mSiteStore.getEnvironmentVariable( job.getSiteHandle() , "wntmp" );
        destDir = ( destDir == null ) ? "/tmp" : destDir;

        String relativeDir = mPOptions.getRelativeDirectory();
        
        workerNodeDir.append( destDir ).append( File.separator ).
                      append( relativeDir.replaceAll( "/" , "-" ) ).
                      //append( File.separator ).append( job.getCompleteTCName().replaceAll( ":[:]*", "-") );
                      append( "-" ).append( job.getID() );


        return workerNodeDir.toString();
    }

    /**
     * Generates a seqexec input file for an Aggregated job. The function first enables the
     * job via kickstart module for worker node execution and then retrieves
     * the commands to put in the input file from the environment variables specified
     * for kickstart.
     *
     * It creates a single input file for the seqexec invocation.
     * The input file contains commands to
     *
     * <pre>
     * 1) create directory on worker node
     * 2) fetch input data files
     * 3) execute the job
     * 4) transfer the output data files
     * 5) cleanup the directory
     * </pre>
     *
     * @param job          the job to be enabled.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     * 
     * @return the file handle to the seqexec input file
     */
    protected File enableAndGenerateSeqexecInputFile( AggregatedJob job, boolean isGlobusJob) {
        File stdIn = new File( mSubmitDir, job.getID() + ".in" );

        //reset the list of sls files that are transferred via condor file transfer
        //job.condorVariables.removeIPFilesForTransfer();
        
        String directory  = this.mKickstartGridStartImpl.getWorkerNodeDirectory( job );

        //remove the remote or initial dir's from the job
        String key = getDirectoryKey( job );
        String dir = (String)job.condorVariables.removeKey( key );

        int taskid = 1;
        try{
            //create a temp file first
            File temp = File.createTempFile( "merge_stdin", null, new File(mSubmitDir));
            OutputStream ostream = new FileOutputStream( temp , true );
            PrintWriter writer = new PrintWriter( new BufferedWriter(new OutputStreamWriter(ostream)) );

           
            //worker node directory is created by setting SEQEXEC_SETUP Env variable
            job.envVariables.construct( SeqExec.SEQEXEC_SETUP_ENV_VARIABLE,  "/bin/mkdir -p " + directory );

            //retrieve name of seqxec prejob file
            String preJob = (String)job.envVariables.removeKey( Kickstart.KICKSTART_PREJOB );


            boolean suppressXMLHeader = false;

            //we need seqexec to cp the sls files to the worker node directories
            List<String> filesToBeCopied = new LinkedList();

            if( !this.mStageSLSFile ){
                //we let condor transfer the file from the submit directory
                //to a directory on the headnode/worker node.
                
                if( mSLS.needsSLSInputTransfers( job ) ){
                      filesToBeCopied.add( mSLS.getSLSInputLFN( job ) );
 
                }
                if( mSLS.needsSLSOutputTransfers( job ) ){
                      filesToBeCopied.add( mSLS.getSLSOutputLFN( job ) );
  
                }
                
                /* no longer required, as the SLS file is added for transfer
                 * correctly now in TransferEngine. Karan March 10, 2011
                String base =  this.mSubmitDir + File.separator ;
                for( String lfn: filesToBeCopied ){
                    job.condorVariables.addIPFileForTransfer( base + lfn );
                }
                 */
            }

            //transfer the proxy and set x bit accordingly
            String remoteProxyPath = null;
            if( this.mLocalUserProxy != null ){
                remoteProxyPath = directory + File.separator + this.mLocalUserProxyBasename;
                job.condorVariables.addIPFileForTransfer( this.mLocalUserProxy );
                job.envVariables.construct( Proxy.X509_USER_PROXY_KEY,
                                            remoteProxyPath );
                filesToBeCopied.add( this.mLocalUserProxyBasename );
            }
            
            // s3cfg - always transfer if defined
            if( this.mLocalS3cfg != null ) {
            
                // first fix permissions
                writer.println( getSeqExecCommentStringForTask( "chmod", null, taskid++) );
                writer.println( enableCommandViaKickstart( job,
                                                           "/bin/chmod 600 " + mLocalS3cfgBasename,
                                                           "chmod",
                                                           job.getSiteHandle(),
                                                           Job.STAGE_IN_JOB,
                                                           suppressXMLHeader ) );
                suppressXMLHeader = true;
                
                String remoteS3cfgPath = directory + File.separator + this.mLocalS3cfgBasename;
                job.condorVariables.addIPFileForTransfer( this.mLocalS3cfg );
                job.envVariables.construct( S3cfg.S3CFG, remoteS3cfgPath );
                filesToBeCopied.add( this.mLocalS3cfgBasename );
            }

            if( !filesToBeCopied.isEmpty() ){
                String cmd = constructCopyFileCommand( filesToBeCopied, directory );
                writer.println( getSeqExecCommentStringForTask( "cp",
                                                                null,
                                                                taskid++) );
                writer.println( enableCommandViaKickstart( job,
                                                           cmd,
                                                           "cp",
                                                           job.getSiteHandle(),
                                                           Job.CREATE_DIR_JOB,
                                                           suppressXMLHeader  ) );
                suppressXMLHeader = true;
            }


            //set the xbit for the proxy
            if( remoteProxyPath != null ){

                writer.println( getSeqExecCommentStringForTask( "chmod",
                                                                null,
                                                                taskid++) );
                writer.println( enableCommandViaKickstart( job,
                                                           "/bin/chmod 600 " + remoteProxyPath,
                                                           "chmod",
                                                           job.getSiteHandle(),
                                                           Job.STAGE_IN_JOB,
                                                           suppressXMLHeader ) );
                suppressXMLHeader = true;
            }

            if( preJob == null ){
                mLogger.log( "PREJOB was not constructed for job " + job.getName(),
                             LogManager.DEBUG_MESSAGE_LEVEL );

            }
            else{
                //enable the pre command via kickstart
                writer.println( getSeqExecCommentStringForTask( PEGASUS_TRANSFER_COMPLETE_TRANSFORMATION_NAME,
                                                                null,
                                                                taskid++) );
                writer.println( enableCommandViaKickstart(  job,
                                                            preJob,
                                                            PEGASUS_TRANSFER_COMPLETE_TRANSFORMATION_NAME,
                                                            job.getSiteHandle(),
                                                            Job.STAGE_OUT_JOB,
                                                            suppressXMLHeader,
                                                            directory ) );
                
                suppressXMLHeader = true;
                
            }

            //write out the main command that needs to be invoked
            //append to the temp file the contents of the aggregated
            //jobs stdin
            File stdin = new File(mSubmitDir, job.getStdIn());
            BufferedReader in = new BufferedReader(new FileReader( stdin ));
            String line = null;
            
            //go through each line of the .in file for clustered job
            while ((line = in.readLine()) != null) {
                if( line.startsWith( SeqExec.MONITORD_COMMENT_MARKER ) ){
                    // comment line for monitord . update the taskid
                    // #@ 1 vahi::findrange ID000002 
                    String contents[] = line.split( " " );
                    writer.println( 
                            getSeqExecCommentStringForTask( contents[2], contents[3], taskid++ ) );
                    
                }
                else if ( line.startsWith( "#" ) ){
                    //skip do nothing
                }
                else{                
                    //figure out the executable and the arguments first.
                    int index = line.indexOf(' '); //the first space
                    String executable = line.substring(0, index);
                    String arguments = line.substring(index);
                    writer.println(executable + ( suppressXMLHeader ? " -H " : "" ) + arguments);
                    suppressXMLHeader = true;
                }
            }
            in.close();
            writer.flush();
            stdin.delete();
            
            //retrieve name of seqxec postjob file
            //there should be a way to determine if 
            //postjob is actually a clustered job itself
            String postJob = (String)job.envVariables.removeKey( Kickstart.KICKSTART_POSTJOB );
            if( postJob != null  ){
                //enable the post command via kickstart
                writer.println( getSeqExecCommentStringForTask( PEGASUS_TRANSFER_COMPLETE_TRANSFORMATION_NAME,
                                                                null,
                                                                taskid++) );
                writer.println( enableCommandViaKickstart( job,
                                                           postJob,
                                                           PEGASUS_TRANSFER_COMPLETE_TRANSFORMATION_NAME,
                                                           job.getSiteHandle(),
                                                           Job.STAGE_OUT_JOB,
                                                           suppressXMLHeader,
                                                           directory ) );
                suppressXMLHeader = true;
                
            }
            
            //write out the cleanup command
            
            //worker node directory is removed by setting SEQEXEC_CLEANUP Env variable
            job.envVariables.construct( SeqExec.SEQEXEC_CLEANUP_ENV_VARIABLE,  "/bin/rm -rf " + directory );

            writer.close();
            ostream.close();
            
            
            //rename tmp to stdin
            temp.renameTo(stdin);
        }
        catch( IOException ioe ){
            throw new RuntimeException( "[SEQEXEC GRIDSTART] Error while writing out seqexec input file " + stdIn , ioe );
        }

        //specifically remove any GRIDSTART variables that may exist
        job.envVariables.removeKey( Kickstart.KICKSTART_SETUP ) ;
        job.envVariables.removeKey( Kickstart.KICKSTART_CLEANUP ) ;
        
        return stdIn;
    }

    /**
     * Generates a seqexec input file for the job. The function first enables the
     * job via kickstart module for worker node execution and then retrieves
     * the commands to put in the input file from the environment variables specified
     * for kickstart.
     *
     * It creates a single input file for the seqexec invocation.
     * The input file contains commands to
     *
     * <pre>
     * 1) create directory on worker node
     * 2) fetch input data files
     * 3) execute the job
     * 4) transfer the output data files
     * 5) cleanup the directory
     * </pre>
     *
     * @param job          the job to be enabled.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     * 
     * @return the file handle to the seqexec input file
     */
    protected File enableAndGenerateSeqexecInputFile(Job job, boolean isGlobusJob) {
        File stdIn = new File( mSubmitDir, job.getID() + ".in" );


        //enable the job first using kickstart
        this.mKickstartGridStartImpl.enable(job, isGlobusJob );

        //figure out the directory in which kickstart is running the job in.
        String cleanupCmd = (String)job.envVariables.removeKey( Kickstart.KICKSTART_CLEANUP ) ;
        String cmdArray[] = cleanupCmd.split( " " );
        //the last token is the directory name
        String directory  = cmdArray[ cmdArray.length - 1 ];

        int taskid = 1;
        
        try{
            OutputStream ostream = new FileOutputStream( stdIn , true );
            PrintWriter writer = new PrintWriter( new BufferedWriter(new OutputStreamWriter(ostream)) );

            
            //worker node directory is created by setting SEQEXEC_SETUP Env variable
            job.envVariables.construct( SeqExec.SEQEXEC_SETUP_ENV_VARIABLE,  "/bin/mkdir -p " + directory );

            //retrieve name of seqxec prejob file
            String preJob = (String)job.envVariables.removeKey( Kickstart.KICKSTART_PREJOB );

            boolean suppressXMLHeader = false;

            List<String> filesToBeCopied = new LinkedList();

            if( !this.mStageSLSFile ){
                //we let condor transfer the file from the submit directory
                //to a directory on the headnode/worker node.
                //we need seqexec to cp the sls files to the worker node directories
                if( mSLS.needsSLSInputTransfers( job ) ){
                    filesToBeCopied.add( mSLS.getSLSInputLFN(job) );
                }
                if( mSLS.needsSLSOutputTransfers( job ) ){
                    filesToBeCopied.add( mSLS.getSLSOutputLFN(job) );
                }
                
            }

            //transfer the proxy and set x bit accordingly
            String remoteProxyPath = null;
            if( this.mLocalUserProxy != null ){
                remoteProxyPath = directory + File.separator + this.mLocalUserProxyBasename;
                job.condorVariables.addIPFileForTransfer( this.mLocalUserProxy );
                job.envVariables.construct( Proxy.X509_USER_PROXY_KEY,
                                            remoteProxyPath );
                filesToBeCopied.add( this.mLocalUserProxyBasename );
            }

            // s3cfg - always transfer if defined
            if( this.mLocalS3cfg != null ) {
            
                // first fix permissions
                writer.println( getSeqExecCommentStringForTask( "chmod", null, taskid++) );
                writer.println( enableCommandViaKickstart( job,
                                                           "/bin/chmod 600 " + mLocalS3cfgBasename,
                                                           "chmod",
                                                           job.getSiteHandle(),
                                                           Job.STAGE_IN_JOB,
                                                           suppressXMLHeader ) );
                suppressXMLHeader = true;
                
                String remoteS3cfgPath = directory + File.separator + this.mLocalS3cfgBasename;
                job.condorVariables.addIPFileForTransfer( this.mLocalS3cfg );
                job.envVariables.construct( S3cfg.S3CFG, remoteS3cfgPath );
                filesToBeCopied.add( this.mLocalS3cfgBasename );
            }
                       
            // add cp's
            if( !filesToBeCopied.isEmpty() ){

                String cmd = constructCopyFileCommand( filesToBeCopied, directory );
                writer.println( getSeqExecCommentStringForTask( "cp",
                                                                null,
                                                                taskid++) );
                writer.println( enableCommandViaKickstart( job,
                                                           cmd,
                                                           "cp",
                                                           job.getSiteHandle(),
                                                           Job.CREATE_DIR_JOB,
                                                           suppressXMLHeader  ) );
                suppressXMLHeader = true;
            }

            //set the xbit for the proxy
            if( remoteProxyPath != null ){

                writer.println( getSeqExecCommentStringForTask( "chmod",
                                                                null,
                                                                taskid++) );
                writer.println( enableCommandViaKickstart( job,
                                                           "/bin/chmod 600 " + remoteProxyPath,
                                                           "chmod",
                                                           job.getSiteHandle(),
                                                           Job.STAGE_IN_JOB,
                                                           suppressXMLHeader ) );
                suppressXMLHeader = true;
            }

            if( preJob == null ){
                mLogger.log( "PREJOB was not constructed for job " + job.getName(),
                             LogManager.DEBUG_MESSAGE_LEVEL );

            }
            else{
                //there should be a way to determine if
                //prejob is actually a clustered job itself
                //enable the pre command via kickstart
                writer.println( getSeqExecCommentStringForTask( PEGASUS_TRANSFER_COMPLETE_TRANSFORMATION_NAME,
                                                                null,
                                                                taskid++) );
                     writer.println( enableCommandViaKickstart( job,
                                                                preJob,
                                                                PEGASUS_TRANSFER_COMPLETE_TRANSFORMATION_NAME,
                                                                job.getSiteHandle(),
                                                                Job.STAGE_OUT_JOB,
                                                                suppressXMLHeader,
                                                                directory ) );
                     suppressXMLHeader = true;
            }

            //write out the main command that needs to be invoked
            //add the kickstart -H option

            // the arguments are no longer set as condor profiles
            // they are now set to the corresponding profiles in
            // the Condor Code Generator only.
 //            writer.println( job.condorVariables.get( "executable" ) + " -H " + job.condorVariables.get( "arguments" ) );
            writer.println( getSeqExecCommentStringForTask( job, taskid++) );
            writer.println( job.getRemoteExecutable() + " -H " + job.getArguments() );

            writer.flush();
            
            //retrieve name of seqxec postjob file
            //there should be a way to determine if 
            //postjob is actually a clustered job itself
            String postJob = (String)job.envVariables.removeKey( Kickstart.KICKSTART_POSTJOB );
            if( postJob != null  ){
                //there should be a way to determine if
                //postjob is actually a clustered job itself
                //enable the post command via kickstart
                writer.println( getSeqExecCommentStringForTask( PEGASUS_TRANSFER_COMPLETE_TRANSFORMATION_NAME,
                                                                null,
                                                                taskid++) );
                writer.println( enableCommandViaKickstart( job,
                                                           postJob,
                                                           PEGASUS_TRANSFER_COMPLETE_TRANSFORMATION_NAME,
                                                           job.getSiteHandle(),
                                                           Job.STAGE_OUT_JOB,
                                                           suppressXMLHeader,
                                                           directory ) );
                suppressXMLHeader = true;
            }
            
            //write out the cleanup command
            //worker node directory is removed by setting SEQEXEC_CLEANUP Env variable
            job.envVariables.construct( SeqExec.SEQEXEC_CLEANUP_ENV_VARIABLE,  cleanupCmd );

            writer.close();
            ostream.close();
        }
        catch( IOException ioe ){
            throw new RuntimeException( "[SEQEXEC GRIDSTART] Error while writing out seqexec input file " + stdIn , ioe );
        }

        return stdIn;
    }
    
    /**
     * Returns the comemnt string required by monitord for a constituent job
     * It generates a comment of the format 
     * 
     * #@ task_id transformation derivation. 
     * 
     * @param job   the job
     * @param id    the task id.
     * 
     * @return the comment string
     * 
     * @see #MONITORD_COMMENT_MARKER
     */
    protected String getSeqExecCommentStringForTask( Job job,
                                                     int id ){
        
        return this.getSeqExecCommentStringForTask( job.getCompleteTCName(),
                                                    job.getLogicalID(),
                                                    id );
    }
    
    /**
     * Returns the comemnt string required by monitord for a constituent job
     * It generates a comment of the format 
     * 
     * #@ task_id transformation derivation. 
     * 
     * @param namespace   the namespace associated with the job
     * @param name        the logical name of the job
     * @param version     the version
     * @param absJobID    the id of the job in the DAX.
     * @param id          the task id.
     * 
     * @return the comment string
     * 
     * @see #MONITORD_COMMENT_MARKER
     */
    protected String getSeqExecCommentStringForTask( String namespace, 
                                                     String name,
                                                     String version,
                                                     String absJobID,
                                                     int id ){
        
        return this.getSeqExecCommentStringForTask( Separator.combine(namespace, name, version),
                                                    absJobID,
                                                    id );
    }
    
    
    /**
     * Returns the comemnt string required by monitord for a constituent job
     * It generates a comment of the format 
     * 
     * #@ task_id transformation derivation. 
     * 
     * @param transformation  the complete transformation 
     * @param absJobID       the id of the job in the DAX.
     * @param id          the task id.
     * 
     * @return the comment string
     * 
     * @see #MONITORD_COMMENT_MARKER
     */
    protected String getSeqExecCommentStringForTask( String transformation,
                                                     String absJobID,
                                                     int id  ){
    
        
        StringBuffer sb = new StringBuffer();
        
        sb.append( MONITORD_COMMENT_MARKER ).append( " " ).
           append( id ).append( " " ).
           append( transformation ).append( " " ).
           append( absJobID  );
           
        return sb.toString();
    }
    

    /**
      * Enables a command via kickstart. This is used to enable commands that
      * are retrieved from the SLS files.
      *
      * @param mainJob   the main job associated with which additional command
      *                  is invoked.    
      * @param command   the command that needs to be invoked via kickstart.
      * @param name      a logical name to assign to the command
      * @param site      the site on which the command executes
      * @param type      the type of job that the command refers to.
      * @param supressXMLHeader   get kickstart to disable XML header creation
      *
      *
      * @return   the command enabled via kickstart
      */

     protected String enableCommandViaKickstart( Job mainJob,
                                                 String command,
                                                 String name,
                                                 String site,
                                                 int type,
                                                 boolean supressXMLHeader ){
    
         return this.enableCommandViaKickstart( mainJob, command, name, site, type, supressXMLHeader, null );
     }

     /**
      * Enables a command via kickstart. This is used to enable commands that
      * are retrieved from the SLS files.
      *
      * @param mainJob   the main job associated with which additional command
      *                  is invoked.
      * @param command   the command that needs to be invoked via kickstart.
      * @param name      a logical name to assign to the command
      * @param site      the site on which the command executes
      * @param type      the type of job that the command refers to.
      * @param supressXMLHeader   get kickstart to disable XML header creation
      * @param changeToDirectory  the directory to which kickstart should change
      *                           before launching the job. If null, then kickstart
      *                           does not change directory
      *
      * @return   the command enabled via kickstart
      */
     protected String enableCommandViaKickstart( Job mainJob,
                                                 String command,
                                                 String name,
                                                 String site,
                                                 int type,
                                                 boolean supressXMLHeader,
                                                 String changeToDirectory
                                                 ){
         //figure out the executable and the arguments first.
         int index = command.indexOf( ' ' ); //the first space
         String executable = command.substring( 0, index );
         String arguments = command.substring(index);

         //create a job corresponding to the command.
         Job job = new Job();
         job.setTransformation( null, name, null );
         job.setJobType( type );
         job.setSiteHandle( site );
         job.setRemoteExecutable( executable );
         job.setArguments( arguments );

         //we pull the pegasus profiles from the main job
         //to ensure that path to kickstart is picked up correctlyh
         //if overriden via profiles
         job.vdsNS = mainJob.vdsNS;
         
         this.mKickstartGridStartImpl.enable( job, true );

         StringBuffer result = new StringBuffer();
         result.append( job.getRemoteExecutable() ).append( " " );
         if ( supressXMLHeader ){
             result.append( " -H " );
         }
         if( changeToDirectory != null ){
             result.append(  " -w " ).append( changeToDirectory );
         }

         // the arguments are no longer set as condor profiles
         // they are now set to the corresponding profiles in
         // the Condor Code Generator only.
         result.append( job.getArguments() );
//         result.append( job.condorVariables.get( "arguments" ) );

         return result.toString();
     }


    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getSeqExecTransformationCatalogEntry(String siteHandle){
        List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.lookup( "pegasus",
                                                "seqexec",
                                                null,
                                                siteHandle,
                                                TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " +
                Separator.combine( "pegasus",
                                   "seqexec",
                                    null ) +
                " Cause:" + e, LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return ( tcentries == null ) ?
                 this.defaultTCEntry( "pegasus",
                                      "seqexec",
                                       null,
                                       siteHandle ): //try using a default one
                 (TransformationCatalogEntry) tcentries.get(0);



    }


    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog.
     *
     * @param namespace  the namespace of the transfer transformation
     * @param name       the logical name of the transfer transformation
     * @param version    the version of the transfer transformation
     *
     * @param site  the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    protected  TransformationCatalogEntry defaultTCEntry(
                                                          String namespace,
                                                          String name,
                                                          String version,
                                                          String site ){

        TransformationCatalogEntry defaultTCEntry = null;
        //check if PEGASUS_HOME is set
        String home = mSiteStore.getPegasusHome( site );
        //if PEGASUS_HOME is not set, use VDS_HOME
        home = ( home == null )? mSiteStore.getVDSHome( site ): home;

        mLogger.log( "Creating a default TC entry for " +
                     Separator.combine( namespace, name, version ) +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //if home is still null
        if ( home == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for " +
                         Separator.combine( namespace, name, version ) +
                         " as PEGASUS_HOME or VDS_HOME is not set in Site Catalog" ,
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


        defaultTCEntry = new TransformationCatalogEntry( namespace,
                                                         name,
                                                         version );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site );
        defaultTCEntry.setType( TCType.INSTALLED );
        defaultTCEntry.setSysInfo( this.mSiteStore.lookup( site ).getSysInfo() );

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
        mLogger.log( "Created entry with path " + defaultTCEntry.getPhysicalTransformation(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
        return defaultTCEntry;
    }

    /**
     * Constructs the copy command to copy files to a destination directory
     *
     * @param files        list of source files
     * @param destination  the destination directory
     *
     * @return
     */
    protected String constructCopyFileCommand( List<String> files, String destination) {
        StringBuffer cp = new StringBuffer();

        cp.append( "/bin/cp" );

        for( String source : files ){
          cp.append( " " ).append(  source  );
        }
        cp.append( " " ).append( destination );

        return cp.toString();
    }

    public void useFullPathToGridStarts(boolean fullPath) {
        throw new UnsupportedOperationException("Not supported yet.");
    }


}
