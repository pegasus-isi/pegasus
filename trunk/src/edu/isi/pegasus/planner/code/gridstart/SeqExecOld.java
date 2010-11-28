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

import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.cluster.aggregator.JobAggregatorFactory;

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

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.List;

/**
 * This class launches all the jobs using seqexec. SeqExecOld by default wraps all
 * invocations in kickstart. This implementation is useful in the case of worker
 * node execution on Amazon EC2 with S3 being used to store the data.
 *
 * <p>
 * Since S3 transfer implementation uses seqexec to execute multiple s3 commands
 * in one job, there is possibility of optimization whereby the contents of all
 * sls files for a job can be coalesced into a single seqexec input file.
 * This seqexec input file will then be transferred to the node by condor when
 * running the job.  The benefit of this approach is that the SLS files that
 * are created during worker node execution dont need to be transferred from the
 * submit host.
 *
 * <p>
 * NOTE : For the worker node execution case, this implementation only works
 * if the SLS transfer implementation is set to S3. In addition the following property
 * should be set to false to disable the staging of the SLS files created by
 * S3 transfer implementation
 *
 * <pre>
 * pegasus.transfer.sls.s3.stage.sls.file     false
 * </pre>
 *
 * To enable this implementation at runtime set the following property
 * <pre>
 * pegasus.gridstart SeqExecOld
 * </pre>
 *
 * @see org.griphyn.cPlanner.transfer.sls.S3#STAGE_SLS_FILE_PROPERTY_KEY
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class SeqExecOld implements GridStart {
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
    protected String mExitParserArguments;

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
    private GridStart mKickstartGridStartImpl;

   
     /**
     * Handle to Transformation Catalog.
     */
    private TransformationCatalog mTCHandle;

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
//        mExitParserArguments = getExitCodeArguments();
        mTCHandle  = bag.getHandleToTransformationCatalog();

        mWorkerNodeExecution = mProps.executeOnWorkerNode();
        if( mWorkerNodeExecution ){
            //load SLS
            mSLS = SLSFactory.loadInstance( bag );
        }
        mEnablingPartOfAggregatedJob = false;
        mKickstartGridStartImpl = new Kickstart();
        mKickstartGridStartImpl.initialize( bag, dag );
   }

    /**
     * Enables a collection of jobs and puts them into an AggregatedJob.
     * The assumption here is that all the jobs are being enabled by the same
     * implementation. It enables the jobs and puts them into the AggregatedJob
     * that is passed to it.
     *
     * @param aggJob the AggregatedJob into which the collection has to be
     *               integrated.
     * @param jobs   the collection of jobs (Job) that need to be enabled.
     *
     * @return the AggregatedJob containing the enabled jobs.
     * @see #enable(Job,boolean)
     */
    public  AggregatedJob enable(AggregatedJob aggJob,Collection jobs){
        //sanity check for the arguments
        if( aggJob.strargs != null && aggJob.strargs.length() > 0){
            construct( aggJob, "arguments", aggJob.strargs);
        }

        this.mKickstartGridStartImpl.enable( aggJob, jobs );
        //we do not want the jobs being clustered to be enabled
        //for worker node execution just yet.
        /*mEnablingPartOfAggregatedJob = true;
        this.mKickstartGridStartImpl.setEnablePartOfAggregatedJob( true );

        for (Iterator it = jobs.iterator(); it.hasNext(); ) {
            Job job = (Job)it.next();
            
            //always pass isGlobus true as always
            //interested only in executable strargs
            this.enable(job, true);
            aggJob.add(job);
        }


        //set the flag back to false
        mEnablingPartOfAggregatedJob = false;
        this.mKickstartGridStartImpl.setEnablePartOfAggregatedJob( false );
        */

        return aggJob;
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

        //this approach only works for S3 for time being!
        //do a sanity check
        if (!(mSLS instanceof edu.isi.pegasus.planner.transfer.sls.S3)) {
            throw new RuntimeException("Second Level Staging with SeqExec for clustered jobs only works with S3");
        }

        //check if job is a clustered compute job
        if( job instanceof AggregatedJob && !mSLS.doesCondorModifications()){
            if( job.getJobType() == Job.COMPUTE_JOB ||
                job.getJobType() == Job.STAGED_COMPUTE_JOB ){

                AggregatedJob clusteredJob = (AggregatedJob) job;
                enableClusteredJobForWorkerNodeExecution( clusteredJob, isGlobusJob );
            }
        }
        // or if a job is non clustered compute job
        else if( job.getJobType() == Job.COMPUTE_JOB ||
                 job.getJobType() == Job.STAGED_COMPUTE_JOB ){

            File seqxecIPFile = enableAndGenerateSeqexecInputFile( job, isGlobusJob );
            construct( job,"input", seqxecIPFile.getAbsolutePath() );

            //construct path to seqexec file
            TransformationCatalogEntry entry = getSeqExecTransformationCatalogEntry( job.getSiteHandle() );
            construct( job, "executable", entry.getPhysicalTransformation() );
            //we want seqexec to fail hard on first error (non-zero exit code or signal death)
            construct( job, "arguments", " -f " );
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
        

        try {
            String key = getDirectoryKey( job );

            //always have the remote dir set to /tmp as
            //we are banking on kickstart to change directory
            //for us for compute jobs
            job.condorVariables.construct( key, "/tmp" );

            AggregatedJob clusteredJob = (AggregatedJob) job;
            Job firstJob = clusteredJob.getConstituentJob(0);

            GridStart gs = this.mKickstartGridStartImpl;

            //add the -f option always
            if( !mProps.abortOnFirstJobFailure() ){
                //the clustering module did not add the -f option
                //we add ourselves here
                //we want seqexec to fail hard on first error (non-zero exit code or signal death)
                construct( job, "arguments", job.getArguments() + " -f " );
            }
            
            //gs.enable( clusteredJob, isGlobusJob );
            //System.out.println( clusteredJob.envVariables );
            //enable the whole clustered job via kickstart
            Job j = (Job) clusteredJob.clone();
            gs.enable(j, isGlobusJob);


            //we merge the sls input and sls output files into
            //the stdin of the clustered job
            File slsInputFile = new File( mSubmitDir, mSLS.getSLSInputLFN(job));
            File slsOutputFile = new File( mSubmitDir, mSLS.getSLSOutputLFN(job));
            File stdin = new File( mSubmitDir, job.getStdIn());

            //create a temp file first
            File temp = File.createTempFile("sls", null, new File( mSubmitDir ));

            //add an entry to create the worker node directory
            PrintWriter writer = new PrintWriter( new FileWriter( temp ) );
            writer.println(   enableCommandViaKickstart(  "/bin/mkdir -p " + gs.getWorkerNodeDirectory( job ),
                                                          "mkdir",
                                                          job.getSiteHandle(),
                                                          Job.CREATE_DIR_JOB,
                                                          false )
                            );

            //append the sls input file to temp file
            BufferedReader in = new BufferedReader( new FileReader( slsInputFile ) );
            String line = null;
            while(( line = in.readLine() ) != null ){
                    writer.println( enableCommandViaKickstart(  line,
                                                                "s3cmd",
                                                                job.getSiteHandle(),
                                                                Job.STAGE_IN_JOB,
                                                                true) );
            }
            in.close();
            slsInputFile.delete();


            //append the stdin to the tmp file and add -H kickstart options
//            addToFile( stdin, tmpOStream );
            in = new BufferedReader( new FileReader( stdin ) );
            line = null;
            while(( line = in.readLine() ) != null ){
                //figure out the executable and the arguments first.
                int index = line.indexOf( ' ' ); //the first space
                String executable = line.substring( 0, index );
                String arguments = line.substring(index);
                writer.println ( executable + " -H " + arguments );
            }
            in.close();
            stdin.delete();


            //append the sls output to temp file
            in = new BufferedReader( new FileReader( slsOutputFile ) );
            line = null;
            while(( line = in.readLine() ) != null ){
                    writer.println( enableCommandViaKickstart(  line,
                                                                "s3cmd",
                                                                job.getSiteHandle(),
                                                                Job.STAGE_OUT_JOB,
                                                                true) );
            }
            in.close();
            slsOutputFile.delete();

            //we need to remove the directory
            writer.println( enableCommandViaKickstart( "/bin/rm -rf " + gs.getWorkerNodeDirectory( job ),
                                                       "rm",
                                                       job.getSiteHandle(),
                                                       Job.CREATE_DIR_JOB,
                                                       true ) );
            writer.close();

            //rename tmp to stdin
            temp.renameTo( stdin );

        } catch (IOException ex) {

        }

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
            if( job.getJobType() == Job.STAGED_COMPUTE_JOB ){
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
        return SeqExecOld.CLASSNAME;
    }


    /**
     * Returns a short textual description in the form of the name of the class.
     *
     * @return  short textual description.
     */
    public String shortDescribe(){
        return SeqExecOld.SHORT_NAME;
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
     * Returns a string containing the arguments with which the exitcode
     * needs to be invoked.
     *
     * @return the argument string.
     */
/*    private String getExitCodeArguments(){
        return mProps.getPOSTScriptArguments();
    }
*/
    
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
    private File enableAndGenerateSeqexecInputFile(Job job, boolean isGlobusJob) {
        File stdIn = new File( mSubmitDir, job.getID() + ".in" );

        //enable the job first using kickstart
        this.mKickstartGridStartImpl.enable(job, isGlobusJob );

        //figure out the directory in which kickstart is running the job in.
        String cleanupCmd = (String)job.envVariables.removeKey( Kickstart.KICKSTART_CLEANUP ) ;
        String cmdArray[] = cleanupCmd.split( " " );
        //the last token is the directory name
        String directory  = cmdArray[ cmdArray.length - 1 ];

        try{
            OutputStream ostream = new FileOutputStream( stdIn , true );
            PrintWriter writer = new PrintWriter( new BufferedWriter(new OutputStreamWriter(ostream)) );
            writer.println( enableCommandViaKickstart(  "/bin/mkdir -p " + directory,
                                                        "mkdir",
                                                        job.getSiteHandle(),
                                                        Job.CREATE_DIR_JOB,
                                                        false )
                           );
            writer.flush();

            //retrieve name of seqxec prejob file
            //there should be a way to determine if
            //prejob is actually a clustered job itself
            String preJob = (String)job.envVariables.removeKey( Kickstart.KICKSTART_PREJOB );
            if( preJob != null ){
                cmdArray = preJob.split( " " );
                //the last token is the sls file
                String name  = cmdArray[ cmdArray.length - 1 ];
                File slsFile = new File ( mSubmitDir, name );
                
                //add contents of sle file to stdin
                BufferedReader in = new BufferedReader( new FileReader( slsFile ) );
                String line = null;
                while(( line = in.readLine() ) != null ){
                    writer.println( enableCommandViaKickstart(  line,
                                                                "s3cmd",
                                                                job.getSiteHandle(),
                                                                Job.STAGE_IN_JOB,
                                                                true) );
                }
                in.close();

                //we delete the sls file now
                slsFile.delete();
            }

            //write out the main command that needs to be invoked
            //add the kickstart -H option
            writer.println( job.condorVariables.get( "executable" ) + " -H " + job.condorVariables.get( "arguments" ) );
            writer.flush();
            
            //retrieve name of seqxec postjob file
            //there should be a way to determine if 
            //postjob is actually a clustered job itself
            String postJob = (String)job.envVariables.removeKey( Kickstart.KICKSTART_POSTJOB );
            if( postJob != null ){
                cmdArray = postJob.split( " " );
                //the last token is the sls file
                String name  = cmdArray[ cmdArray.length - 1 ];
                File slsFile = new File ( mSubmitDir, name );

                //add contents to stdin
                //add contents of sle file to stdin
                BufferedReader in = new BufferedReader( new FileReader( slsFile ) );
                String line = null;
                while(( line = in.readLine() ) != null ){
                    writer.println( enableCommandViaKickstart(   line,
                                                                 "s3cmd",
                                                                 job.getSiteHandle(),
                                                                 Job.STAGE_OUT_JOB,
                                                                 true ) );
                }
                in.close();

                //delete the sls file
                slsFile.delete();
            }
            
            //write out the cleanup command
            writer.println( enableCommandViaKickstart(   cleanupCmd,
                                                         "rm",
                                                         job.getSiteHandle(),
                                                         Job.CREATE_DIR_JOB,
                                                         true ));
            writer.close();
            ostream.close();
        }
        catch( IOException ioe ){
            throw new RuntimeException( "[SEQEXEC GRIDSTART] Error while writing out seqexec input file " + stdIn , ioe );
        }

        return stdIn;
    }

     /**
      * Enables a command via kickstart. This is used to enable commands that
      * are retrieved from the SLS files.
      *
      * @param command   the command that needs to be invoked via kickstart.
      * @param name      a logical name to assign to the command
      * @param site      the site on which the command executes
      * @param type      the type of job that the command refers to.
      * @param supressXMLHeader   get kickstart to disable XML header creation
      *
      *
      * @return   the command enabled via kickstart
      */
     protected String enableCommandViaKickstart( String command,
                                                 String name,
                                                 String site,
                                                 int type,
                                                 boolean supressXMLHeader
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
         this.mKickstartGridStartImpl.enable( job, true );

         StringBuffer result = new StringBuffer();
         result.append( job.condorVariables.get( "executable" ) ).append( " " );
         if ( supressXMLHeader ){
             result.append( " -H " );
         }
         result.append( job.condorVariables.get( "arguments" ) );

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

}
