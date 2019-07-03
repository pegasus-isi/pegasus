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

import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.code.GridStart;

import edu.isi.pegasus.planner.code.POSTScript;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.planner.transfer.SLS;


import edu.isi.pegasus.planner.namespace.Pegasus;

import java.io.File;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.io.IOException;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.namespace.Condor;
/**
 * This class ends up running the job directly on the grid, without wrapping
 * it in any other launcher executable.
 * It ends up connecting the jobs stdio and stderr to condor commands to
 * ensure they are sent back to the submit host.
 *
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class NoGridStart implements GridStart {
    private PegasusBag mBag;
    private ADag mDAG;

    /**
     * The basename of the class that is implmenting this. Could have
     * been determined by reflection.
     */
    public static final String CLASSNAME = "NoGridStart";

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "none";


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
    //protected boolean mWorkerNodeExecution;

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
     * Boolean indicating whether worker package staging is enabled or not.
     */
    protected boolean mWorkerPackageStagingEnabled;

    /**
     * Boolean indicating whether to use full path or not
     */
    private boolean mUseFullPathToGridStart;

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
        mWorkerPackageStagingEnabled = mProps.transferWorkerPackage();
//        mExitParserArguments = getExitCodeArguments();

/* JIRA PM-495
        mWorkerNodeExecution = mProps.executeOnWorkerNode();
        if( mWorkerNodeExecution ){
            //load SLS
            mSLS = SLSFactory.loadInstance( bag );
        }
 */
        mEnablingPartOfAggregatedJob = false;
        mUseFullPathToGridStart = true;
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

            //construct( aggJob, "arguments", aggJob.strargs);

           // the arguments are no longer set as condor profiles
           // they are now set to the corresponding profiles in
           // the Condor Code Generator only.
           aggJob.setArguments( aggJob.strargs );

        }

        //we do not want the jobs being clustered to be enabled
        //for worker node execution just yet.
        mEnablingPartOfAggregatedJob = true;

        for (Iterator it = jobs.iterator(); it.hasNext(); ) {
            Job job = (Job)it.next();
            
            //always pass isGlobus true as always
            //interested only in executable strargs
            this.enable(job, true);
            aggJob.add(job);
        }


        //set the flag back to false
        mEnablingPartOfAggregatedJob = false;


        return aggJob;
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


        //get hold of the JobAggregator determined for this clustered job
        //during clustering
        JobAggregator aggregator = job.getJobAggregator();
        if( aggregator == null ){
            throw new RuntimeException( "Clustered job not associated with a job aggregator " + job.getID() );
        }

        boolean first = true;
        for (Iterator it = job.constituentJobsIterator(); it.hasNext(); ) {
            Job constituentJob = (Job)it.next();

            //earlier was set in SeqExec JobAggregator in the enable function
            constituentJob.vdsNS.construct( Pegasus.GRIDSTART_KEY,
                                            this.getVDSKeyValue() );

            if(first){
                first = false;
            }
            else{
                //we need to pass -H to kickstart
                //to suppress the header creation
                constituentJob.vdsNS.construct(Pegasus.GRIDSTART_ARGUMENTS_KEY,"-H");
            }


            //always pass isGlobus true as always
            //interested only in executable strargs
            //due to the fact that seqexec does not allow for setting environment
            //per constitutent constituentJob, we cannot set the postscript removal option
            this.enable( constituentJob, isGlobusJob );

            
        }

        //all the constitutent jobs are enabled.
        //get the job aggregator to render the job
        //to it's executable form
        aggregator.makeAbstractAggregatedJobConcrete( job  );

        //set the flag back to false
        //mEnablingPartOfAggregatedJob = false;

        //the aggregated job itself needs to be enabled via NoGridStart
        this.enable( (Job)job, isGlobusJob);

        return true;

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

        
        // the arguments are no longer set as condor profiles
        // they are now set to the corresponding profiles in
        // the Condor Code Generator only.
        job.setRemoteExecutable( handleTransferOfExecutable( job ) );

        //JIRA PM-543
        //set the directory key with the job
        if( requiresToSetDirectory( job ) ){
             job.setDirectory( this.getDirectory( job ) );

        }
        
/*
        //the executable path and arguments are put
        //in the Condor namespace and not printed to the
        //file so that they can be overriden if desired
        //later through profiles and key transfer_executable
        construct(job,"executable", handleTransferOfExecutable( job ) );
        //sanity check for the arguments
        if(job.strargs != null && job.strargs.length() > 0){
            construct(job, "arguments", job.strargs);
        }
*/
        // handle stdin
        if (job.stdIn.length() > 0) {
            //PM-833 for planner added auxillary jobs pick the .in file from
            //right submit directory
            if (job.logicalName.equals(
                edu.isi.pegasus.planner.transfer.implementation.Transfer.TRANSFORMATION_NAME)
                || job.logicalName.equals(edu.isi.pegasus.planner.refiner.cleanup.Cleanup.TRANSFORMATION_NAME )
                || job.logicalName.equals( edu.isi.pegasus.planner.refiner.createdir.DefaultImplementation.TRANSFORMATION_NAME )
                || job.logicalName.equals(edu.isi.pegasus.planner.cluster.aggregator.SeqExec.
                                         COLLAPSE_LOGICAL_NAME)
                || job.logicalName.equals(edu.isi.pegasus.planner.cluster.aggregator.MPIExec.
                                         COLLAPSE_LOGICAL_NAME)
                                         ) {


                //condor needs to pick up the constituentJob stdin and
                //transfer it to the remote end
                construct( job, "input" , job.getFileFullPath( submitDir, ".in") );
            }
            else{
                construct(job,"input",submitDir + job.stdIn);
            }
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
            construct(job,"output", job.getFileFullPath( submitDir, ".out") );
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
            construct(job,"error", job.getFileFullPath( submitDir, ".err"));
            if (isGlobusJob) {
                construct(job,"transfer_error","true");
            }
        }



        if( mGenerateLOF ){
            //but generate lof files nevertheless


            //inefficient check here again. just a prototype
            //we need to generate -S option only for non transfer jobs
            //generate the list of filenames file for the input and output files.
            if (! (job instanceof TransferJob)) {
                generateListofFilenamesFile( job.getInputFiles(),
                                             job,
                                             ".in.lof");
            }

            //for cleanup jobs no generation of stats for output files
            if (job.getJobType() != Job.CLEANUP_JOB) {
                generateListofFilenamesFile(job.getOutputFiles(),
                                           job,
                                           ".out.lof");

            }
        }///end of mGenerateLOF

        return true;
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
//            if( job.getJobType() == Job.STAGED_COMPUTE_JOB ){
            if( job.userExecutablesStagedForJob() ){
                //the executable is being staged to the remote site.
                //all we need to do is unset transfer_executable
                cvar.construct( "transfer_executable", "false" );
            }
            else if ( mWorkerPackageStagingEnabled && 
                      ( job.getJobType() == Job.CREATE_DIR_JOB || job.getJobType() == Job.CLEANUP_JOB)  ){
                //we dont complain. 
                //JIRA PM-281
            }
            else{
                mLogger.log( "Transfer of Executables in NoGridStart only works for staged computes jobs " + job.getName(),
                             LogManager.ERROR_MESSAGE_LEVEL );
                
            }
        }
        else{
            //the executable paths are correct and
            //point to the executable on the remote pool
            //PM-1360 if this is being used in PegasusLite, then update the path
            //figure out job executable
            path = ( !this.mUseFullPathToGridStart && job.userExecutablesStagedForJob() )?
                                //the basename of the executable used for pegasus lite
                                //and staging of executables
                                "." + File.separator  + job.getStagedExecutableBaseName( ):
                                //use whatever is set in the executable field
                                job.executable;
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
        return this.CLASSNAME;
    }


    /**
     * Returns a short textual description in the form of the name of the class.
     *
     * @return  short textual description.
     */
    public String shortDescribe(){
        return this.SHORT_NAME;
    }

    /**
     * Returns the SHORT_NAME for the POSTScript implementation that is used
     * to be as default with this GridStart implementation.
     *
     * @param job
     * @return the id for the POSTScript.
     *
     * @see POSTScript#shortDescribe()
     */
    public String defaultPOSTScript(Job job){
        return this.defaultPOSTScript();
    }

    
    /**
     * Returns the SHORT_NAME for the POSTScript implementation that is used
     * to be as default with this GridStart implementation.
     *
     * @return  the identifier for the NoPOSTScript POSTScript implementation.
     *
     * @see POSTScript#shortDescribe()
     */
    public String defaultPOSTScript(){
        return NoPOSTScript.SHORT_NAME;
    }
    
    /**
     * Indicates whether the GridStart implementation can generate 
     * checksums of generated output files or not
     *
     * @return boolean indicating whether can generate checksums or not
     */
    public boolean canGenerateChecksumsOfOutputs(){
        return false;
    }

    /**
     * Returns a boolean indicating whether we need to set the directory for
     * the job or not.
     *
     * @param job the job for which to set directory.
     *
     * @return
     */
    protected boolean requiresToSetDirectory( Job job ) {
        //the cleanup jobs should never have directory set as full path
        //is specified
        return ( job.getJobType() != Job.CLEANUP_JOB );
    }
    
    /**
     * Returns the directory in which the job should run.
     *
     * @param job   the job in which the directory has to run.
     *
     * @return
     */
    protected String getDirectory( Job job ){
        String execSiteWorkDir = mSiteStore.getInternalWorkDirectory(job);
        String workdir = (String) job.globusRSL.removeKey("directory"); // returns old value
        workdir = (workdir == null)?execSiteWorkDir:workdir;

        return workdir;
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
     * @param job     the job
     * @param suffix  the suffix to be applied to files
     *
     * @return the full path to lof file created, else null if no file is written out.
     */
     protected String generateListofFilenamesFile( Set files, Job job, String suffix ){
         //sanity check
         if ( files == null || files.isEmpty() ){
             return null;
         }

         String result = null;
         //writing the stdin file
        try {
            File f = new File( job.getFileFullPath(mSubmitDir, suffix) );
            FileWriter input;
            input = new FileWriter( f );
            PegasusFile pf;
            for( Iterator it = files.iterator(); it.hasNext(); ){
                pf = ( PegasusFile ) it.next();
                String lfn= pf.getLFN();
                StringBuilder sb = new StringBuilder();
                //to make sure that kickstart generates lfn attribute in statcall
                //element
                sb.append( lfn ).append( "=" ).
                   append( lfn ).append(  "\n" );
                input.write( sb.toString() );
            }
            //close the stream
            input.close();
            result = f.getAbsolutePath();

        } catch ( IOException e) {
            mLogger.log("Unable to write the lof file for job " + job.getID() + " with suffix " + suffix , e ,
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

    public void useFullPathToGridStarts(boolean fullPath) {
        mUseFullPathToGridStart = fullPath;
    }

}
