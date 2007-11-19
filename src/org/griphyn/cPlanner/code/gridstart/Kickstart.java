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
package org.griphyn.cPlanner.code.gridstart;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.AggregatedJob;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.NameValue;

import org.griphyn.cPlanner.common.UserOptions;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;
import org.griphyn.cPlanner.poolinfo.PoolMode;

import org.griphyn.cPlanner.namespace.Condor;
import org.griphyn.cPlanner.namespace.ENV;
import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.cPlanner.code.GridStart;
import org.griphyn.cPlanner.code.POSTScript;

import org.griphyn.cPlanner.code.generator.condor.CondorQuoteParser;
import org.griphyn.cPlanner.code.generator.condor.CondorQuoteParserException;

import java.util.Collection;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This enables a job to be run on the grid, by launching it through kickstart.
 * The kickstart executable is a light-weight program which  connects  the
 * stdin,  stdout  and  stderr  filehandles for VDS jobs on the remote
 * site.
 * <p>
 * Sitting in between the remote scheduler and the executable, it is
 * possible  for  kickstart  to  gather additional information about the
 * executable run-time behavior, including the  exit  status  of  jobs.
 * <p>
 * Kickstart is an executable distributed with VDS that can generally be found
 * at $PEGASUS_HOME/bin/kickstart
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public class Kickstart implements GridStart {

    /**
     * The suffix for the kickstart input file, that is generated to use
     * invoke at the remote end.
     */
    public static final String KICKSTART_INPUT_SUFFIX = "arg";

    /**
     * The basename of the class that is implmenting this. Could have
     * been determined by reflection.
     */
    public static final String CLASSNAME = "Kickstart";

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "kickstart";


    /**
     * The environment variable used to the set Kickstart SETUP JOB.
     */
    public static final String KICKSTART_SETUP = "GRIDSTART_SETUP";


    /**
     * The environment variable used to the set Kickstart PREJOB.
     */
    public static final String KICKSTART_PREJOB = "GRIDSTART_PREJOB";


    /**
     * The environment variable used to the set Kickstart POSTJOB.
     */
    public static final String KICKSTART_POSTJOB = "GRIDSTART_POSTJOB";

    /**
     * The environment variable used to the set Kickstart CLEANUP JOB.
     */
    public static final String KICKSTART_CLEANUP = "GRIDSTART_CLEANUP";


    /**
     * The LogManager object which is used to log all the messages.
     */
    private LogManager mLogger;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    private PegasusProperties mProps;

    /**
     * The handle to the workflow that is being enabled.
     */
    private ADag mConcDAG;

    /**
     * Handle to the site catalog.
     */
    private PoolInfoProvider mSiteHandle;

    /**
     * The submit directory where the submit files are being generated for
     * the workflow.
     */
    private String mSubmitDir;

    /**
     * A boolean indicating whether to use invoke always or not.
     */
    private boolean mInvokeAlways;

    /**
     * A boolean indicating whether to stat files or not.
     */
    private boolean mDoStat;

    /**
     * A boolean indicating whether to generate lof files or not.
     */
    private boolean mGenerateLOF;

    /**
     * The invoke limit trigger.
     */
    private long mInvokeLength;


    /**
     * Initializes the GridStart implementation.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param submitDir  the submit directory where the submit file for the job
     *                   has to be generated.
     * @param dag        the concrete dag so far.
     */
    public void initialize( PegasusProperties properties, String submitDir, ADag dag ){
        mSubmitDir    = submitDir;
        mProps        = properties;
        mInvokeAlways = properties.useInvokeInGridStart();
        mInvokeLength = properties.getGridStartInvokeLength();
        mDoStat       = properties.doStatWithKickstart();
        mGenerateLOF  = properties.generateLOFFiles();
        mLogger       = LogManager.getInstance();
        mConcDAG      = dag;


        String poolmode = mProps.getPoolMode();
        String poolClass = PoolMode.getImplementingClass(poolmode);
        mSiteHandle = PoolMode.loadPoolInstance(poolClass,mProps.getPoolFile(),
                                                PoolMode.SINGLETON_LOAD);


    }


    /**
     * Enables a collection of jobs and puts them into an AggregatedJob.
     * The assumption here is that all the jobs are being enabled by the same
     * implementation. It enables the jobs and puts them into the AggregatedJob
     * that is passed to it.
     * However, to create a valid single XML file, it suppresses the header
     * creation for all but the first job.
     *
     * @param aggJob the AggregatedJob into which the collection has to be
     *               integrated.
     * @param jobs   the collection of jobs (SubInfo) that need to be enabled.
     *
     * @return the AggregatedJob containing the enabled jobs.
     * @see #enable(SubInfo,boolean)
     */
    public  AggregatedJob enable(AggregatedJob aggJob,Collection jobs){
        boolean first = true;

        for (Iterator it = jobs.iterator(); it.hasNext(); ) {
            SubInfo job = (SubInfo)it.next();
            if(first){
                first = false;
            }
            else{
                //we need to pass -H to kickstart
                //to suppress the header creation
                job.vdsNS.construct(VDS.GRIDSTART_ARGUMENTS_KEY,"-H");
            }
            //always pass isGlobus true as always
            //interested only in executable strargs
            //due to the fact that seqexec does not allow for setting environment
            //per constitutent job, we cannot set the postscript removal option
            this.enable( job, true, mDoStat, false );
            aggJob.add( job );
            //check if any files are being transferred via
            //Condor and add to Aggregated Job
            //add condor keys to transfer files
            //This is now taken care of in the merge profiles section
//            if(job.condorVariables.containsKey(Condor.TRANSFER_IP_FILES_KEY)){
//              aggJob.condorVariables.addIPFileForTransfer(
//                                          (String)job.condorVariables.get( Condor.TRANSFER_IP_FILES_KEY) );
//
//           }
        }
        return aggJob;
    }


    /**
     * Enables a job to run on the grid by launching it through kickstart.
     * Does the stdio, and stderr handling of the job to be run on the grid.
     * It modifies the job description, and also constructs all the valid
     * option to be passed to kickstart for launching the executable.
     *
     * @param job  the <code>SubInfo</code> object containing the job description
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
    public boolean enable( SubInfo job, boolean isGlobusJob ){
        return this.enable( job, isGlobusJob, mDoStat , true );
    }


    /**
     * Enables a job to run on the grid by launching it through kickstart.
     * Does the stdio, and stderr handling of the job to be run on the grid.
     * It modifies the job description, and also constructs all the valid
     * option to be passed to kickstart for launching the executable.
     *
     * @param job  the <code>SubInfo</code> object containing the job description
     *             of the job that has to be enabled on the grid.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     * @param stat  boolean indicating whether to generate the lof files
     *                     for kickstart stat option or not.
     * @param addPostScript boolean indicating whether to add a postscript or not.
     *
     * @return boolean true if enabling was successful,else false in case when
     *         the path to kickstart could not be determined on the site where
     *         the job is scheduled.
     */
    protected boolean enable( SubInfo job, boolean isGlobusJob, boolean stat, boolean addPostScript ) {
        //check for worker node execution
        boolean mWorkerNodeExecution = true;

        //take care of relative submit directory if specified.
        String submitDir = mSubmitDir + mSeparator;
//        String submitDir = getSubmitDirectory( mSubmitDir , job) + mSeparator;

        //To get the gridstart/kickstart path on the remote
        //pool, querying with entry for vanilla universe.
        //In the new format the gridstart is associated with the
        //pool not pool, condor universe
        SiteInfo site = mSiteHandle.getPoolEntry(job.executionPool,
                                                 Condor.VANILLA_UNIVERSE);
        String gridStartPath = site.getKickstartPath();
        //sanity check
        if (gridStartPath == null){
            return false;
        }
        StringBuffer gridStartArgs = new StringBuffer();

        // the executable is gridstart, the application becomes its argument
        gridStartArgs.append("-n ");
        gridStartArgs.append(job.getCompleteTCName());
        gridStartArgs.append(' ');
        gridStartArgs.append("-N ");
        gridStartArgs.append(job.getCompleteDVName());
        gridStartArgs.append(' ');

        // handle stdin
        if (job.stdIn.length() > 0) {

            //for using the transfer script and other vds executables the
            //input file is transferred from the submit host by Condor to
            //stdin. We fool the kickstart to pick up the input file from
            //standard stdin by giving the input file name as -
            if (job.logicalName.equals(
                org.griphyn.cPlanner.transfer.implementation.Transfer.TRANSFORMATION_NAME)
                ||job.logicalName.equals(
                org.griphyn.cPlanner.transfer.implementation.T2.TRANSFORMATION_NAME)
                ||job.logicalName.equals(org.griphyn.cPlanner.cluster.aggregator.SeqExec.
                                         COLLAPSE_LOGICAL_NAME)
                ||job.logicalName.equals(org.griphyn.cPlanner.cluster.aggregator.MPIExec.
                                         COLLAPSE_LOGICAL_NAME)
                ||job.logicalName.equals(org.griphyn.cPlanner.engine.cleanup.Cleanup.TRANSFORMATION_NAME )
                                         ) {


                //condor needs to pick up the job stdin and
                //transfer it to the remote end
                construct( job, "input" , submitDir + job.getStdIn() );
                gridStartArgs.append("-i ").append("-").append(' ');

            } else {
                //kickstart provides the app's *tracked* stdin
                gridStartArgs.append("-i ").append(job.stdIn).append(' ');
            }
        }

        // handle stdout
        if (job.stdOut.length() > 0) {
            // gridstart saves the app's *tracked* stdout
            gridStartArgs.append("-o ").append(job.stdOut).append(' ');
        }

        // the Condor output variable and kickstart -o option
        // must not point to the same file for any local job.
        if (job.stdOut.equals(job.jobName + ".out") && !isGlobusJob) {
            mLogger.log("Detected WAW conflict for stdout",LogManager.WARNING_MESSAGE_LEVEL);
        }
        // the output of gridstart is propagated back to the submit host
        construct(job,"output",submitDir + job.jobName + ".out");


        if (isGlobusJob) {
            construct(job,"transfer_output","true");
        }

        // handle stderr
        if (job.stdErr.length() > 0) {
            // gridstart saves the app's *tracked* stderr
            gridStartArgs.append("-e ").append(job.stdErr).append(' ');
        }

        // the Condor error variable and kickstart -e option
        // must not point to the same file for any local job.
        if (job.stdErr.equals(job.jobName + ".err") && !isGlobusJob) {
            mLogger.log("Detected WAW conflict for stderr",LogManager.WARNING_MESSAGE_LEVEL);
        }
        // the error from gridstart is propagated back to the submit host
        construct(job,"error",submitDir + job.jobName + ".err");
        if (isGlobusJob) {
            construct(job,"transfer_error","true");
        }

        //we need to pass the resource handle
        //to kickstart as argument
        gridStartArgs.append("-R ").append(job.executionPool).append(' ');

        //handle the -w option that asks kickstart to change
        //directory before launching an executable.
        String style = (String)job.vdsNS.get(VDS.STYLE_KEY);
        if(job.vdsNS.getBooleanValue(VDS.CHANGE_DIR_KEY) && !mWorkerNodeExecution ){
	    style = (String)job.vdsNS.get(VDS.STYLE_KEY);

//            Commented to take account of submitting to condor pool
//            directly or glide in nodes. However, does not work for
//            standard universe jobs. Also made change in Kickstart
//            to pick up only remote_initialdir Karan Nov 15,2005
            String directory = (style.equalsIgnoreCase(VDS.GLOBUS_STYLE) ||
                                style.equalsIgnoreCase(VDS.GLIDEIN_STYLE))?
                     (String)job.condorVariables.removeKey("remote_initialdir"):
                     (String)job.condorVariables.removeKey("initialdir");


            //pass the directory as an argument to kickstart
            gridStartArgs.append("-w ").append(directory).append(' ');
        }

        if(job.vdsNS.getBooleanValue(VDS.TRANSFER_PROXY_KEY)){
            //just remove the remote_initialdir key
            //the job needs to be run in the directory
            //Condor or GRAM decides to run
            job.condorVariables.removeKey("remote_initialdir");

        }


        if ( mWorkerNodeExecution ){
            if( job.getJobType() == SubInfo.COMPUTE_JOB ){
                mLogger.log( "Going to generate a SLS file for " + job.getName() ,
                              LogManager.DEBUG_MESSAGE_LEVEL );

                //To Do handle staged compute jobs also.
                //and clustered jobs

                //remove the remote or initial dir's for the compute jobs
                String key = ( style.equalsIgnoreCase( VDS.GLOBUS_STYLE ) ||
                                     ( style.equalsIgnoreCase( VDS.GLIDEIN_STYLE ) || style.equalsIgnoreCase( VDS.CONDOR_STYLE ) )
                              )?
                               "remote_initialdir" :
                               "initialdir";

                String directory = (String)job.condorVariables.removeKey( key );

                String destDir = mSiteHandle.getEnvironmentVariable( job.getSiteHandle() , "wntmp" );
                destDir = ( destDir == null ) ? "/tmp" : destDir;

                String relativeDir = UserOptions.getInstance().getOptions().getRelativeSubmitDirectory();
                String workerNodeDir = destDir + File.separator + relativeDir.replaceAll( "/" , "-" );


                //pass the worker node directory as an argument to kickstart
                //because most jobmanagers cannot handle worker node tmp
                //as they check for existance on the head node
                gridStartArgs.append("-W ").append( workerNodeDir ).append(' ');

                //always have the remote dir set to /tmp as we are
                //banking upon kickstart to change the directory for us
                job.condorVariables.construct( key, destDir );

                //generate the sls file with the mappings in the submit directory
                File slsInputFile = generateSLSInputFile( job, mSubmitDir, directory, workerNodeDir );
                //to add the environment for GRIDSTART_PREJOB
                if( slsInputFile != null ){
                    //construct a setup job not reqd as kickstart creating the directory
                    //String setupJob = constructSetupJob( job, workerNodeDir );
                    //setupJob = quote( setupJob );
                    //job.envVariables.construct( this.KICKSTART_SETUP, setupJob );

                    String preJob = constructPREJob( job,
                                                     mSiteHandle.getURLPrefix(job.getSiteHandle()),
                                                     directory,
                                                     workerNodeDir,
                                                     slsInputFile.getName() );

                    preJob = quote( preJob );
                    job.envVariables.construct( this.KICKSTART_PREJOB, preJob );

                    //construct the postjob that transfers the output files
                    //back to head node directory
                    //to fix later. right now post job only created is pre job
                    //created
                    File slsOutputFile = generateSLSOutputFile( job, mSubmitDir, directory, workerNodeDir );
                    String postJob = constructPOSTJob( job,
                                                       mSiteHandle.getURLPrefix(job.getSiteHandle()),
                                                       directory,
                                                       workerNodeDir,
                                                       slsOutputFile.getName() );

                    postJob = quote( postJob );
                    job.envVariables.construct( this.KICKSTART_POSTJOB, postJob );


                    String cleanupJob = constructCleanupJob( job, workerNodeDir );
                    cleanupJob = quote( cleanupJob );
                    job.envVariables.construct( this.KICKSTART_CLEANUP, cleanupJob );
                }
            }
        }

        //check if the job type indicates staging of executable
//        The -X functionality is handled by the setup jobs that
//        are added as childern to the stage in jobs.
//        Karan November 22, 2005
//        if(job.getJobClassInt() == SubInfo.STAGED_COMPUTE_JOB){
//            //add the -X flag to denote turning on
//            gridStartArgs.append("-X ");
//       }

        //add the stat options to kickstart only for certain jobs for time being
        //and if the input variable is true
        if ( stat ){
            if (job.getJobType() == SubInfo.COMPUTE_JOB ||
                job.getJobType() == SubInfo.STAGED_COMPUTE_JOB ||
                job.getJobType() == SubInfo.CLEANUP_JOB ||
                job.getJobType() == SubInfo.STAGE_IN_JOB ||
                job.getJobType() == SubInfo.INTER_POOL_JOB) {

                String lof;
                List files = new ArrayList(2);

                //inefficient check here again. just a prototype
                //we need to generate -S option only for non transfer jobs
                //generate the list of filenames file for the input and output files.
                if (! (job instanceof TransferJob)) {
                    lof = generateListofFilenamesFile(job.getInputFiles(),
                                                      job.getID() + ".in.lof");
                    if (lof != null) {
                        File file = new File(lof);
                        job.condorVariables.addIPFileForTransfer(lof);
                        //arguments just need basename . no path component
                        gridStartArgs.append("-S @").append(file.getName()).
                            append(" ");
                        files.add(file.getName());
                    }
                }

                //for cleanup jobs no generation of stats for output files
                if (job.getJobType() != SubInfo.CLEANUP_JOB) {
                    lof = generateListofFilenamesFile(job.getOutputFiles(),
                                                      job.getID() + ".out.lof");
                    if (lof != null) {
                        File file = new File(lof);
                        job.condorVariables.addIPFileForTransfer(lof);
                        //arguments just need basename . no path component
                        gridStartArgs.append("-s @").append(file.getName()).append(" ");
                        files.add(file.getName());
                    }
                }
                //add kickstart postscript that removes these files
                if( addPostScript ) {addCleanupPostScript(job, files); }
            }
        }//end of if ( stat )
        else if( mGenerateLOF ){
            //dostat is false. so no generation of stat option
            //but generate lof files nevertheless


            //inefficient check here again. just a prototype
            //we need to generate -S option only for non transfer jobs
            //generate the list of filenames file for the input and output files.
            if (! (job instanceof TransferJob)) {
                 generateListofFilenamesFile( job.getInputFiles(),
                                              job.getID() + ".in.lof");
            }

            //for cleanup jobs no generation of stats for output files
            if (job.getJobType() != SubInfo.CLEANUP_JOB) {
                generateListofFilenamesFile(job.getOutputFiles(),
                                            job.getID() + ".out.lof");

            }
        }///end of mGenerateLOF

        //append any arguments that need to be passed
        //kickstart directly, set elsewhere
        if(job.vdsNS.containsKey(VDS.GRIDSTART_ARGUMENTS_KEY)){
            gridStartArgs.append(job.vdsNS.get(VDS.GRIDSTART_ARGUMENTS_KEY))
                         .append(' ');
        }

        if(mProps.generateKickstartExtraOptions() && mConcDAG != null){
            gridStartArgs.append("-L ").append(mConcDAG.getLabel()).append(" ");
            gridStartArgs.append("-T ").append(mConcDAG.getMTime()).append(" ");
        }

        long argumentLength = gridStartArgs.length() +
                              job.executable.length() +
                              1 +
                              job.strargs.length();
        if(mInvokeAlways || argumentLength > mInvokeLength){
            if(!useInvoke(job,gridStartArgs)){
                mLogger.log("Unable to use invoke for job ",
                            LogManager.ERROR_MESSAGE_LEVEL);
                return false;
            }
        }
        else{
            gridStartArgs.append(job.executable).
                append(' ').append(job.strargs);
        }

        //the executable path and arguments are put
        //in the Condor namespace and not printed to the
        //file so that they can be overriden if desired
        //later through profiles and key transfer_executable
        construct(job,"executable", gridStartPath);
        construct(job,"arguments", gridStartArgs.toString());

        //all finished successfully
        return true;
    }


    /**
     * Indicates whether the enabling mechanism can set the X bit
     * on the executable on the remote grid site, in addition to launching
     * it on the remote grid site.
     *
     * @return true indicating Kickstart can set the X bit or not.
     */
    public  boolean canSetXBit(){
        return true;
    }

    /**
     * Returns the value of the vds profile with key as VDS.GRIDSTART_KEY,
     * that would result in the loading of this particular implementation.
     * It is usually the name of the implementing class without the
     * package name.
     *
     * @return the value of the profile key.
     * @see org.griphyn.cPlanner.namespace.VDS#GRIDSTART_KEY
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
     * @return  the identifier for the ExitPOST POSTScript implementation.
     *
     * @see POSTScript#shortDescribe()
     */
    public String defaultPOSTScript(){
        return ExitPOST.SHORT_NAME;
    }


    /**
     * Triggers the creation of the kickstart input file, that contains the
     * the remote executable and the arguments with which it has to be invoked.
     * The kickstart input file is created in the submit directory.
     *
     * @param job  the <code>SubInfo</code> object containing the job description.
     * @param args the arguments buffer for gridstart invocation so far.
     *
     * @return boolean indicating whether kickstart input file was generated or not.
     *                 false in case of any error.
     */
    private boolean useInvoke(SubInfo job,StringBuffer args){
        boolean result = true;

        String inputBaseName = job.jobName + "." + this.KICKSTART_INPUT_SUFFIX;

        //writing the stdin file
        try {
            FileWriter input;
            input = new FileWriter(new File(mSubmitDir,
                                            inputBaseName));
            //the first thing that goes in is the executable name
            input.write(job.executable);
            input.write("\n");
            //write out all the arguments
            //one on each line
            StringTokenizer st = new StringTokenizer(job.strargs);
            while(st.hasMoreTokens()){
                input.write(st.nextToken());
                input.write("\n");
            }
            //close the stream
            input.close();
        } catch (Exception e) {
            mLogger.log("Unable to write the kickstart input file for job " +
                        job.getCompleteTCName() + " " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }

        //construct list of files that need to be transferred
        //via Condor file transfer mechanism
        String fileList;
        if(job.condorVariables.containsKey(Condor.TRANSFER_IP_FILES_KEY)){
            //update the existing list.
            fileList = (String)job.condorVariables.get(Condor.TRANSFER_IP_FILES_KEY);
            if(fileList != null){
                fileList += "," + inputBaseName;
            }
        }
        else{
            fileList = inputBaseName;
        }

        construct(job,Condor.TRANSFER_IP_FILES_KEY,fileList);
        construct(job,"should_transfer_files","YES");
        construct(job,"when_to_transfer_output","ON_EXIT");

        //add the -I argument to kickstart
        args.append("-I ").append(inputBaseName).append(" ");
        return result;
    }


    /**
     * Generates a second level staging file for the input files. Responsible
     * for transferring from the head node to the worker node temp.
     *
     * @param job        the job for which the file is being created
     * @param submitDir  the submit directory where it has to be written out.
     * @param headNodeDirectory  the directory on the head node of the compute site.
     * @param workerNodeDirectory  the worker node directory
     *
     * @return the full path to lof file created, else null if no file is written out.
     */
    protected File generateSLSInputFile( SubInfo job,
                                         String submitDir,
                                         String headNodeDirectory,
                                         String workerNodeDirectory ){
        //sanity check
        Set files = job.getInputFiles();
        if ( files == null || files.isEmpty() ){
            mLogger.log( "Not Writing out a SLS file for job " + job.getName() ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }

        File sls = null;

        //figure out the remote site's headnode gridftp server
        //and the working directory on it.
        //the below should be cached somehow
        String sourceURLPrefix = mSiteHandle.getURLPrefix( job.getSiteHandle() );
        //String sourceDir = mSiteHandle.getExecPoolWorkDir( job );
        String sourceDir = headNodeDirectory;
        String destDir = workerNodeDirectory;


        //writing the stdin file
        try {
            StringBuffer name = new StringBuffer();
            name.append( "sls_" ).append( job.getName() ).append( ".in" );
            sls = new File( submitDir, name.toString() );
            FileWriter input = new FileWriter( sls );
            PegasusFile pf;

            //To do. distinguish the sls file from the other input files
            for( Iterator it = files.iterator(); it.hasNext(); ){
                pf = ( PegasusFile ) it.next();

                if( pf.getLFN().equals( ENV.X509_USER_PROXY_KEY ) ){
                    //ignore the proxy file for time being
                    //as we picking it from the head node directory
                    continue;
                }


                input.write( sourceURLPrefix ); input.write( File.separator );
                input.write( sourceDir ); input.write( File.separator );
                input.write( pf.getLFN() );
                input.write( "\n" );

                //destination
                input.write( "file://" );
                input.write( destDir ); input.write( File.separator );
                input.write( pf.getLFN() );
                input.write( "\n" );


            }
            //close the stream
            input.close();

        } catch ( IOException e) {
            mLogger.log( "Unable to write the sls file for job " + job.getName(), e ,
                         LogManager.ERROR_MESSAGE_LEVEL);
        }

        return sls;
    }


    /**
     * Generates a second level staging file for the output files. Transfers
     * the output files from the worker node back to head node.
     *
     * @param job        the job for which the file is being created
     * @param submitDir  the submit directory where it has to be written out.
     * @param headNodeDirectory  the directory on the head node of the compute site.
     * @param workerNodeDirectory  the worker node directory
     *
     * @return the full path to lof file created, else null if no file is written out.
     */
    protected File generateSLSOutputFile( SubInfo job,
                                         String submitDir,
                                         String headNodeDirectory,
                                         String workerNodeDirectory ){
        //sanity check
        Set files = job.getOutputFiles();
        if ( files == null || files.isEmpty() ){
            mLogger.log( "Not Writing out a SLS output file for job " + job.getName() ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }

        File sls = null;

        //figure out the remote site's headnode gridftp server
        //and the working directory on it.
        //the below should be cached somehow
        String destURLPrefix = mSiteHandle.getURLPrefix( job.getSiteHandle() );
        //String sourceDir = mSiteHandle.getExecPoolWorkDir( job );
        String destDir = headNodeDirectory;
        String sourceDir = workerNodeDirectory;


        //writing the stdin file
        try {
            StringBuffer name = new StringBuffer();
            name.append( "sls_" ).append( job.getName() ).append( ".out" );
            sls = new File( submitDir, name.toString() );
            FileWriter input = new FileWriter( sls );
            PegasusFile pf;

            //To do. distinguish the sls file from the other input files
            for( Iterator it = files.iterator(); it.hasNext(); ){
                pf = ( PegasusFile ) it.next();

                //source
                input.write( "file://" );
                input.write( sourceDir ); input.write( File.separator );
                input.write( pf.getLFN() );
                input.write( "\n" );

                //destination
                input.write( destURLPrefix ); input.write( File.separator );
                input.write( destDir ); input.write( File.separator );
                input.write( pf.getLFN() );
                input.write( "\n" );

            }
            //close the stream
            input.close();

        } catch ( IOException e) {
            mLogger.log( "Unable to write the sls output file for job " + job.getName(), e ,
                         LogManager.ERROR_MESSAGE_LEVEL);
        }

        return sls;
    }


    /**
     * Constructs a kickstart setup job
     *
     * @param job           the job to be run.
     * @param workerNodeTmp the worker node tmp to run the job in.
     *
     * @return String
     */
    protected String constructSetupJob( SubInfo job, String workerNodeTmp ){
       StringBuffer setup = new StringBuffer();

       setup.append( "/bin/mkdir -p " ).append( workerNodeTmp );


       return setup.toString();
    }

    /**
     * Constructs a kickstart setup job
     *
     * @param job           the job to be run.
     * @param workerNodeTmp the worker node tmp to run the job in.
     *
     * @return String
     */
    protected String constructCleanupJob( SubInfo job, String workerNodeTmp ){
       StringBuffer setup = new StringBuffer();

       setup.append( "/bin/rm -rf " ).append( workerNodeTmp );


       return setup.toString();
    }



    /**
     * Constructs the prejob  that fetches sls file, and then invokes transfer
     * again.
     *
     * @param job   the job for which the prejob is being created
     * @param headNodeURLPrefix String
     * @param headNodeDirectory String
     * @param workerNodeDirectory String
     * @param slsFile String
     *
     * @return String containing the prescript invocation
     */
    protected String constructPREJob( SubInfo job,
                                    String headNodeURLPrefix,
                                    String headNodeDirectory,
                                    String workerNodeDirectory,
                                    String slsFile ){

        StringBuffer preJob = new StringBuffer();

        //first figure out the path to transfer
        //hardcoded for now
        String transfer = "/nfs/home/vahi/PEGASUS/default/bin/transfer";


        //try and figure out the proxy
        String proxy = null;
        StringBuffer proxyPath = null;
        for( Iterator it = job.getInputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = ( PegasusFile ) it.next();
            if( pf instanceof FileTransfer && pf.getLFN().equals( ENV.X509_USER_PROXY_KEY ) ){
                //there is a proxy that needs to be set for the job
                //actually set it in prejob somehow.
                proxy =  ((NameValue)((FileTransfer)pf).getDestURL()).getValue();
                proxy = new File( proxy ).getName();
                proxyPath =  new StringBuffer();
                proxyPath.append( headNodeDirectory ).append( File.separator ).append( proxy );
                job.envVariables.construct( ENV.X509_USER_PROXY_KEY, proxyPath.toString()  );
                break;
            }
        }

        //add the command to chmod the proxy
        if( proxy != null ){
            preJob.append( "/bin/bash -c \"chmod 600 " ).append( proxyPath.toString() ).append( " && " );
        }


        //first we need to get the sls file to worker node
        /*
        preJob.append( "/bin/echo -e \" " ).
               append( headNodeURLPrefix ).append( File.separator ).
               append( headNodeDirectory ).append( File.separator ).
               append( slsFile ).append( " \\n " ).
               append( "file://" ).append( workerNodeDirectory ).append( File.separator ).
               append( slsFile ).append( "\"" ).
               append( " | " ).append( transfer ).append( " base mnt " );

        preJob.append( " && " );

        //now we need to get transfer to execute this sls file
        preJob.append( transfer ).append( " base mnt < " ).append( slsFile );
        */
        preJob.append( transfer ).append( " base mnt " ).append( headNodeDirectory ).
                                  append( File.separator ).append( slsFile );

        if( proxy != null ){
            //add the end quote
            preJob.append( "\"" );
        }


        return preJob.toString();
    }


    /**
     * Constructs the post job  that fetches sls file, and then invokes transfer
     * again.
     *
     * @param job   the job for which the prejob is being created
     * @param headNodeURLPrefix String
     * @param headNodeDirectory String
     * @param workerNodeDirectory String
     * @param slsFile String
     *
     * @return String containing the prescript invocation
     */
    protected String constructPOSTJob( SubInfo job,
                                       String headNodeURLPrefix,
                                       String headNodeDirectory,
                                       String workerNodeDirectory,
                                       String slsFile ){

       StringBuffer postJob = new StringBuffer();

       //first figure out the path to transfer
       //hardcoded for now
       String transfer = "/nfs/home/vahi/PEGASUS/default/bin/transfer";


       //no need to figure out proxy as already done in prejob?
       String proxy = null;
       StringBuffer proxyPath = null;
       for( Iterator it = job.getInputFiles().iterator(); it.hasNext(); ){
           PegasusFile pf = ( PegasusFile ) it.next();
           if( pf instanceof FileTransfer && pf.getLFN().equals( ENV.X509_USER_PROXY_KEY ) ){
               //there is a proxy that needs to be set for the job
               //actually set it in prejob somehow.
               proxy =  ((NameValue)((FileTransfer)pf).getDestURL()).getValue();
               proxy = new File( proxy ).getName();
               proxyPath =  new StringBuffer();
               proxyPath.append( headNodeDirectory ).append( File.separator ).append( proxy );
               job.envVariables.construct( ENV.X509_USER_PROXY_KEY, proxyPath.toString()  );
               break;
           }
       }

       //add the command to chmod the proxy
       if( proxy != null ){
           postJob.append( "/bin/bash -c \"chmod 600 " ).append( proxyPath.toString() ).append( " && " );
       }



       postJob.append( transfer ).append( " base mnt " ).append( headNodeDirectory ).
                                 append( File.separator ).append( slsFile );

       if( proxy != null ){
           //add the end quote
           postJob.append( "\"" );
       }


       return postJob.toString();
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
     * Constructs a condor variable in the condor profile namespace
     * associated with the job. Overrides any preexisting key values.
     *
     * @param job   contains the job description.
     * @param key   the key of the profile.
     * @param value the associated value.
     */
    private void construct(SubInfo job, String key, String value){
        job.condorVariables.construct(key,value);
    }


    /**
     * Condor Quotes a string
     *
     * @param string   the string to be quoted.
     *
     * @return quoted string.
     */
    private String quote( String string ){
        String result;
        try{
            mLogger.log("Unquoted Prejob is  " + string, LogManager.DEBUG_MESSAGE_LEVEL);
            result = CondorQuoteParser.quote( string, false );
            mLogger.log("Quoted Prejob is  " + result, LogManager.DEBUG_MESSAGE_LEVEL );
        }
        catch (CondorQuoteParserException e) {
            throw new RuntimeException("CondorQuoting Problem " +
                                       e.getMessage());
        }
        return result;

    }



    /**
     * Adds a /bin/rm post job to kickstart that removes the files passed.
     * The post jobs is added as an environment variable.
     *
     * @param job   the job in which the post job needs to be added.
     * @param files the files to be deleted.
     */
    private void addCleanupPostScript( SubInfo job, List files ){
        //sanity check
        if ( files == null || !mDoStat || files.isEmpty() ) { return; }

        //do not add if job already has a postscript specified
        if( job.envVariables.containsKey( this.KICKSTART_CLEANUP ) ){
            mLogger.log( "Not adding lof cleanup as another kickstart cleanup already exists",
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return;
        }

        StringBuffer ps = new StringBuffer();
        //maybe later we might want to pick it up from the TC
        ps.append( "/bin/rm -rf").append( " " );
        for( Iterator it = files.iterator(); it.hasNext(); ){
            ps.append( it.next() ).append( " " );
        }

        job.envVariables.construct( this.KICKSTART_CLEANUP, ps.toString() );

        return;
    }


}
