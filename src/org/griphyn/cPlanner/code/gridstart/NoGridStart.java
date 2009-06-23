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

package org.griphyn.cPlanner.code.gridstart;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.common.logging.LogManager;

import java.io.BufferedWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.code.GridStart;
import org.griphyn.cPlanner.code.gridstart.GridStartFactory;

import org.griphyn.cPlanner.code.POSTScript;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.AggregatedJob;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.transfer.sls.SLSFactory;
import org.griphyn.cPlanner.transfer.SLS;


import org.griphyn.cPlanner.namespace.VDS;
import org.griphyn.cPlanner.namespace.Dagman;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.io.IOException;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.namespace.ENV;
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
        mExitParserArguments = getExitCodeArguments();

        mWorkerNodeExecution = mProps.executeOnWorkerNode();
        if( mWorkerNodeExecution ){
            //load SLS
            mSLS = SLSFactory.loadInstance( bag );
        }
        mEnablingPartOfAggregatedJob = false;
    }

    /**
     * Enables a collection of jobs and puts them into an AggregatedJob.
     * The assumption here is that all the jobs are being enabled by the same
     * implementation. It enables the jobs and puts them into the AggregatedJob
     * that is passed to it.
     *
     * @param aggJob the AggregatedJob into which the collection has to be
     *               integrated.
     * @param jobs   the collection of jobs (SubInfo) that need to be enabled.
     *
     * @return the AggregatedJob containing the enabled jobs.
     * @see #enable(SubInfo,boolean)
     */
    public  AggregatedJob enable(AggregatedJob aggJob,Collection jobs){
        //sanity check for the arguments
        if( aggJob.strargs != null && aggJob.strargs.length() > 0){
            construct( aggJob, "arguments", aggJob.strargs);
        }

        //we do not want the jobs being clustered to be enabled
        //for worker node execution just yet.
        mEnablingPartOfAggregatedJob = true;

        for (Iterator it = jobs.iterator(); it.hasNext(); ) {
            SubInfo job = (SubInfo)it.next();
            
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
     * Enables a job to run on the grid by launching it directly. It ends
     * up running the executable directly without going through any intermediate
     * launcher executable. It connects the stdio, and stderr to underlying
     * condor mechanisms so that they are transported back to the submit host.
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
    public boolean enable(SubInfo job, boolean isGlobusJob) {
        //take care of relative submit directory if specified
        String submitDir = mSubmitDir + mSeparator;
//        String submitDir = getSubmitDirectory( mSubmitDir , job) + mSeparator;

        //the executable path and arguments are put
        //in the Condor namespace and not printed to the
        //file so that they can be overriden if desired
        //later through profiles and key transfer_executable
        construct(job,"executable", job.executable);

        //sanity check for the arguments
        if(job.strargs != null && job.strargs.length() > 0){
            construct(job, "arguments", job.strargs);
        }

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

        //handle stuff differently for clustered jobs
        //for non condor modified SLS
        if( mWorkerNodeExecution && !mSLS.doesCondorModifications() ){
            if( job instanceof AggregatedJob ){
                try {
                    //this approach only works for S3 for time being!
                    //do a sanity check
                    if (!(mSLS instanceof org.griphyn.cPlanner.transfer.sls.S3)) {
                        throw new RuntimeException("Second Level Staging with NoGridStart for clustered jobs only works with S3");
                    }
                    
                    String style = (String)job.vdsNS.get( VDS.STYLE_KEY );
                    //remove the remote or initial dir's for the compute jobs
                    String key = ( style.equalsIgnoreCase( VDS.GLOBUS_STYLE )  )?
                                   "remote_initialdir" :
                                   "initialdir";
                    
                    //always have the remote dir set to /tmp as
                    //we are banking on kickstart to change directory 
                    //for us for compute jobs
                    job.condorVariables.construct( key, "/tmp" );

                    AggregatedJob clusteredJob = (AggregatedJob) job;
                    SubInfo firstJob = clusteredJob.getConstituentJob(0);

                    GridStartFactory factory = new GridStartFactory();
                    factory.initialize(mBag, mDAG);
                    GridStart gs = factory.loadGridStart(firstJob, "/tmp");
                    
                    //gs.enable( clusteredJob, isGlobusJob );
                    //System.out.println( clusteredJob.envVariables );
                    //enable the whole clustered job via kickstart
                    SubInfo j = (SubInfo) clusteredJob.clone();
                    gs.enable(j, isGlobusJob);
                    
                    //enable all constitutents jobs through the factory
                    for (Iterator it = clusteredJob.constituentJobsIterator(); it.hasNext();) {
                        SubInfo cJob = (SubInfo) it.next();
                        gs.enable(cJob, isGlobusJob);
                    }

                    //we merge the sls input and sls output files into
                    //the stdin of the clustered job
                    File slsInputFile = new File( mSubmitDir, mSLS.getSLSInputLFN(job));
                    File slsOutputFile = new File( mSubmitDir, mSLS.getSLSOutputLFN(job));
                    File stdin = new File( mSubmitDir, job.getStdIn());

                    //create a temp file first
                    File temp = File.createTempFile("sls", null, new File( mSubmitDir ));
                    
                    //add an entry to create the worker node directory
                    PrintWriter writer = new PrintWriter( new FileWriter( temp ) );
                    writer.println( "/bin/mkdir " + gs.getWorkerNodeDirectory( job ) );
                    writer.close();
                    
                    OutputStream tmpOStream = new FileOutputStream( temp , true );
                    //append the sls input file to temp file
                    addToFile( slsInputFile, tmpOStream );
                    //append the stdin to the tmp file
                    addToFile( stdin, tmpOStream );
                    //append the sls output to temp file
                    addToFile( slsOutputFile, tmpOStream );
                    tmpOStream.close();
                    
                    //delete the stdin file and sls files
                    stdin.delete();
                    slsInputFile.delete();
                    slsOutputFile.delete();
                    
                    //rename tmp to stdin
                    temp.renameTo( stdin );
                    
                } catch (IOException ex) {
                    
                }
                 
                
            }
            else if( !mEnablingPartOfAggregatedJob ){
                if( job.getJobType() == SubInfo.COMPUTE_JOB ||
                    job.getJobType() == SubInfo.STAGED_COMPUTE_JOB ){

                    if( !mSLS.doesCondorModifications() ){
                        throw new RuntimeException( "Second Level Staging with NoGridStart only works with Condor SLS" );
                    }

                    String style = (String)job.vdsNS.get( VDS.STYLE_KEY );

                    //remove the remote or initial dir's for the compute jobs
                    String key = ( style.equalsIgnoreCase( VDS.GLOBUS_STYLE )  )?
                                   "remote_initialdir" :
                                   "initialdir";

                    String directory = (String)job.condorVariables.removeKey( key );

                    String destDir = mSiteStore.getEnvironmentVariable( job.getSiteHandle() , "wntmp" );
                    destDir = ( destDir == null ) ? "/tmp" : destDir;
                    
                    String relativeDir = mPOptions.getRelativeSubmitDirectory();
                    String workerNodeDir = destDir + File.separator + relativeDir.replaceAll( "/" , "-" );



                    //always have the remote dir set to /tmp as we are
                    //banking upon kickstart to change the directory for us
                    job.condorVariables.construct( key, "/tmp" );


                    //modify the job if required
                    if ( !mSLS.modifyJobForWorkerNodeExecution( job,
                                                            //mSiteHandle.getURLPrefix( job.getSiteHandle() ),
                                                                mSiteStore.lookup( job.getSiteHandle() ).getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix(),    
                                                                directory,
                                                                workerNodeDir ) ){
                        throw new RuntimeException( "Unable to modify job " + job.getName() + " for worker node execution" );
                    }


                }
            }//end of enabling worker node execution for non clustered jobs
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
            if (job.getJobType() != SubInfo.CLEANUP_JOB) {
                generateListofFilenamesFile(job.getOutputFiles(),
                                           job.getID() + ".out.lof");

            }
        }///end of mGenerateLOF

        return true;
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
     * @return  the identifier for the NoPOSTScript POSTScript implementation.
     *
     * @see POSTScript#shortDescribe()
     */
    public String defaultPOSTScript(){
        return NoPOSTScript.SHORT_NAME;
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
    private boolean removeDirectoryKey(SubInfo job){
        String style = job.vdsNS.containsKey(VDS.STYLE_KEY) ?
                       null :
                       (String)job.vdsNS.get(VDS.STYLE_KEY);

        //is being run. Remove remote_initialdir if there
        //condor style associated with the job
        //Karan Nov 15,2005
        return (style == null)?
                false:
                style.equalsIgnoreCase(VDS.CONDOR_STYLE);

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
     * Returns a string containing the arguments with which the exitcode
     * needs to be invoked.
     *
     * @return the argument string.
     */
    private String getExitCodeArguments(){
        return mProps.getPOSTScriptArguments();
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
    public String getWorkerNodeDirectory( SubInfo job ){
        StringBuffer workerNodeDir = new StringBuffer();
        String destDir = mSiteStore.getEnvironmentVariable( job.getSiteHandle() , "wntmp" );
        destDir = ( destDir == null ) ? "/tmp" : destDir;

        String relativeDir = mPOptions.getRelativeSubmitDirectory();
        
        workerNodeDir.append( destDir ).append( File.separator ).
                      append( relativeDir.replaceAll( "/" , "-" ) ).
                      //append( File.separator ).append( job.getCompleteTCName().replaceAll( ":[:]*", "-") );
                      append( File.separator ).append( job.getID() );


        return workerNodeDir.toString();
    }

}
