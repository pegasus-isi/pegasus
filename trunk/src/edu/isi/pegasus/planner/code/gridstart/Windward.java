/*
 * 
 *   Copyright 2007-2008 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */

package edu.isi.pegasus.planner.code.gridstart;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Job;

import edu.isi.pegasus.planner.code.GridStart;

import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.Pegasus;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Collection;
import java.util.Properties;

/**
 * The launcher to be used for Windward jobs. The compute jobs in Windward are 
 * launched via a GU wrapper that expects the following arguments on the command
 * line
 * <pre>
 *      -m the name of the module being launched
 *      -k the kb being used.
 *      -w workflow id ( used for logging )
 *      -j job id      ( used for logging )
 *      -h host
 *      -p port
 *      -c CVS params file.
 * </pre>
 *
 * The windward launcher launches the job via kickstart, but adds the relevant 
 * arguments for the GU wrapped jobs first.
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class Windward implements GridStart{

    /**
     * The properties prefix to pick up the allegro graph connection 
     * parameters.
     */
    public static final String ALLEGRO_PROPERTIES_PREFIX = "pegasus.windward.allegro.";

    /**
     * The basename of the class that is implmenting this. Could have
     * been determined by reflection.
     */
    public static final String CLASSNAME = "Windward";

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "Windward Launcher";

    /**
     * The name of GU environment variable that turns on logging
     */
    public static final String GU_LOGGING_ENV_VARIABLE = "GU_SERVER_LOGGING";
    
    /**
     * The KickStart instance that is initialized.
     */
    private Kickstart mKickstartLauncher;
    
    /**
     * The handle to pegasus properties.
     */
    private PegasusProperties mProps;
    
    /***
     * The hostname for the AllegroGraph KB store.
     */
    private String mAllegroHost;
    
    /***
     * The port for the AllegroGraph KB store.
     */
    private String mAllegroPort;
    
    /***
     * The kb in the allegrograph server.
     */
    private String mAllegroKB;
    
    /**
     * The submit directory where the submit files are being generated for
     * the workflow.
     */
    private String mSubmitDir;
    
    /**
     * The workflow id to be passed to GU
     */
    private String mWorkflowID;
    
    /**
     * The handle to the logger.
     */
    private LogManager mLogger;

    /**
     * Http URL for log4j properties.
     */
    private String mHttpLog4jURL;
    
    
    /**
     * The handle to the SiteStore.
     */
    private SiteStore mSiteStore;
    
    /**
     * The default constructor.
     */
    public Windward() {
        
    }
    
    /**
     * Ends up initializing the internal Kickstart launcher instance
     * 
     * @param bag  the bag of Pegasus initialization objects
     * @param dag  the workflow being worked upon.
     */
    public void initialize( PegasusBag bag, ADag dag ) {
        //short circuiting the factory..
        mKickstartLauncher = new Kickstart();
        mKickstartLauncher.initialize( bag, dag);
        mSubmitDir   = bag.getPlannerOptions().getSubmitDirectory();
        mLogger      = bag.getLogger();
        mProps       = bag.getPegasusProperties();
        Properties p = mProps.matchingSubset( Windward.ALLEGRO_PROPERTIES_PREFIX, false );
        mAllegroHost = p.getProperty( "host" );
        mAllegroPort = p.getProperty( "port" );
        
        String base  = p.getProperty( "basekb" );
        File f = new File( base, bag.getPlannerOptions().getRelativeDirectory() );
        mAllegroKB = f.getAbsolutePath();
        mWorkflowID  = dag.getExecutableWorkflowID();
        //set it in the properties for postscript to pick up
        mProps.setProperty( NetloggerPostScript.WORKFLOW_ID_PROPERTY , mWorkflowID );
        
        mHttpLog4jURL = mProps.getHttpLog4jURL();
        if( mHttpLog4jURL == null || mHttpLog4jURL.length() == 0 ){
            mLogger.log( "No http log4j url specified for workflow " + mWorkflowID, 
                         LogManager.WARNING_MESSAGE_LEVEL );
        }
        
        mSiteStore = bag.getHandleToSiteStore();
    } 

    /**
     * Enables a job to run on the grid by launching it via the Kickstart executable.
     * For the compute jobs the additional arguments to the GU wrapper are constructed
     * and a config file is constructed for the arguments in the DAX.
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
    public boolean enable( Job job, boolean isGlobusJob ) {
        if( job.getJobType() == Job.COMPUTE_JOB ||
            job.getJobType() == Job.STAGED_COMPUTE_JOB ){
            String args = job.getArguments();
            //we need to construct extra arguments for the 
            //GU wrapper and the config file.
            String config = createConfigFileOnSubmitHost( job.getName(), args );
            
            //add the file for transfer via condor file transfer mechanism
            job.condorVariables.addIPFileForTransfer( config );
            
            //trigger the second level transfer in case of windows env
            //from the jobmanager to backend windows node.
            if( mSiteStore.lookup( job.getSiteHandle() ).getOS() == SysInfo.OS.WINDOWS ){
                mLogger.log( "Triggering SLS for job " + job.getID() ,
                             LogManager.DEBUG_MESSAGE_LEVEL );
                StringBuffer tip = new StringBuffer();
                tip.append( "( " ).append( "transfer_input_files" ).append( " " ).
                    append( new File(config).getName() ).append( " )" );
                //add the forward globus rsl key
                job.globusRSL.construct( "condorsubmit", tip.toString() );
                
                //we dont want any directories set for windows jobs.
                String style = (String)job.vdsNS.get(Pegasus.STYLE_KEY);
                String directory = (style.equalsIgnoreCase(Pegasus.GLOBUS_STYLE) ||
                                   style.equalsIgnoreCase(Pegasus.GLIDEIN_STYLE))?
                                   (String)job.condorVariables.removeKey("remote_initialdir"):
                                   (String)job.condorVariables.removeKey("initialdir");
            }
            //reset the arguments to contain only the gu wrapper components
            args = getGUWrapperArguments( job );
            //append the cvs file path
            args += " -c " + new File( config ).getName();            
            job.setArguments( args );
        }
         //for windows os for auxillary jobs also delete remote_initialdir and intialdir
        else if( mSiteStore.lookup( job.getSiteHandle() ).getOS() == SysInfo.OS.WINDOWS ){
            //we dont want any directories set for windows jobs.
            String style = (String)job.vdsNS.get(Pegasus.STYLE_KEY);
            String directory = (style.equalsIgnoreCase(Pegasus.GLOBUS_STYLE) ||
                                style.equalsIgnoreCase(Pegasus.GLIDEIN_STYLE))?
                                   (String)job.condorVariables.removeKey("remote_initialdir"):
                                   (String)job.condorVariables.removeKey("initialdir");
        }
        
        //add the GU logging envvariable
        job.envVariables.construct( Windward.GU_LOGGING_ENV_VARIABLE, "1" );
        
        
        //launch the jobs directly via kickstart
        return mKickstartLauncher.enable( job, isGlobusJob );
    }

    /**
     * Returns the GU wrapper components for the job.
     * 
     * @param job the compute job
     * 
     * @return  the wrapper arguments for the GU wrapper
     */
    protected String getGUWrapperArguments( Job job ){
        StringBuffer args = new StringBuffer();
        
        //add the log4j properties url if it does not exist
        if( this.mHttpLog4jURL != null ){
            args.append( " -Dlog4j.properties=" ).append( mHttpLog4jURL );
        }
        
        args.append( " -m " ).append( job.getTXName() );
        args.append( " -w " ).append( mWorkflowID );
        args.append( " -j " ).append( job.getID() );
        args.append( " -k " ).append( this.mAllegroKB );
        args.append( " -h " ).append( this.mAllegroHost );
        args.append( " -p " ).append( this.mAllegroPort );
        
        if( true ){//should be a debug flag
            args.append( " -d -z -r -t ");
        }
        
        return args.toString();
    }
    
    /**
     * Creates a config file on the submit host. The argument contents are piped
     * to a text file.
     * 
     * @param name  the base name for the file
     * @param args  the arguments
     * 
     * @return the path to the config file that is created
     */
    protected String createConfigFileOnSubmitHost( String name, String args ) {
        //writing the stdin file
        File config; 
        try {
            String basename = name + ".config";
            FileWriter cWriter;
            config = new File( mSubmitDir, basename );
            cWriter = new FileWriter( config );
            //pipe all the args to the file
            cWriter.write( args );
            cWriter.write("\n");
            
            //close the stream
            cWriter.close();
        } catch ( IOException e) {
            mLogger.log( "Unable to write the config file for job " +
                        name + " " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( "Unable to write the config file for job " +
                        name , e );
        }
        return config.getAbsolutePath();
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
        //we want the merged jobs to also have the kickstart postscript i.e. exitpost
        return Kickstart.CLASSNAME;
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
     * @return  the identifier for the Kickstart postscript
     *
     *
     */
    public String defaultPOSTScript(){
        return NetloggerPostScript.SHORT_NAME;
    }

    
    /**
     * Enables an aggregated job.  Always set the POSTSCRIPT to be used as
     * netlogger-exitcode for clustered jobs.
     * 
     * @param aggJob
     * @param jobs
     * @return the enabled clustered job
     */
    public AggregatedJob enable(AggregatedJob aggJob, Collection jobs) {
        //set the postscript always to default
        //overriding one in seqexec
        aggJob.dagmanVariables.construct( Dagman.POST_SCRIPT_KEY, this.defaultPOSTScript() );
        
        //add the GU logging envvariable
        aggJob.envVariables.construct( Windward.GU_LOGGING_ENV_VARIABLE, "1" );
        
        //let the job launch via kickstart directly for time being.
        return mKickstartLauncher.enable( aggJob, jobs );
    }

    /**
     * 
     * @return  boolean
     */
    public boolean canSetXBit() {
        return mKickstartLauncher.canSetXBit();
    }

     /**
     * Returns the directory in which the job executes on the worker node.
     * 
     * @param job
     * 
     * @return  the full path to the directory where the job executes
     */
    public String getWorkerNodeDirectory( Job job ){
        throw new RuntimeException( "Method not implemented getWorkerNodeDirectory(SubInfo)");
    }
    
}