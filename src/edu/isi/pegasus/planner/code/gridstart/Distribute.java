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

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.Version;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;

import edu.isi.pegasus.planner.code.GridStart;

import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.namespace.Pegasus;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class wraps all the jobs, both remote or local to be executed using
 * the Distribute wrapper for HubZero.
 * 
 * This wrapper only works for the shared filesystem approach and only jobs 
 * scheduled for non local sites are wrapped by distribute. To use this, 
 * users needs to catalog the executable hubzero::distribute in their 
 * transformation catalog.
 *
 * 
 * @author Karan Vahi
 * @version $Revision$
 */

public class Distribute implements GridStart {
    private PegasusBag mBag;
    private ADag mDAG;

    /**
     * The basename of the class that is implmenting this. Could have
     * been determined by reflection.
     */
    public static final String CLASSNAME = "Distribute";

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "distribute";

    /**
     * The transformation namespace for the distribute
     */
    public static final String TRANSFORMATION_NAMESPACE = "hubzero";

    /**
     * The logical name of distribute
     */
    public static final String TRANSFORMATION_NAME = "distribute";

    /**
     * The version number for distribute.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The basename of the distribute executable.
     */
    public static final String EXECUTABLE_BASENAME = "distribute";
    
    /**
     * Stores the major version of the planner.
     */
    private String mMajorVersionLevel;

    /**
     * Stores the major version of the planner.
     */
    private String mMinorVersionLevel;

    /**
     * Stores the major version of the planner.
     */
    private String mPatchVersionLevel;
    
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
     * A boolean indicating whether to generate lof files or not.
     */
    protected boolean mGenerateLOF;

    /**
     * A boolean indicating whether to have worker node execution or not.
     */
    protected boolean mWorkerNodeExecution;
    
    /**
     * The options passed to the planner.
     */
    protected PlannerOptions mPOptions;

    /**
     * Handle to the site catalog store.
     */
    protected SiteStore mSiteStore;

    /**
     * An instance variable to track if enabling is happening as part of a clustered job.
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
     * Boolean indicating whether worker package transfer is enabled or not
     */
    protected boolean mTransferWorkerPackage;

    /** A map indexed by execution site and the corresponding worker package
     *location in the submit directory
     */
    Map<String,String> mWorkerPackageMap ;

    
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
        mSiteStore = bag.getHandleToSiteStore();
        mPOptions  = bag.getPlannerOptions();
        mSubmitDir = mPOptions.getSubmitDirectory();
        mProps     = bag.getPegasusProperties();
        mGenerateLOF  = mProps.generateLOFFiles();
        mTCHandle  = bag.getHandleToTransformationCatalog();

        mTransferWorkerPackage = mProps.transferWorkerPackage();

        if( mTransferWorkerPackage ){
            mWorkerPackageMap = bag.getWorkerPackageMap();
            if( mWorkerPackageMap == null ){
                mWorkerPackageMap = new HashMap<String,String>();
            }
        }
        else{
            mWorkerPackageMap = new HashMap<String,String>();
        }


        Version version = Version.instance();
        mMajorVersionLevel = version.getMajor();
        mMinorVersionLevel = version.getMinor();
        mPatchVersionLevel = version.getPatch();

        mWorkerNodeExecution = mProps.executeOnWorkerNode();
        if( mWorkerNodeExecution ){
            //sanity check
            throw new RuntimeException( "Distribute wrapper only works in the sharedfs mode. Please set " +
                                        PegasusConfiguration.PEGASUS_CONFIGURATION_PROPERTY_KEY  + " to " +
                                        PegasusConfiguration.SHARED_FS_CONFIGURATION_VALUE );
        }

        mEnablingPartOfAggregatedJob = false;
        mKickstartGridStartImpl = new Kickstart();
        mKickstartGridStartImpl.initialize( bag, dag );
        
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

        //in pegasus lite mode we dont want kickstart to change or create
        //worker node directories
        for( Iterator it = job.constituentJobsIterator(); it.hasNext() ; ){
            Job j = (Job) it.next();
            j.vdsNS.construct( Pegasus.CHANGE_DIR_KEY , "true" );
            j.vdsNS.construct( Pegasus.CREATE_AND_CHANGE_DIR_KEY, "false" );
        }

        //for time being we treat clustered jobs same as normal jobs
        //in pegasus-lite
        //return this.enable( (Job)job, isGlobusJob );

        //consider case for non worker node execution first
        if( !mWorkerNodeExecution ){
            //shared filesystem case.

            System.out.println( "Job " + job.getID() + " scheduled at site " + job.getSiteHandle() );
            if( job.getSiteHandle().equals( "local") ){
                //all jobs scheduled to local site just get 
                //vanilla treatment from the kickstart enabling.
                return mKickstartGridStartImpl.enable( job, isGlobusJob );
            }
            else{
                //the clustered jobs are never lauched via kickstart
                //as their constitutents are enabled
                mKickstartGridStartImpl.enable( job, isGlobusJob );
               
                //now we enable the jobs with the distribute wrapper
                wrapJobWithDistribute( job, isGlobusJob );
            }
                
        }
        else{
            throw new RuntimeException( "Distribute Job Wrapper only works for sharedfs deployments");
        }
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

        //consider case for non worker node execution first
        if( !mWorkerNodeExecution ){
            //shared filesystem case.

            System.out.println( "Job " + job.getID() + " scheduled at site " + job.getSiteHandle() );
            if( job.getSiteHandle().equals( "local") ){
                //all jobs scheduled to local site just get 
                //vanilla treatment from the kickstart enabling.
                return mKickstartGridStartImpl.enable( job, isGlobusJob );
            }
            else{
                //jobs scheduled to non local site are wrapped
                //with distribute after wrapping them with kickstart
                mKickstartGridStartImpl.enable( job, isGlobusJob );
               
                //now we enable the jobs with the distribute wrapper
                wrapJobWithDistribute( job, isGlobusJob );
            }
        }
        else{
            throw new RuntimeException( "Distribute Job Wrapper only works for sharedfs deployments");
        }
        return true;

    }
    
    /**
     * Wraps a job with the distribute wrapper.
     * The job existing executable and arguments are retrived to construct
     * an invocation string that is passed as an argument to the distribute
     * job launcher. Also, the job is modified to run on local site.
     * 
     * @param job          the job to be wrapped with distribute
     * @param globusJob    boolean
     */
    protected void wrapJobWithDistribute(Job job, boolean globusJob) {
        //retrieve the kickstart invocation
        StringBuilder ksInvocation = new StringBuilder();
        ksInvocation.append( job.getRemoteExecutable() ).append( " " ).
                     append( job.getArguments() );

        //construct the path to distribute executable
        //on local site.
        TransformationCatalogEntry entry = this.getTransformationCatalogEntry( "local" );

        String distributePath = ( entry == null )?
                     //rely on the path determined from profiles 
                     (String)job.vdsNS.get( Pegasus.GRIDSTART_PATH_KEY ):
                     //else the tc entry has highest priority
                     entry.getPhysicalTransformation();

        if( distributePath == null ){
            throw new RuntimeException( "Unable to determine path to the distribute wrapper on local site");
        }

        job.setRemoteExecutable( distributePath );
        job.setArguments( ksInvocation.toString() );

        //update the job to run on local site
        //and the style to condor
        job.setSiteHandle( "local" );
        job.condorVariables.construct(Pegasus.STYLE_KEY, Pegasus.CONDOR_STYLE );
        return;
    }



    /**
     * Returns the transformation catalog entry for kickstart on a site
     * 
     * @param site  the site on which the entry is required
     * 
     * @return the entry if found else null
     */
    protected TransformationCatalogEntry getTransformationCatalogEntry( String site ){
        List entries = null;
        try {
            entries = mTCHandle.lookup( Distribute.TRANSFORMATION_NAMESPACE,
                                        Distribute.TRANSFORMATION_NAME,
                                        Distribute.TRANSFORMATION_VERSION,
                                        site,
                                        TCType.INSTALLED );
        } catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entries from TC " +
                    e.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL);


        }
        
        return ( entries == null ) ?
                null  :
               (TransformationCatalogEntry) entries.get(0);
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
        return Distribute.CLASSNAME;
    }


    /**
     * Returns a short textual description in the form of the name of the class.
     *
     * @return  short textual description.
     */
    public String shortDescribe(){
        return Distribute.SHORT_NAME;
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

    public void useFullPathToGridStarts(boolean fullPath) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public String getWorkerNodeDirectory(Job job) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    
}
