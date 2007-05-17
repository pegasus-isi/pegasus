/*
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

package org.griphyn.cPlanner.transfer.implementation;

import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.JobManager;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.Utility;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.code.gridstart.GridStartFactory;

import org.griphyn.cPlanner.namespace.Condor;
import org.griphyn.cPlanner.namespace.VDS;
import org.griphyn.cPlanner.namespace.ENV;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;
import org.griphyn.cPlanner.poolinfo.PoolMode;

import org.griphyn.cPlanner.transfer.Implementation;
import org.griphyn.cPlanner.transfer.Refiner;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.catalog.transformation.TCMode;

import java.io.File;

import java.util.Collection;
import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;

/**
 * An abstract implementation that implements some of the common functions in
 * the Implementation Interface that are required by all the implementations.
 *
 * @author Karan Vahi
 * @version $Revision: 1.4 $
 */
public abstract class Abstract implements Implementation{

    /**
     * The logical name of the transformation that creates directories on the
     * remote execution pools.
     */
    public static final String CHANGE_XBIT_TRANSFORMATION = "dirmanager";

    /**
     * The transformation namespace for the setXBit jobs.
     */
    public static final String XBIT_TRANSFORMATION_NS = "VDS";

    /**
     * The version number for the derivations for setXBit  jobs.
     */
    public static final String XBIT_TRANSFORMATION_VERSION = "1.0";

    /**
     * The derivation namespace for the setXBit  jobs.
     */
    public static final String XBIT_DERIVATION_NS = "VDS";

    /**
     * The version number for the derivations for setXBit  jobs.
     */
    public static final String XBIT_DERIVATION_VERSION = "1.7";

    /**
     * The prefix for the jobs which are added to set X bit for the staged
     * executables.
     */
    public static final String SET_XBIT_PREFIX = "chmod_";

    /**
     * The prefix for the NoOP jobs that are created.
     */
    public static final String NOOP_PREFIX = "noop_";


    /**
     * The path to the user proxy on the submit host (local pool), that is picked
     * up for use in transfer of proxies.
     */
    protected String mLocalUserProxy;

    /**
     * The basename of the user proxy , that is picked up for use in transfer of
     * proxies.
     */
    protected String mLocalUserProxyBasename;

    /**
     * The handle to the properties object holding the properties relevant to
     * Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * Contains the various options to the Planner as passed by the user at
     * runtime.
     */
    protected PlannerOptions mPOptions;

    /**
     * The handle to the Site Catalog. It is instantiated in this class.
     */
    protected PoolInfoProvider mSCHandle;

    /**
     * The handle to the Transformation Catalog. It must be instantiated  in the
     * implementing class
     */
    protected TransformationCatalog mTCHandle;

    /**
     * The handle to the refiner that loaded this implementation.
     */
    protected Refiner mRefiner;

    /**
     * The logging object which is used to log all the messages.
     *
     * @see org.griphyn.cPlanner.common.LogManager
     */
    protected LogManager mLogger;

    /**
     * The set of sites for which chmod job creation has to be disabled while
     * doing executable staging.
     */
    protected Set mDisabledChmodSites;

    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param properties  the properties object.
     * @param options     the options passed to the Planner.
     */
    public Abstract( PegasusProperties properties, PlannerOptions options ){
        mProps = properties;
        mPOptions = options;
        mLogger = LogManager.getInstance();

        //build up the set of disabled chmod sites
        mDisabledChmodSites = determineDisabledChmodSites( properties.getChmodDisabledSites() );

        //load the site catalog
        String poolFile = mProps.getPoolFile();
        String poolClass = PoolMode.getImplementingClass(mProps.getPoolMode());
        mSCHandle = PoolMode.loadPoolInstance(poolClass, poolFile,
                                              PoolMode.SINGLETON_LOAD);
        //load transformation catalog
        mTCHandle = TCMode.loadInstance();
        mLocalUserProxy = getPathToUserProxy();
        mLocalUserProxyBasename = (mLocalUserProxy == null) ?
                                  null :
                                  new File(mLocalUserProxy).getName();
    }


    /**
     * Applies priorities to the transfer jobs if a priority is specified
     * in the properties file.
     *
     * @param job   the transfer job .
     */
    public void applyPriority(TransferJob job){
        String priority = this.getPriority(job);
        if(priority != null){
            job.condorVariables.construct(Condor.PRIORITY_KEY,
                                          priority);
        }
    }


    /**
     * Determines if there is a need to transfer proxy for the transfer
     * job or not.  If there is a need to transfer proxy, then the job is
     * modified to create the correct condor commands to transfer the proxy.
     * Proxy is usually transferred if the VDS profile TRANSFER_PROXY is set,
     * or the job is being run in the condor vanilla universe. The proxy is
     * transferred from the submit host (i.e site local). The location is
     * determined from the value of the X509_USER_PROXY profile key associated
     * in the env namespace.
     *
     * @param job   the transfer job .
     *
     * @return boolean true job was modified to transfer the proxy, else
     *                 false when job is not modified.
     */
    public boolean checkAndTransferProxy(TransferJob job){
        boolean transfer = false;
        //not handling for third party transfers correctly.

        String style = job.vdsNS.containsKey(VDS.STYLE_KEY)?
                       (String)job.vdsNS.get(VDS.STYLE_KEY):
                       VDS.GLOBUS_STYLE;
        String universe = job.condorVariables.containsKey(Condor.UNIVERSE_KEY)?
                          (String)job.condorVariables.get(Condor.UNIVERSE_KEY):
                          //empty
                          "";
        boolean condition1 = job.vdsNS.getBooleanValue(VDS.TRANSFER_PROXY_KEY) ;
        boolean condition2 = ((style.equalsIgnoreCase(VDS.CONDOR_STYLE))||
                              (style.equalsIgnoreCase(VDS.GLIDEIN_STYLE))||
                               (job.executionPool.equalsIgnoreCase("local") &&
                                   (universe.equalsIgnoreCase(Condor.VANILLA_UNIVERSE)
                                    || universe.equalsIgnoreCase(Condor.STANDARD_UNIVERSE))
                                )
                              );


        //condition1 is explicit request for transfer of proxy
        //condition2 is determination of the glide in case
        if(condition1 || condition2){
            if(mLocalUserProxyBasename != null){
                //set the transfer of proxy from the submit host
                //to the remote execution pool, using internal
                //condor transfer mechanism

                //add condor key transfer_input_files
                //and other required condor keys
                /*
                job.condorVariables.checkKeyInNS(Condor.TRANSFER_IP_FILES_KEY,
                                                 mLocalUserProxy);
                job.condorVariables.construct("should_transfer_files","YES");
                job.condorVariables.construct("when_to_transfer_output","ON_EXIT");
                */
                job.condorVariables.addIPFileForTransfer(mLocalUserProxy);

                //set the environment variable to basefile name
                job.envVariables.checkKeyInNS(ENV.X509_USER_PROXY_KEY,
                                              mLocalUserProxyBasename);

                if(!condition2){
                    //means the transfer job is not being run in
                    //condor vanilla universe. This means, that in
                    //all probability the proxy is being transferred
                    //by gass_cache, and that does not preserve file
                    //permissions correctly
                    job.envVariables.checkKeyInNS(ENV.GRIDSTART_PREJOB,
                                                  "chmod 600 " +
                                                  mLocalUserProxyBasename);
                }
                if(!condition1){
                    //for glide in jobs also tag we are
                    //transferring proxy
                    job.vdsNS.checkKeyInNS(VDS.TRANSFER_PROXY_KEY,"true");
                }
                //we want the transfer job to be run in the
                //directory that Condor or GRAM decided to run
                job.condorVariables.removeKey("remote_initialdir");
                transfer = true;
            }
        }
        return transfer;
    }

    /**
     * Sets the callback to the refiner, that has loaded this implementation.
     *
     * @param refiner  the transfer refiner that loaded the implementation.
     */
    public void setRefiner(Refiner refiner){
        mRefiner = refiner;
    }


    /**
     * Adds the dirmanager to the workflow, that do a chmod on the files
     * being staged.
     *
     * @param computeJob the computeJob for which the files are being staged.
     * @param txJob      the transfer job that is staging the files.
     * @param execFiles  the executable files that are being staged.
     *
     * @return boolean indicating whether any XBitJobs were succesfully added or
     *         not.
     */
    protected boolean addSetXBitJobs(SubInfo computeJob,
                                     SubInfo txJob,
                                     Collection execFiles){

        return this.addSetXBitJobs( computeJob, txJob.getName(), execFiles, txJob.getJobType() );
    }

    /**
     * Adds the dirmanager job to the workflow, that do a chmod on the files
     * being staged.
     *
     * @param computeJob     the computeJob for which the files are
     *                       being staged.
     * @param txJobName      the name of the transfer job that is staging the files.
     * @param execFiles      the executable files that are being staged.
     * @param transferClass  the class of transfer job
     *
     * @return boolean indicating whether any XBitJobs were succesfully added or
     *         not.
     */
    public boolean addSetXBitJobs( SubInfo computeJob,
                                   String txJobName,
                                   Collection execFiles,
                                   int transferClass ){

        boolean added = false;
        String computeJobName = computeJob.getName();
        String site = computeJob.getSiteHandle();

        //sanity check
        if(execFiles == null || execFiles.isEmpty()){
            return added;
        }
        if(transferClass != SubInfo.STAGE_IN_JOB){
            //extra check. throw an exception
            throw new RuntimeException("Invalid Transfer Type (" +
                                       txJobName + "," + transferClass +
                                           ") for staging executable files ");
        }


        //figure out whether we need to create a chmod or noop
        boolean noop = this.disableChmodJobCreation( site );

        //add setXBit jobs into the workflow
        int counter = 0;
        for( Iterator it = execFiles.iterator(); it.hasNext(); counter++ ){
            FileTransfer execFile = (FileTransfer)it.next();

            String xBitJobName = this.getSetXBitJobName( computeJobName, counter );//create a chmod job


            SubInfo xBitJob =  noop  ?
                               this.createNoOPJob( xBitJobName ) : //create a NOOP job
                               this.createSetXBitJob( execFile, xBitJobName ); //create a chmod job

            if( xBitJob == null ){
                //error occured while creating the job
                throw new RuntimeException("Unable to create setXBitJob " +
                                           "corresponding to  compute job " +
                                           computeJobName + " and transfer" +
                                           " job " + txJobName);

            }
            else{
                added = true;
                mRefiner.addJob( xBitJob );
                //add the relation txJob->XBitJob->ComputeJob
                mRefiner.addRelation( txJobName, xBitJob.getName(),
                                      xBitJob.getSiteHandle(), true);
                mRefiner.addRelation( xBitJob.getName(), computeJobName );
            }
        }

        return added;
    }

    /**
     * Generates the name of the setXBitJob , that is unique for the given
     * workflow.
     *
     * @param name    the name of the compute job
     * @param counter the index for the setXBit job.
     *
     * @return the name of the setXBitJob .
     */
    public String getSetXBitJobName(String name, int counter){
        StringBuffer sb = new StringBuffer();
        sb.append(this.SET_XBIT_PREFIX).append(name).
            append("_").append(counter);

        return sb.toString();
    }

    /**
     * Generates the name of the noop job , that is unique for the given
     * workflow.
     *
     * @param name    the name of the compute job
     * @param counter the index for the noop job.
     *
     * @return the name of the setXBitJob .
     */
    public String getNOOPJobName( String name, int counter ){
        StringBuffer sb = new StringBuffer();
        sb.append( this.NOOP_PREFIX ).append( name ).
            append( "_" ).append( counter );

        return sb.toString();
    }



    /**
     * It creates a NoOP job that runs on the submit host.
     *
     * @param name the name to be assigned to the noop job
     *
     * @return  the noop job.
     */
    public SubInfo createNoOPJob( String name ) {

        SubInfo newJob = new SubInfo();
        List entries = null;
        String execPath =  null;

        //jobname has the dagname and index to indicate different
        //jobs for deferred planning
        newJob.setName( name );
        newJob.setTransformation( "vds", "noop", "1.0" );
        newJob.setDerivation( "vds", "noop", "1.0" );

        newJob.setUniverse( "vanilla" );
        //the noop job does not get run by condor
        //even if it does, giving it the maximum
        //possible chance
        newJob.executable = "/bin/true";

        //construct noop keys
        newJob.setSiteHandle( "local" );
        newJob.setJobType( SubInfo.CREATE_DIR_JOB );
        construct(newJob,"noop_job","true");
        construct(newJob,"noop_job_exit_code","0");

        //we do not want the job to be launched
        //by kickstart, as the job is not run actually
        newJob.vdsNS.checkKeyInNS( VDS.GRIDSTART_KEY,
                                   GridStartFactory.GRIDSTART_SHORT_NAMES[GridStartFactory.NO_GRIDSTART_INDEX] );

        return newJob;

    }


    /**
     * Creates a dirmanager job, that does a chmod on the file being staged.
     * The file being staged should be of type executable. Though no explicit
     * check is made for that. The staged file is the one whose X bit would be
     * set on execution of this job. The site at which job is executed, is
     * determined from the site associated with the destination URL.
     *
     * @param file  the <code>FileTransfer</code> containing the file that has
     *              to be X Bit Set.
     * @param name  the name that has to be assigned to the job.
     *
     * @return  the chmod job, else null if it is not able to be created
     *          for some reason.
     */
    protected SubInfo createSetXBitJob(FileTransfer file, String name){
        SubInfo xBitJob = new SubInfo();
        TransformationCatalogEntry entry   = null;
        JobManager jobManager = null;
        NameValue destURL  = (NameValue)file.getDestURL();
        String eSiteHandle = destURL.getKey();
        try {
            List entries= mTCHandle.getTCEntries(null,
                                                 this.CHANGE_XBIT_TRANSFORMATION, null,
                                                 eSiteHandle, TCType.INSTALLED);

            if(entries == null || entries.isEmpty()){
                //log a message and return null
                mLogger.log("Unable to map transformation " +
                            this.CHANGE_XBIT_TRANSFORMATION +
                            " on site " + eSiteHandle,
                            LogManager.ERROR_MESSAGE_LEVEL);
                return null;
            }
            else{
                entry = (TransformationCatalogEntry) entries.get(0);
            }

        } catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entries from TC " +
                        e.getMessage(), LogManager.ERROR_MESSAGE_LEVEL );
            return null;
        }

        SiteInfo eSite = mSCHandle.getPoolEntry(eSiteHandle, "transfer");
        jobManager     = eSite.selectJobManager("transfer",true);
        String arguments = " -X -f " + Utility.getAbsolutePath(destURL.getValue());

        xBitJob.jobName     = name;
        xBitJob.logicalName = this.CHANGE_XBIT_TRANSFORMATION;
        xBitJob.namespace   = this.XBIT_TRANSFORMATION_NS;
        xBitJob.version     = this.XBIT_TRANSFORMATION_VERSION;
        xBitJob.dvName      = this.CHANGE_XBIT_TRANSFORMATION;
        xBitJob.dvNamespace = this.XBIT_DERIVATION_NS;
        xBitJob.dvVersion   = this.XBIT_DERIVATION_VERSION;
        xBitJob.condorUniverse  = "vanilla";
        xBitJob.globusScheduler = jobManager.getInfo(JobManager.URL);
        xBitJob.executable      = entry.getPhysicalTransformation();
        xBitJob.executionPool   = eSiteHandle;
        xBitJob.strargs         = arguments;
        xBitJob.jobClass        = SubInfo.CREATE_DIR_JOB;
        xBitJob.jobID           = name;

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        xBitJob.updateProfiles(mSCHandle.getPoolProfile(eSiteHandle));

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        xBitJob.updateProfiles(entry);

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        xBitJob.updateProfiles(mProps);

        return xBitJob;
    }

    /**
     * Builds up a set of disabled chmod sites
     *
     * @param sites comma separated list of sites.
     *
     * @return a Set containing the site names.
     */
    protected Set determineDisabledChmodSites( String sites ){
        Set s = new HashSet();

        //sanity checks
        if( sites == null || sites.length() == 0 ) { return s;}

        for( StringTokenizer st = new StringTokenizer( sites ); st.hasMoreTokens() ; ){
            s.add( st.nextToken() );
        }

        return s;
    }

    /**
     * Returns a boolean indicating whether to disable chmod job creation for
     * a site or not.
     *
     * @param site  the name of the site
     *
     * @return boolean
     */
    protected boolean disableChmodJobCreation( String site ){
        return this.mDisabledChmodSites.contains( site );
    }

    /**
     * Returns the priority for the transfer job as specified in the properties
     * file.
     *
     * @param job  the Transfer job.
     *
     * @return the priority of the job as determined from properties, can be null
     *         if invalid value passed or property not set.
     */
    protected String getPriority(TransferJob job){
        String priority;
        int type     = job.jobClass;
        switch(type){
            case SubInfo.STAGE_IN_JOB:
                priority = mProps.getTransferStageInPriority();
                break;

            case SubInfo.STAGE_OUT_JOB:
                priority = mProps.getTransferStageOutPriority();
                break;

            case SubInfo.INTER_POOL_JOB:
                priority = mProps.getTransferInterPriority();
                break;

            default:
                priority = null;
        }
        return priority;
    }

    /**
     * Returns the path to the user proxy from the pool configuration file and
     * the properties file. The value in the properties file overrides the
     * value from the pool configuration file.
     *
     * @return path to user proxy on local pool.
     *         null if no path is found.
     */
    protected String getPathToUserProxy(){
        List l = mSCHandle.getPoolProfile("local",Profile.ENV);
        String proxy = null;

        if(l != null){
            //try to get the path to the proxy on local pool
            for(Iterator it = l.iterator();it.hasNext();){
                Profile p = (Profile)it.next();
                proxy = p.getProfileKey().equalsIgnoreCase(ENV.X509_USER_PROXY_KEY)?
                        p.getProfileValue():
                        proxy;
            }
        }

        //overload from the properties file
        ENV env = new ENV();
        env.checkKeyInNS(mProps,"local");
        proxy = env.containsKey(ENV.X509_USER_PROXY_KEY)?
                (String)env.get(ENV.X509_USER_PROXY_KEY):
                proxy;

        return proxy;
    }


    /**
     * Constructs a condor variable in the condor profile namespace
     * associated with the job. Overrides any preexisting key values.
     *
     * @param job   contains the job description.
     * @param key   the key of the profile.
     * @param value the associated value.
     */
    protected void construct(SubInfo job, String key, String value){
        job.condorVariables.checkKeyInNS(key,value);
    }



}
