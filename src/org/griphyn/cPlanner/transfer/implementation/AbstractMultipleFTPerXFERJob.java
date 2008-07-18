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


package org.griphyn.cPlanner.transfer.implementation;

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.JobManager;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.transfer.MultipleFTPerXFERJob;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.util.Separator;


import java.io.File;
import java.io.FileWriter;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;


/**
 * An abstract implementation for implementations that can handle multiple
 * file transfers in a single file transfer job.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public abstract class AbstractMultipleFTPerXFERJob extends Abstract
                      implements MultipleFTPerXFERJob  {



    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param bag  the bag of Pegasus initialization objects
     */
    public AbstractMultipleFTPerXFERJob( PegasusBag bag ) {
        super( bag );
    }

    /**
     * Constructs a general transfer job that handles multiple transfers per
     * transfer job. There are appropriate callouts to generate the implementation
     * specific details.
     *
     * @param job         the SubInfo object for the job, in relation to which
     *                    the transfer node is being added. Either the transfer
     *                    node can be transferring this jobs input files to
     *                    the execution pool, or transferring this job's output
     *                    files to the output pool.
     * @param files       collection of <code>FileTransfer</code> objects
     *                    representing the data files and staged executables to be
     *                    transferred.
     * @param execFiles   subset collection of the files parameter, that identifies
     *                    the executable files that are being transferred.
     * @param txJobName   the name of transfer node.
     * @param jobClass    the job Class for the newly added job. Can be one of the
     *                    following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     *
     * @return  the created TransferJob.
     */
    public TransferJob createTransferJob(SubInfo job,
                                         Collection files,
                                         Collection execFiles,
                                         String txJobName,
                                         int jobClass) {
        TransferJob txJob = new TransferJob();
//        SiteInfo ePool;
//        JobManager jobmanager;
        SiteCatalogEntry ePool;
        GridGateway jobmanager;

        //site where the transfer is scheduled
        //to be run. For thirdparty site it makes
        //sense to schedule on the local host unless
        //explicitly designated to run TPT on remote site
        String tPool = mRefiner.isSiteThirdParty(job.getSiteHandle(),jobClass) ?
                                //check if third party have to be run on remote site
                                mRefiner.runTPTOnRemoteSite(job.getSiteHandle(),jobClass) ?
                                          job.getSiteHandle() : "local"
                                :job.getSiteHandle();

        //the non third party site for the transfer job is
        //always the job execution site for which the transfer
        //job is being created.
        txJob.setNonThirdPartySite(job.getSiteHandle());


        //we first check if there entry for transfer universe,
        //if no then go for globus
//        ePool = mSCHandle.getTXPoolEntry(tPool);
        ePool = mSiteStore.lookup( tPool );

        txJob.jobName = txJobName;
        txJob.executionPool = tPool;
        txJob.condorUniverse = "globus";

        TransformationCatalogEntry tcEntry = this.getTransformationCatalogEntry(tPool);
        if(tcEntry == null){
            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append( "Could not find entry in tc for lfn " ).append( getCompleteTCName() ).
                  append(" at site " ).append( txJob.getSiteHandle());
            mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( error.toString() );
        }


        txJob.namespace   = tcEntry.getLogicalNamespace();
        txJob.logicalName = tcEntry.getLogicalName();
        txJob.version     = tcEntry.getLogicalVersion();

        txJob.dvName      = this.getDerivationName();
        txJob.dvNamespace = this.getDerivationNamespace();
        txJob.dvVersion   = this.getDerivationVersion();

        //this should in fact only be set
        // for non third party pools
//        jobmanager = ePool.selectJobManager(this.TRANSFER_UNIVERSE,true);
        jobmanager = ePool.getGridGateway( GridGateway.JOB_TYPE.transfer );
        txJob.globusScheduler = (jobmanager == null) ?
                                  null :
//                                  jobmanager.getInfo(JobManager.URL);
                                  jobmanager.getContact();  

        txJob.jobClass = jobClass;
        txJob.jobID = job.jobName;

        txJob.stdErr = "";
        txJob.stdOut = "";

        txJob.executable = tcEntry.getPhysicalTransformation();

        //the i/p and o/p files remain empty
        //as we doing just copying urls
        txJob.inputFiles = new HashSet();

        //to get the file stat information we need to put
        //the files as output files of the transfer job
        txJob.outputFiles = new HashSet( files );

        try{
            txJob.stdIn = prepareSTDIN(txJobName, files);
        } catch (Exception e) {
            mLogger.log("Unable to write the stdIn file for job " +
                        txJob.getCompleteTCName() + " " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
        }

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
//        txJob.updateProfiles(mSCHandle.getPoolProfile(tPool));
        txJob.updateProfiles( ePool.getProfiles() );

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        txJob.updateProfiles(tcEntry);

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        txJob.updateProfiles(mProps);

        //take care of transfer of proxies
        this.checkAndTransferProxy(txJob);

        //apply the priority to the transfer job
        this.applyPriority(txJob);

        //constructing the arguments to transfer script
        //they only have to be incorporated after the
        //profile incorporation
        txJob.strargs = this.generateArgumentString(txJob);

        if(execFiles != null){
            //we need to add setup jobs to change the XBit
            super.addSetXBitJobs(job,txJob,execFiles);
        }

        //a callout that allows the derived transfer implementation
        //classes do their own specific stuff on the job
        this.postProcess( txJob );

        return txJob;
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

        //get the essential environment variables required to get
        //it to work correctly
        List envs = this.getEnvironmentVariables( site );
        if( envs == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for as could not construct necessary environment " +
                         Separator.combine( namespace, name, version ) ,
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
        defaultTCEntry.setProfiles( envs );

        //register back into the transformation catalog
        //so that we do not need to worry about creating it again
        try{
            mTCHandle.addTCEntry( defaultTCEntry , false );
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
     * Returns the environment profiles that are required for the default
     * entry to sensibly work.
     *
     * @param site the site where the job is going to run.
     *
     * @return List of environment variables, else null in case where the
     *         required environment variables could not be found.
     */
    protected abstract List getEnvironmentVariables( String site );


    /**
     * An optional method that allows the derived classes to do their own
     * post processing on the the transfer job before it is returned to
     * the calling module.
     *
     * @param job  the <code>TransferJob</code> that has been created.
     */
    public void postProcess( TransferJob job ){

    }

    /**
     * Prepares the stdin for the transfer job. Usually involves writing out a
     * text file that Condor transfers to the remote end.
     *
     * @param name  the name of the transfer job.
     * @param files    Collection of <code>FileTransfer</code> objects containing
     *                 the information about sourceam fin and destURL's.
     *
     * @return  the path to the prepared stdin file.
     *
     * @throws Exception in case of error.
     */
    protected String prepareSTDIN(String name, Collection files)throws Exception{
        //writing the stdin file
        FileWriter stdIn;
        String basename = name + ".in";
        stdIn = new FileWriter(new File(mPOptions.getSubmitDirectory(),
                                        basename));
        writeJumboStdIn(stdIn, files);
        //close the stdin stream
        stdIn.close();
        return basename;
    }


    /**
     * Returns the namespace of the derivation that this implementation
     * refers to.
     *
     * @return the namespace of the derivation.
     */
    protected abstract String getDerivationNamespace();


    /**
     * Returns the logical name of the derivation that this implementation
     * refers to.
     *
     * @return the name of the derivation.
     */
    protected abstract String getDerivationName();

    /**
     * Returns the version of the derivation that this implementation
     * refers to.
     *
     * @return the version of the derivation.
     */
    protected abstract String getDerivationVersion();

    /**
     * It constructs the arguments to the transfer executable that need to be passed
     * to the executable referred to in this transfer mode.
     *
     * @param job   the object containing the transfer node.
     * @return  the argument string
     */
    protected abstract String generateArgumentString(TransferJob job);

    /**
     * Writes to a FileWriter stream the stdin which goes into the magic script
     * via standard input
     *
     * @param stdIn    the writer to the stdin file.
     * @param files    Collection of <code>FileTransfer</code> objects containing
     *                 the information about sourceam fin and destURL's.
     *
     * @throws Exception
     */
    protected abstract void writeJumboStdIn(FileWriter stdIn, Collection files)
              throws Exception ;

    /**
     * Returns the complete name for the transformation that the implementation
     * is using..
     *
     * @return the complete name.
     */
    protected abstract String getCompleteTCName();


}
