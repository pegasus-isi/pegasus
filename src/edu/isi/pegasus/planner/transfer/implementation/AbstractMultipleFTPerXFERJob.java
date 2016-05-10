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


package edu.isi.pegasus.planner.transfer.implementation;

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.transfer.MultipleFTPerXFERJob;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.classes.FileTransfer;


import java.io.File;
import java.io.FileWriter;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import edu.isi.pegasus.planner.classes.Profile;
import java.util.Iterator;


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
     * @param job         the Job object for the job, in relation to which
     *                    the transfer node is being added. Either the transfer
     *                    node can be transferring this jobs input files to
     *                    the execution pool, or transferring this job's output
     *                    files to the output pool.
     * @param site        the site where the transfer job should run.
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
     *                              stage-in worker transfer
     *
     * @return  the created TransferJob.
     */
    public TransferJob createTransferJob( Job job,
                                          String site,
                                          Collection files,
                                          Collection execFiles,
                                          String txJobName,
                                          int jobClass ) {

        TransferJob txJob = new TransferJob();
        SiteCatalogEntry ePool;
        GridGateway jobmanager;

        String tPool = site;

        String ntptSite = this.getNonThirdPartySite(job, files, jobClass );
        if( ntptSite == null && jobClass != Job.STAGE_IN_WORKER_PACKAGE_JOB ){
            throw new RuntimeException( "Unable to determine the non third party site for transfer job " + txJobName + 
                                        " of type " + jobClass );
        }
        mLogger.log( "Non Third Party Transfer site for transfer job " + txJobName + " is " + ntptSite, 
                      LogManager.DEBUG_MESSAGE_LEVEL );
        txJob.setNonThirdPartySite( ntptSite ); 
        
        //we first check if there entry for transfer universe,
        //if no then go for globus
        ePool = mSiteStore.lookup( tPool );

        txJob.jobName = txJobName;
        txJob.executionPool = tPool;
        txJob.setUniverse( GridGateway.JOB_TYPE.transfer.toString() );
        
        //PM-833 set the relative submit directory for the transfer
        //job based on the associated file factory
        txJob.setRelativeSubmitDirectory( this.mSubmitDirFactory.getRelativeDir( txJob ));

        TransformationCatalogEntry tcEntry = this.getTransformationCatalogEntry( tPool, jobClass );
        if(tcEntry == null){
            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append( "Could not find entry in TC for lfn " ).append( getCompleteTCName() ).
                  append(" at site " ).append( txJob.getSiteHandle());
            error.append( " . " ).append( "Either add an entry in the TC or make sure that PEGASUS_HOME is set as an env profile in the site catalog for site " ).
                  append( txJob.getSiteHandle() ).append( " . " );
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
/*       JIRA PM-277
        
        jobmanager = ePool.selectGridGateway( GridGateway.JOB_TYPE.transfer );
        txJob.globusScheduler = (jobmanager == null) ?
                                  null :
                                  jobmanager.getContact();  
*/
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
            //credentials are handled generically now when the stdin is
            //written out
            txJob.stdIn = prepareSTDINAndAssociateCredentials( txJob, files, job.getSiteHandle(), jobClass );
        } catch (Exception e) {
            StringBuffer error = new StringBuffer();
            error.append( "Unable to write the stdIn file for job " ).
                  append( txJob.getCompleteTCName() ).append( " " ).append( e.getMessage());
            error.append( "Files that were being written out " ).append(files);
            throw new RuntimeException( error.toString(), e );
        }

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        txJob.updateProfiles( ePool.getProfiles() );

        //add any notifications specified in the transformation
        //catalog for the job. JIRA PM-391
        txJob.addNotifications( tcEntry );

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        txJob.updateProfiles(tcEntry);

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        txJob.updateProfiles(mProps);

        //apply the priority to the transfer job
        this.applyPriority(txJob);

        //constructing the arguments to transfer script
        //they only have to be incorporated after the
        //profile incorporation
        txJob.strargs = this.generateArgumentString(txJob);

        //PM-810 worker node exeucution is per job level now
        boolean addNodesForSettingXBit = !mPegasusConfiguration.jobSetupForWorkerNodeExecution(job);
        if(execFiles != null && addNodesForSettingXBit ){
            //we need to add setup jobs to change the XBit
            super.addSetXBitJobs(job,txJob,execFiles);
        }

        //a callout that allows the derived transfer implementation
        //classes do their own specific stuff on the job
        this.postProcess( txJob );

        return txJob;
    }


    /**
     * Determines the correct site to be asscociated with the transfer job, to ensure
     * cleanup algorithms work correctly
     * 
     * @param job           the associated compute job for which transfer job is being constructed
     * @param files         the files to be transfered
     * @param jobClass      type of transfer job
     * 
     * @return 
     */
    protected String getNonThirdPartySite( Job job, Collection<FileTransfer> files , int jobClass ){
        String ntptSite = null;
        if( jobClass == Job.INTER_POOL_JOB ){
            //PM-936 the non third party site for inter site transfer should be
            //set to be the site handle of the parent job where the inputs are 
            //coming from, to ensure inplace cleanup job is a child to 
            //inter site transfer job
            
            for( Iterator it = files.iterator(); it.hasNext(); ){
                FileTransfer ft = (FileTransfer)it.next();
                ntptSite = ft.getSourceURL().getKey();
                break;
            }
        }
        else{
            //the non third party site for the transfer job is
            //always the job execution site for which the transfer
            //job is being created.
            ntptSite =  job.getStagingSiteHandle() ;
        }
        
        
        return ntptSite;
    }
    
    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog.
     *
     * @param namespace  the namespace of the transfer transformation
     * @param name       the logical name of the transfer transformation
     * @param version    the version of the transfer transformation
     * @param executableBasename  the basename of the executable
     * @param site  the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    protected  TransformationCatalogEntry defaultTCEntry(
                                                       String namespace,
                                                       String name,
                                                       String version,
                                                       String executableBasename,
                                                       String site ){

        TransformationCatalogEntry defaultTCEntry = null;
        //check if PEGASUS_HOME is set
        String home = mSiteStore.getPegasusHome( site );

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
            append( executableBasename );


        defaultTCEntry = new TransformationCatalogEntry( namespace,
                                                         name,
                                                         version );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site );
        defaultTCEntry.setType( TCType.INSTALLED );
        defaultTCEntry.addProfiles( envs );
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
     * Returns the environment profiles that are required for the default
     * entry to sensibly work.
     *
     * @param site the site where the job is going to run.
     *
     * @return List of environment variables, else null in case where the
     *         required environment variables could not be found.
     */
    protected abstract List<Profile> getEnvironmentVariables( String site );


    /**
     * An optional method that allows the derived classes to do their own
     * post processing on the the transfer job before it is returned to
     * the calling module.
     *
     * @param job  the <code>TransferJob</code> that has been created.
     */
    public void postProcess( TransferJob job ){
        //JIRA PM-538
        // change the type of stage worker job back to stage in job

        if( job.getJobType() == Job.STAGE_IN_WORKER_PACKAGE_JOB ){
            job.setJobType( Job.STAGE_IN_JOB );
        }
    }

    /**
     * Prepares the stdin for the transfer job. Usually involves writing out a
     * text file that Condor transfers to the remote end. Additionally, it associates
     * credentials with the job that are requried to for the transfers.
     *
     * @param name  the name of the transfer job.
     * @param files    Collection of <code>FileTransfer</code> objects containing
     *                 the information about sourceam fin and destURL's.
     * @param stagingSite the site where the data will be populated by first
     *                    level staging jobs.
     * @param jobClass    the job Class for the newly added job. Can be one of the
     *                    following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     *
     * @return  the path to the prepared stdin file.
     *
     * @throws Exception in case of error.
     */
    protected String prepareSTDINAndAssociateCredentials(TransferJob job,  Collection files, String stagingSite, int jobClass )throws Exception{
        //writing the stdin file
        FileWriter stdIn;
        String basename = job.getName() + ".in";
        
        //PM-833 the .in file is written in the same directory 
        //where the submit file for the job will be written out
        File dir = new File(mPOptions.getSubmitDirectory(), job.getRelativeSubmitDirectory() );
        
        stdIn = new FileWriter(new File( dir,
                                         basename));
        writeStdInAndAssociateCredentials(job, stdIn, files, stagingSite, jobClass );
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
     * @param job      the transfer job .
     * @param stdIn    the writer to the stdin file.
     * @param files    Collection of <code>FileTransfer</code> objects containing
     *                 the information about sourceam fin and destURL's.
     * @param stagingSite the site where the data will be populated by first
     *                    level staging jobs.
     * @param jobClass    the job Class for the newly added job. Can be one of the
     *                    following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     *
     * @throws Exception
     */
    protected abstract void writeStdInAndAssociateCredentials( TransferJob job, FileWriter stdIn, Collection files, String stagingSite, int jobClass )
              throws Exception ;

    /**
     * Returns the complete name for the transformation that the implementation
     * is using..
     *
     * @return the complete name.
     */
    protected abstract String getCompleteTCName();


}
