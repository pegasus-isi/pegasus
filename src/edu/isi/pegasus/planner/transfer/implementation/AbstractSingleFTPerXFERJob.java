/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.transfer.implementation;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.transfer.SingleFTPerXFERJob;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

/**
 * An abstract implementation for implementations that can handle only a single file transfer in a
 * single file transfer job.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class AbstractSingleFTPerXFERJob extends Abstract implements SingleFTPerXFERJob {

    /**
     * The overloaded constructor, that is called by the Factory to load the class.
     *
     * @param bag the bag of Pegasus initialization objects
     */
    public AbstractSingleFTPerXFERJob(PegasusBag bag) {
        super(bag);
    }

    /**
     * Constructs a general transfer job that handles single transfers per transfer job. There are
     * appropriate callouts to generate the implementation specific details. It throws an error if
     * asked to create a transfer job for more than one transfer.
     *
     * @param job the Job object for the job, in relation to which the transfer node is being added.
     *     Either the transfer node can be transferring this jobs input files to the execution pool,
     *     or transferring this job's output files to the output pool.
     * @param site the site where the transfer job should run.
     * @param files collection of <code>FileTransfer</code> objects representing the data files and
     *     staged executables to be transferred.
     * @param execFiles subset collection of the files parameter, that identifies the executable
     *     files that are being transferred.
     * @param txJobName the name of transfer node.
     * @param jobClass the job Class for the newly added job. Can be one of the following: stage-in
     *     stage-out inter-pool transfer
     * @return the created TransferJob.
     */
    public TransferJob createTransferJob(
            Job job,
            String site,
            Collection files,
            Collection execFiles,
            String txJobName,
            int jobClass) {

        if (files.size() > 1) {
            // log an error
            // should throw an exception!
            StringBuffer error = new StringBuffer();

            error.append("Transfer Implementation ")
                    .append(this.getDescription())
                    .append(" supports single transfer per transfer job ");
            mLogger.log(error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException(error.toString());
        }

        Iterator it = files.iterator();
        FileTransfer ft = (FileTransfer) it.next();
        TransferJob txJob = this.createTransferJob(job, site, ft, execFiles, txJobName, jobClass);

        // to get the file stat information we need to put
        // the files as output files of the transfer job
        txJob.outputFiles = new HashSet(files);
        return txJob;
    }

    /**
     * Constructs a general transfer job that handles single transfers per transfer job. There are
     * appropriate callouts to generate the implementation specific details.
     *
     * @param job the Job object for the job, in relation to which the transfer node is being added.
     *     Either the transfer node can be transferring this jobs input files to the execution pool,
     *     or transferring this job's output files to the output pool.
     * @param site the site where the transfer job should run.
     * @param file collection of <code>FileTransfer</code> objects representing the data files and
     *     staged executables to be transferred.
     * @param execFiles subset collection of the files parameter, that identifies the executable
     *     files that are being transferred.
     * @param txJobName the name of transfer node.
     * @param jobClass the job Class for the newly added job. Can be one of the following: stage-in
     *     stage-out inter-pool transfer
     * @return the created TransferJob.
     */
    public TransferJob createTransferJob(
            Job job,
            String site,
            FileTransfer file,
            Collection execFiles,
            String txJobName,
            int jobClass) {

        TransferJob txJob = new TransferJob();
        SiteCatalogEntry ePool;
        GridGateway jobmanager;

        // site where the transfer is scheduled
        // to be run. For thirdparty site it makes
        // sense to schedule on the local host unless
        // explicitly designated to run TPT on remote site
        /*
        String tPool = mRefiner.isSiteThirdParty(job.getSiteHandle(),jobClass) ?
                                //check if third party have to be run on remote site
                                mRefiner.runTPTOnRemoteSite(job.getSiteHandle(),jobClass) ?
                                             job.getSiteHandle() : "local"
                                :job.getSiteHandle();
                                */
        String tPool = site;

        // the non third party site for the transfer job is
        // always the job execution site for which the transfer
        // job is being created.
        txJob.setNonThirdPartySite(job.getStagingSiteHandle());

        // we first check if there entry for transfer universe,
        // if no then go for globus
        ePool = mSiteStore.lookup(tPool);

        txJob.jobName = txJobName;
        txJob.executionPool = tPool;

        txJob.setUniverse(GridGateway.JOB_TYPE.transfer.toString());

        TransformationCatalogEntry tcEntry = this.getTransformationCatalogEntry(tPool, jobClass);
        if (tcEntry == null) {
            // should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ")
                    .append(getCompleteTCName())
                    .append(" at site ")
                    .append(txJob.getSiteHandle());
            mLogger.log(error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException(error.toString());
        }

        txJob.namespace = tcEntry.getLogicalNamespace();
        txJob.logicalName = tcEntry.getLogicalName();
        txJob.version = tcEntry.getLogicalVersion();

        txJob.dvName = this.getDerivationName();
        txJob.dvNamespace = this.getDerivationNamespace();
        txJob.dvVersion = this.getDerivationVersion();

        // this should in fact only be set
        // for non third party pools
        /*      JIRA PM-277
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

        // the i/p and o/p files remain empty
        // as we doing just copying urls
        txJob.inputFiles = new HashSet();
        txJob.outputFiles = new HashSet();

        // no stdin file is written out

        // the profile information from the pool catalog needs to be
        // assimilated into the job.        txJob.updateProfiles( ePool.getProfiles() );

        // add any notifications specified in the transformation
        // catalog for the job. JIRA PM-391
        txJob.addNotifications(tcEntry);

        // the profile information from the transformation
        // catalog needs to be assimilated into the job
        // overriding the one from pool catalog.
        txJob.updateProfiles(tcEntry);

        // the profile information from the properties file
        // is assimilated overidding the one from transformation
        // catalog.
        txJob.updateProfiles(mProps);

        // apply the priority to the transfer job
        this.applyPriority(txJob);

        // constructing the arguments to transfer script
        // they only have to be incorporated after the
        // profile incorporation
        txJob.strargs = this.generateArgumentStringAndAssociateCredentials(txJob, file);

        if (execFiles != null) {
            // we need to add setup jobs to change the XBit
            super.addSetXBitJobs(job, txJob, execFiles);
        }
        return txJob;
    }

    /**
     * Returns the namespace of the derivation that this implementation refers to.
     *
     * @return the namespace of the derivation.
     */
    protected abstract String getDerivationNamespace();

    /**
     * Returns the logical name of the derivation that this implementation refers to.
     *
     * @return the name of the derivation.
     */
    protected abstract String getDerivationName();

    /**
     * Returns the version of the derivation that this implementation refers to.
     *
     * @return the version of the derivation.
     */
    protected abstract String getDerivationVersion();

    /**
     * It constructs the arguments to the transfer executable that need to be passed to the
     * executable referred to in this transfer mode.
     *
     * @param job the job containing the transfer node.
     * @param file the FileTransfer that needs to be done.
     * @return the argument string
     */
    protected abstract String generateArgumentStringAndAssociateCredentials(
            TransferJob job, FileTransfer file);

    /**
     * Returns the complete name for the transformation that the implementation is using.
     *
     * @return the complete name.
     */
    protected abstract String getCompleteTCName();
}
