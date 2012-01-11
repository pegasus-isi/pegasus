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
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.catalog.site.impl.old.classes.SiteInfo;
import edu.isi.pegasus.planner.catalog.site.impl.old.classes.JobManager;


import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.namespace.Pegasus;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.common.util.Separator;


import java.util.Collection;
import java.util.Iterator;
import java.util.HashSet;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * The implementation that creates transfer jobs referring to the stork data
 * placement scheduler that can handle only one transfer per job.
 *
 * <p>
 * Stork is directly invoked by DAGMAN. The appropriate Stork modules need to
 * be installed on the submit host.
 *
 * <p>
 * It leads to the creation of the setup chmod jobs to the workflow, that appear
 * as parents to compute jobs in case the transfer implementation does not
 * preserve the X bit on the file being transferred. This is required for
 * staging of executables as part of the workflow. The setup jobs are only added
 * as children to the stage in jobs.
 *
 * <p>
 * In order to use the transfer implementation implemented by this class, the
 * property <code>vds.transfer.*.impl</code> must be set to
 * value <code>Stork</code>.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Stork extends AbstractSingleFTPerXFERJob {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = null;

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "stork";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "condor";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "stork";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION =
                    "Stork Data Placement Scheduler that does only one transfer per invocation";


    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param  bag  bag of intialization objects.
     */
    public Stork( PegasusBag bag ){
        super( bag );
    }

    /**
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. A value of false, results in the
     * direct or peer to peer transfers being done.
     * <p>
     * A value of false does not preclude third party transfers. They still can
     * be done, by setting the property "vds.transfer.*.thirdparty.sites".
     *
     * @return boolean indicating whether to always use third party transfers
     *         or not.
     *
     */
    public boolean useThirdPartyTransferAlways(){
        return true;
    }

    /**
     * Returns a boolean indicating whether the transfer protocol being used by
     * the implementation preserves the X Bit or not while staging.
     *
     * @return boolean
     */
    public boolean doesPreserveXBit(){
        return false;
    }


    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public  String getDescription(){
        return this.DESCRIPTION;
    }

    /**
     *  Constructs a general transfer job that handles single transfers per
     * transfer job. There are appropriate callouts to generate the implementation
     * specific details. It throws an error if asked to create a transfer job
     * for more than one transfer.
     *
     * @param job         the Job object for the job, in relation to which
     *                    the transfer node is being added. Either the transfer
     *                    node can be transferring this jobs input files to
     *                    the execution pool, or transferring this job's output
     *                    files to the output pool.
     * @param file        collection of <code>FileTransfer</code> objects
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
    public TransferJob createTransferJob(Job job,
                                         FileTransfer file,
                                         Collection execFiles,
                                         String txJobName,
                                         int jobClass) {

        TransferJob txJob = new TransferJob();
        SiteInfo ePool;
        JobManager jobmanager;

        //Stork does the transfer . Hence set the transfer pool to stork
        String tPool = "stork";

        //the non third party site for the transfer job is
        //always the job execution site for which the transfer
        //job is being created.
        txJob.setNonThirdPartySite( job.getStagingSiteHandle() );


        //we first check if there entry for transfer universe,
        //if no then go for globus
    //    ePool = mSCHandle.getTXPoolEntry(tPool);

        txJob.jobName = txJobName;
        txJob.executionPool = tPool;

//        txJob.condorUniverse = "globus";
        txJob.setUniverse( GridGateway.JOB_TYPE.transfer.toString() );

     


        txJob.namespace   = this.TRANSFORMATION_NAMESPACE;
        txJob.logicalName = this.TRANSFORMATION_NAME;
        txJob.version     = null;

        txJob.dvName      = this.getDerivationName();
        txJob.dvNamespace = this.getDerivationNamespace();
        txJob.dvVersion   = this.getDerivationVersion();

        //this should in fact only be set
        // for non third party pools
//        jobmanager = ePool.selectJobManager(this.TRANSFER_UNIVERSE,true);
//        txJob.globusScheduler = (jobmanager == null) ?
//                                  null :
//                                  jobmanager.getInfo(JobManager.URL);

        txJob.jobClass = jobClass;
        txJob.jobID = job.jobName;

        txJob.stdErr = "";
        txJob.stdOut = "";

        txJob.executable = null;

        //the i/p and o/p files remain empty
        //as we doing just copying urls
        txJob.inputFiles = new HashSet();
        txJob.outputFiles = new HashSet();

        //no stdin file is written out

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
//        txJob.updateProfiles(mSCHandle.getPoolProfile(tPool));
        txJob.updateProfiles( mSiteStore.lookup(tPool).getProfiles() );

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        //txJob.updateProfiles(tcEntry);

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
        txJob.strargs = this.generateArgumentStringAndAssociateCredentials(txJob,file);

        if(execFiles != null){
            //we need to add setup jobs to change the XBit
            super.addSetXBitJobs(job,txJob,execFiles);
        }
        return txJob;
    }


    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation. The entry
     * does not refer to any physical path.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     * @param jobClass    the job Class for the newly added job. Can be one of the
     *                    following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     *                              stage-in worker transfer
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry(String siteHandle, int jobClass ){
       return new TransformationCatalogEntry(this.TRANSFORMATION_NAMESPACE,
                                             this.TRANSFORMATION_NAME,
                                             this.TRANSFORMATION_VERSION);
    }

    /**
     * Returns the namespace of the derivation that this implementation
     * refers to.
     *
     * @return the namespace of the derivation.
     */
    protected String getDerivationNamespace(){
        return this.DERIVATION_NAMESPACE;
    }


    /**
     * Returns the logical name of the derivation that this implementation
     * refers to.
     *
     * @return the name of the derivation.
     */
    protected String getDerivationName(){
        return this.DERIVATION_NAME;
    }

    /**
     * Returns the version of the derivation that this implementation
     * refers to.
     *
     * @return the version of the derivation.
     */
    protected String getDerivationVersion(){
        return this.DERIVATION_VERSION;
    }


    /**
     * It constructs the arguments to the transfer executable that need to be passed
     * to the executable referred to in this transfer mode.
     *
     * @param job  the transfer job that is being created.
     * @param file the FileTransfer that needs to be done.
     * @return  the argument string
     */
    protected String generateArgumentStringAndAssociateCredentials(TransferJob job,FileTransfer file){
        StringBuffer sb = new StringBuffer();
        if(job.vdsNS.containsKey(Pegasus.TRANSFER_ARGUMENTS_KEY)){
            sb.append(
                job.vdsNS.removeKey(Pegasus.TRANSFER_ARGUMENTS_KEY)
                );
         }
         String source = ((NameValue)file.getSourceURL()).getValue();
         String dest   = ((NameValue)file.getDestURL()).getValue();
         sb.append( source ).append("\n").
            append( dest );

         job.addCredentialType( source );
         job.addCredentialType( dest );

        return sb.toString();
    }

    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name.
     */
    protected String getCompleteTCName(){
        return Separator.combine(this.TRANSFORMATION_NAMESPACE,
                                 this.TRANSFORMATION_NAME,
                                 this.TRANSFORMATION_VERSION);
    }
}
