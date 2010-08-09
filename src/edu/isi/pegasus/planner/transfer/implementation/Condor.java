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
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.JobManager;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.NameValue;

import org.griphyn.cPlanner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.Separator;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import java.io.FileWriter;

import java.net.URL;
import org.griphyn.cPlanner.classes.PegasusBag;

/**
 * A prototype implementation that leverages the Condor file transfer mechanism
 * to do the transfer to the remote directory . Currently, this will only
 * work for staging in data to a remote site from the submit host.
 * <p>
 * Additionally, this will only work with local replica selector that prefers
 * file urls from the submit host for staging.
 *
 * <p>
 * In order to use the transfer implementation implemented by this class,
 * <pre>
 *     - property <code>pegasus.transfer.stagein.impl</code> must be set to
 *       value <code>Condor</code>.
 *     - property <code>pegasus.selector.replica</code> must be set to value
 *       <code>Local</code>
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Condor extends AbstractMultipleFTPerXFERJob {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "true";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "true";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION = "Condor File Transfer Mechanism";


    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     *
     * @param  bag  bag of intialization objects.
     */
    public Condor( PegasusBag bag ) {
        super( bag );
    }


    /**
     * Returns a boolean indicating whether the transfer protocol being used
     * by the implementation preserves the X Bit or not while staging.
     *
     * @return false
     */
    public boolean doesPreserveXBit() {
        return false;
    }

    /**
     * It constructs the arguments to the transfer executable that need to be
     * passed to the executable referred to in this transfer mode.
     *
     * @param job the object containing the transfer node.
     * @return the argument string
     */
    protected String generateArgumentString( TransferJob job ) {
        return "";
    }

    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name.
     */
    protected String getCompleteTCName(){
        return Separator.combine( this.TRANSFORMATION_NAMESPACE,
                                  this.TRANSFORMATION_NAME,
                                  this.TRANSFORMATION_VERSION );
    }


    /**
     * Returns the logical name of the derivation that this implementation
     * refers to.
     *
     * @return the name of the derivation.
     */
    protected String getDerivationName() {
        return this.DERIVATION_NAME;
    }

    /**
     * Returns the namespace of the derivation that this implementation
     * refers to.
     *
     * @return the namespace of the derivation.
     */
    protected String getDerivationNamespace() {
        return this.DERIVATION_NAMESPACE;
    }

    /**
     * Returns the version of the derivation that this implementation refers
     * to.
     *
     * @return the version of the derivation.
     */
    protected String getDerivationVersion() {
        return this.DERIVATION_VERSION;
    }

    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public String getDescription() {
        return Condor.DESCRIPTION;
    }

    /**
     * Returns the environment profiles that are required for the default
     * entry to sensibly work. There are no variables to be returned for
     * this case.
     *
     * @param site the site where the job is going to run.
     * @return an empty list
     */
    protected List getEnvironmentVariables( String site ) {
        return new ArrayList( 0 );
    }

    /**
     * Constructs a  condor file transfer job that handles multiple transfers.
     * The job itself is a /bin/true job that currently only manages to
     * transfer input files from the local host.
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
    public TransferJob createTransferJob( SubInfo job,
                                          Collection files,
                                          Collection execFiles,
                                          String txJobName,
                                          int jobClass ) {


        //sanity check
        if( jobClass != SubInfo.STAGE_IN_JOB ){
            throw new RuntimeException( "Condor file transfer can only be used for stagein" );
        }

        TransferJob txJob = new TransferJob();

        //run job always on the site where the compute job runs
        txJob.setSiteHandle( job.getSiteHandle() );

        //the non third party site for the transfer job is
        //always the job execution site for which the transfer
        //job is being created.
        txJob.setNonThirdPartySite(job.getSiteHandle());

        txJob.setName( txJobName );
//        txJob.setUniverse( "globus" );
        txJob.setUniverse( GridGateway.JOB_TYPE.transfer.toString() );

        txJob.setTransformation( this.TRANSFORMATION_NAMESPACE,
                                 this.TRANSFORMATION_NAME,
                                 this.TRANSFORMATION_VERSION );

        txJob.setDerivation( this.DERIVATION_NAMESPACE,
                             this.DERIVATION_NAMESPACE,
                             this.DERIVATION_VERSION );

        txJob.setRemoteExecutable( "/bin/true" );


        //add input files for transfer since we are only doing for
        //creating stagein jobs
        for( Iterator it = files.iterator(); it.hasNext(); ){
            FileTransfer ft = ( FileTransfer )it.next();
            NameValue nv = ft.getSourceURL(  );
            //sanity check first
            if( !nv.getKey().equals( "local" ) ){
                throw new RuntimeException( "Condor File transfer can only do stagein from local site. " +
                                            "Unable to transfer " + ft );
            }
            //put the url in only if it is a file url
            String url = nv.getValue();
            if( url.startsWith( "file:/" ) ){
                try{
                    txJob.condorVariables.addIPFileForTransfer(new URL(url).
                        getPath());
                }
                catch( Exception e ){
                    throw new RuntimeException ( "Malformed source URL " + url );
                }
            }
        }

        //this should in fact only be set
        // for non third party pools
        //we first check if there entry for transfer universe,
        //if no then go for globus
//        SiteInfo ePool = mSCHandle.getTXPoolEntry( txJob.getSiteHandle() );
//        JobManager jobmanager = ePool.selectJobManager( this.TRANSFER_UNIVERSE, true );
//        txJob.setJobManager( ( jobmanager == null) ?
//                               null :
//                               jobmanager.getInfo( JobManager.URL ) );
        
        SiteCatalogEntry ePool = mSiteStore.lookup( txJob.getSiteHandle() );
        GridGateway jobmanager = ePool.selectGridGateway( GridGateway.JOB_TYPE.transfer );
        txJob.setJobManager( ( jobmanager == null) ?
                               null :
                               jobmanager.getContact() );

        txJob.setJobType( jobClass );
        txJob.setVDSSuperNode( job.jobName );

        txJob.stdErr = "";
        txJob.stdOut = "";

        //the i/p and o/p files remain empty
        //as we doing just copying urls
        txJob.inputFiles = new HashSet();

        //to get the file stat information we need to put
        //the files as output files of the transfer job
        txJob.outputFiles = new HashSet( files );


        //the profile information from the pool catalog needs to be
        //assimilated into the job.
//        txJob.updateProfiles( mSCHandle.getPoolProfile( txJob.getSiteHandle() ) );
        txJob.updateProfiles( ePool.getProfiles()  );

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
 //       txJob.updateProfiles(tcEntry);

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        txJob.updateProfiles( mProps );

        //apply the priority to the transfer job
        this.applyPriority( txJob );

        //constructing the arguments to transfer script
        //they only have to be incorporated after the
        //profile incorporation
        txJob.strargs = this.generateArgumentString(txJob);

        if(execFiles != null){
            //we need to add setup jobs to change the XBit
            super.addSetXBitJobs( job, txJob, execFiles );
        }


        return txJob;
    }


    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param siteHandle the handle of the site where the transformation is
     *   to be searched.
     * @return the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry( String siteHandle ) {
        return null;
    }

    /**
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. Fix me. should say NEVER.
     *
     * @return boolean indicating whether to always use third party
     *   transfers or not.
     *
     */
    public boolean useThirdPartyTransferAlways() {
        return false;
    }

    /**
     * Writes to a FileWriter stream the stdin which goes into the magic
     * script via standard input
     *
     * @param stdIn the writer to the stdin file.
     * @param files Collection of <code>FileTransfer</code> objects
     *   containing the information about sourceam fin and destURL's.
     * @throws Exception
     */
    protected void writeJumboStdIn( FileWriter stdIn, Collection files ) throws
        Exception {
    }
}
