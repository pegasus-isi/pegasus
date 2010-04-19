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

import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.Profile;

import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.VDS;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import org.griphyn.common.util.Separator;



import java.io.FileWriter;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;


import java.util.StringTokenizer;
import java.io.File;
import org.griphyn.cPlanner.classes.PegasusBag;

/**
 * The implementation that creates transfer jobs referring to the transfer
 * executable distributed with the VDS.
 *
 * <p>
 * The Transfer client is generally invoked on the remote execution sites, unless
 * the user uses the thirdparty transfer option, in which case the transfer is
 * invoked on the submit host. Hence there should be an entry in the transformation
 * catalog for logical transformation <code>Transfer</code> at the execution sites.
 * Transfer is distributed as part of the VDS worker package and can be found at
 * $PEGASUS_HOME/bin/transfer.
 * <p>
 * It leads to the creation of the setup chmod jobs to the workflow, that appear
 * as parents to compute jobs in case the transfer implementation does not
 * preserve the X bit on the file being transferred. This is required for
 * staging of executables as part of the workflow. The setup jobs are only added
 * as children to the stage in jobs.
 * <p>
 * In order to use the transfer implementation implemented by this class, the
 * property <code>pegasus.transfer.*.impl</code> must be set to
 * value <code>Transfer</code>.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Transfer extends AbstractMultipleFTPerXFERJob {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "transfer";

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
    public static final String DERIVATION_NAME = "transfer";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION = "Pegasus Transfer Wrapper around GUC";

    /**
     * The number of g-u-c processes that are spawned to transfer the files in
     * one transfer job.
     */
    protected String mNumOfTXProcesses;

    /**
     * The number of streams that each g-u-c process opens to do the ftp transfer.
     */
    protected String mNumOfTXStreams;

    /**
     * Whether to use force option for the transfer executable or not.
     */
    protected boolean mUseForce;



    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param bag   the bag of initialization objects.
     */
    public Transfer( PegasusBag bag ){
        super( bag );
        mNumOfTXProcesses = mProps.getNumOfTransferProcesses();
        mNumOfTXStreams   = mProps.getNumOfTransferStreams();
        mUseForce         = mProps.useForceInTransfer();

    }

    /**
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. A value of false, results in the
     * direct or peer to peer transfers being done.
     * <p>
     * A value of false does not preclude third party transfers. They still can
     * be done, by setting the property "pegasus.transfer.*.thirdparty.sites".
     *
     * @return boolean indicating whether to always use third party transfers
     *         or not.
     *
     */
    public boolean useThirdPartyTransferAlways(){
        return false;
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
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry(String siteHandle){
        List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.getTCEntries(this.TRANSFORMATION_NAMESPACE,
                                               this.TRANSFORMATION_NAME,
                                               this.TRANSFORMATION_VERSION,
                                               siteHandle,
                                               TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " + getCompleteTCName()
                + " Cause:" + e, LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return ( tcentries == null ) ?
                 this.defaultTCEntry( this.TRANSFORMATION_NAMESPACE,
                                      this.TRANSFORMATION_NAME,
                                      this.TRANSFORMATION_VERSION,
                                      siteHandle ): //try using a default one
                 (TransformationCatalogEntry) tcentries.get(0);



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
    protected List getEnvironmentVariables( String site ){
        List result = new ArrayList(2) ;

        //create the CLASSPATH from home
        String globus = mSiteStore.getEnvironmentVariable( site, "GLOBUS_LOCATION" );
        if( globus == null ){
            mLogger.log( "GLOBUS_LOCATION not set in site catalog for site " + site,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }



        //check for LD_LIBRARY_PATH
        String ldpath = mSiteStore.getEnvironmentVariable( site, "LD_LIBRARY_PATH" );
        if ( ldpath == null ){
            //construct a default LD_LIBRARY_PATH
            ldpath = globus;
            //remove trailing / if specified
            ldpath = ( ldpath.charAt( ldpath.length() - 1 ) == File.separatorChar )?
                                ldpath.substring( 0, ldpath.length() - 1 ):
                                ldpath;

            ldpath = ldpath + File.separator + "lib";
            mLogger.log( "Constructed default LD_LIBRARY_PATH " + ldpath,
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }

        //we have both the environment variables
        result.add( new Profile( Profile.ENV, "GLOBUS_LOCATION", globus) );
        result.add( new Profile( Profile.ENV, "LD_LIBRARY_PATH", ldpath) );

        return result;
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
     * @param job   the object containing the transfer node.
     * @return  the argument string
     */
    protected String generateArgumentString(TransferJob job) {
        StringBuffer sb = new StringBuffer();
        if(job.vdsNS.containsKey(VDS.TRANSFER_ARGUMENTS_KEY)){
            sb.append(
                      job.vdsNS.removeKey(VDS.TRANSFER_ARGUMENTS_KEY)
                      );
        }
        else{
            sb.append(" -P ").append(mNumOfTXProcesses).
                append(" -p ").append(mNumOfTXStreams);

            sb = (this.mUseForce)?
                sb.append(" -f ") :
                sb;
        }

        sb.append(" base-uri se-mount-point");

        return sb.toString();
    }


    /**
     * Writes to a FileWriter stream the stdin which goes into the magic script
     * via standard input
     *
     * @param writer    the writer to the stdin file.
     * @param files    Collection of <code>FileTransfer</code> objects containing
     *                 the information about sourceam fin and destURL's.
     *
     *
     * @throws Exception
     */
    protected void writeJumboStdIn(FileWriter writer, Collection files) throws
        Exception {
        for(Iterator it = files.iterator();it.hasNext();){
            FileTransfer ft = (FileTransfer) it.next();
            NameValue source = ft.getSourceURL();
            //we want to leverage multiple dests if possible
            NameValue dest   = ft.getDestURL( true );
            writer.write("#" + source.getKey() + "\n" + source.getValue() + "\n#" +
                        dest.getKey() + "\n" + dest.getValue() + "\n");
            writer.flush();
        }


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
