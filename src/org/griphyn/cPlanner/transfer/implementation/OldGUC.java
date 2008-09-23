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

import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.util.Separator;

import java.io.FileWriter;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.griphyn.cPlanner.classes.PegasusBag;

/**
 * The implementation that creates transfer jobs referring to the old GUC
 * that could handle only one transfer per guc invocation.
 *
 * <p>
 * The client is generally invoked on the remote execution sites, unless
 * the user uses the thirdparty transfer option, in which case the transfer is
 * invoked on the submit host. Hence there should be an entry in the transformation
 * catalog for logical transformation <code>globus-url-copy</code> at the
 * execution sites.
 * GUC is distributed as part of the VDS worker package and can be found at
 * $GLOBUS_LOCATION/bin/globus-url-copy.
 *
 * <p>
 * It leads to the creation of the setup chmod jobs to the workflow, that appear
 * as parents to compute jobs in case the transfer implementation does not
 * preserve the X bit on the file being transferred. This is required for
 * staging of executables as part of the workflow. The setup jobs are only added
 * as children to the stage in jobs.
 *
 * <p>
 * In order to use the transfer implementation implemented by this class,
 * <pre>
 *        - the property vds.transfer.*.impl must be set to value OldGUC.
 * </pre>
 *
 * <p>
 * There should be an entry in the transformation catalog with the fully qualified
 * name as <code>globus-url-copy</code> for all the sites where workflow is run,
 * or on the local site in case of third party transfers.
 *
 * <p>
 * The arguments with which the client is invoked can be specified
 * <pre>
 *       - by specifying the property vds.transfer.arguments
 *       - associating the VDS profile key transfer.arguments
 * </pre>

 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class OldGUC extends AbstractSingleFTPerXFERJob {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = null;

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "globus-url-copy";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "VDS";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "globus-url-copy";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION =
                    "Old GUC client that does only one transfer per invocation";

    /**
     * The number of streams that each g-u-c process opens to do the ftp transfer.
     */
    protected String mNumOfTXStreams;

    /**
     * Whether to use force option for the transfer executable or not.
     */
    protected boolean mUseForce;

    /**
     * A boolean indicating whehter to quote the urls or not.
     */
    protected boolean mQuoteURL;


    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param  bag  bag of intialization objects.
     */
    public OldGUC( PegasusBag bag ){
        super( bag );
        mNumOfTXStreams   = mProps.getNumOfTransferStreams();
        mUseForce         = mProps.useForceInTransfer();
        mQuoteURL       = mProps.quoteTransferURL();
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
     * @see PegasusProperties#getThirdPartySites(String)
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
                + " :" + e.getMessage(),LogManager.ERROR_MESSAGE_LEVEL);
        }

        //see if any record is returned or not
        return(tcentries == null)?
               null:
              (TransformationCatalogEntry) tcentries.get(0);
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
    protected String generateArgumentString(TransferJob job,FileTransfer file){
        StringBuffer sb = new StringBuffer();

        if(job.vdsNS.containsKey(VDS.TRANSFER_ARGUMENTS_KEY)){
            sb.append(
                      job.vdsNS.removeKey(VDS.TRANSFER_ARGUMENTS_KEY)
                      );
        }
        else{
            //append the number of streams that are
            //opened for each transfer
            sb.append(" -p ").append(mNumOfTXStreams).append(" ");
        }

        sb = (mQuoteURL)?sb.append("'"):sb;
        sb.append( ((NameValue)file.getSourceURL()).getValue());

        sb = (mQuoteURL)?sb.append("' '"):sb.append(" ");

        sb.append( ((NameValue)file.getDestURL()).getValue());
        sb = (mQuoteURL)?sb.append("'"):sb;

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
