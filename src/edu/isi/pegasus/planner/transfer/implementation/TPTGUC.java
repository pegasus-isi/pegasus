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

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.File;

/**
 * The implementation that is used to create transfer jobs that callout to the new globus-url-copy
 * client, that support multiple file transfers
 *
 * <p>In order to use the transfer implementation implemented by this class,
 *
 * <pre>
 *        - the property pegasus.transfer.*.impl must be set to value TPTGUC.
 * </pre>
 *
 * <p>There should be an entry in the transformation catalog with the fully qualified name as <code>
 * globus::guc</code> for all the sites where workflow is run, or on the local site in case of third
 * party transfers.
 *
 * <p>Pegasus can automatically construct the path to the globus-url-copy client, if the environment
 * variable GLOBUS_LOCATION is specified in the site catalog for the site.
 *
 * <p>The arguments with which the client is invoked can be specified
 *
 * <pre>
 *       - by specifying the property pegasus.transfer.arguments
 *       - associating the Pegasus profile key transfer.arguments
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TPTGUC extends GUC {

    /**
     * The overloaded constructor, that is called by the Factory to load the class.
     *
     * @param bag the bag of Pegasus initialization objects.
     */
    public TPTGUC(PegasusBag bag) {
        super(bag);
    }

    /**
     * Return a boolean indicating whether the transfers to be done always in a third party transfer
     * mode. A value of false, results in the direct or peer to peer transfers being done.
     *
     * <p>A value of false does not preclude third party transfers. They still can be done, by
     * setting the property "pegasus.transfer.*.thirdparty.sites".
     *
     * @return true always
     */
    public boolean useThirdPartyTransferAlways() {
        return true;
    }

    /**
     * It constructs the arguments to the transfer executable that need to be passed to the
     * executable referred to in this transfer mode.
     *
     * @param job the object containing the transfer node.
     * @return the argument string
     */
    protected String generateArgumentString(TransferJob job) {
        StringBuffer sb = new StringBuffer();
        if (job.vdsNS.containsKey(Pegasus.TRANSFER_ARGUMENTS_KEY)) {
            sb.append(job.vdsNS.removeKey(Pegasus.TRANSFER_ARGUMENTS_KEY));
        } else {
            // just add the default -p option
            sb.append(" -p ").append(mNumOfTXStreams);
        }

        // always append -cd option and verbose option
        sb.append(" -cd -vb");
        // specify the name of the stdin file on command line
        // since this transfer mode only executes on submit node
        // we can give full path to the stdin
        File f = new File(mPOptions.getSubmitDirectory(), job.getStdIn());
        sb.append(" -f ").append(f.getAbsolutePath());

        return sb.toString();
    }

    /**
     * Makes sure the stdin is transferred by the Condor File Transfer Mechanism. In addition, the
     * stdin is set to null, after the file has been marked for transfer by Condor File Transfer
     * Mechanism.
     *
     * @param job the <code>TransferJob</code> that has been created.
     */
    public void postProcess(TransferJob job) {
        super.postProcess(job);
        job.setStdIn("");
    }
}
