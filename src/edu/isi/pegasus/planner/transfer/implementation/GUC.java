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
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * The implementation that is used to create transfer jobs that callout to the new globus-url-copy
 * client, that support multiple file transfers
 *
 * <p>In order to use the transfer implementation implemented by this class,
 *
 * <pre>
 *        - the property pegasus.transfer.*.impl must be set to value GUC.
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
 * @version $Revision: 145 $
 */
public class GUC extends AbstractMultipleFTPerXFERJob {

    /** The transformation namespace for the transfer job. */
    public static final String TRANSFORMATION_NAMESPACE = "globus";

    /**
     * The name of the underlying transformation that is queried for in the Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "guc";

    /** The version number for the transfer job. */
    public static final String TRANSFORMATION_VERSION = null;

    /** The derivation namespace for for the transfer job. */
    public static final String DERIVATION_NAMESPACE = "globus";

    /** The name of the underlying derivation. */
    public static final String DERIVATION_NAME = "guc";

    /** The derivation version number for the transfer job. */
    public static final String DERIVATION_VERSION = null;

    /** A short description of the transfer implementation. */
    public static final String DESCRIPTION =
            "GUC client that supports multiple file transfers. Available in globus 4.x series";

    /** The number of streams that each g-u-c process opens to do the ftp transfer. */
    protected String mNumOfTXStreams;

    /** Whether to use force option for the transfer executable or not. */
    protected boolean mUseForce;

    /**
     * The overloaded constructor, that is called by the Factory to load the class.
     *
     * @param bag the bag of Pegasus initialization objects.
     */
    public GUC(PegasusBag bag) {
        super(bag);
        mNumOfTXStreams = mProps.getNumOfTransferStreams();
        mUseForce = mProps.useForceInTransfer();
    }

    /**
     * Return a boolean indicating whether the transfers to be done always in a third party transfer
     * mode. A value of false, results in the direct or peer to peer transfers being done.
     *
     * <p>A value of false does not preclude third party transfers. They still can be done, by
     * setting the property "pegasus.transfer.*.thirdparty.sites".
     *
     * @return boolean indicating whether to always use third party transfers or not.
     */
    public boolean useThirdPartyTransferAlways() {
        return false;
    }

    /**
     * Returns a boolean indicating whether the transfer protocol being used by the implementation
     * preserves the X Bit or not while staging.
     *
     * @return boolean
     */
    public boolean doesPreserveXBit() {
        return false;
    }

    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public String getDescription() {
        return this.DESCRIPTION;
    }

    /**
     * Retrieves the transformation catalog entry for the executable that is being used to transfer
     * the files in the implementation.
     *
     * @param siteHandle the handle of the site where the transformation is to be searched.
     * @param jobClass the job Class for the newly added job. Can be one of the following: stage-in
     *     stage-out inter-pool transfer stage-in worker transfer
     * @return the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry(
            String siteHandle, int jobClass) {
        List tcentries = null;
        try {
            // namespace and version are null for time being
            tcentries =
                    mTCHandle.lookup(
                            this.TRANSFORMATION_NAMESPACE,
                            this.TRANSFORMATION_NAME,
                            this.TRANSFORMATION_VERSION,
                            siteHandle,
                            TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                    "Unable to retrieve entry from TC for " + getCompleteTCName() + " Cause:" + e,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }

        return (tcentries == null)
                ? this.defaultTCEntry(
                        this.TRANSFORMATION_NAMESPACE,
                        this.TRANSFORMATION_NAME,
                        this.TRANSFORMATION_VERSION,
                        siteHandle)
                : // try using a default one
                (TransformationCatalogEntry) tcentries.get(0);
    }

    /**
     * Returns a default TC entry to be used in case entry is not found in the transformation
     * catalog.
     *
     * @param namespace the namespace of the transfer transformation
     * @param name the logical name of the transfer transformation
     * @param version the version of the transfer transformation
     * @param site the site for which the default entry is required.
     * @return the default entry.
     */
    protected TransformationCatalogEntry defaultTCEntry(
            String namespace, String name, String version, String site) {

        TransformationCatalogEntry defaultTCEntry = null;

        mLogger.log(
                "Creating a default TC entry for "
                        + Separator.combine(namespace, name, version)
                        + " at site "
                        + site,
                LogManager.DEBUG_MESSAGE_LEVEL);

        // get the essential environment variables required to get
        // it to work correctly
        List envs = this.getEnvironmentVariables(site);
        if (envs == null) {
            // cannot create default TC
            mLogger.log(
                    "Unable to create a default entry for as could not construct necessary environment "
                            + Separator.combine(namespace, name, version),
                    LogManager.DEBUG_MESSAGE_LEVEL);
            // set the flag back to true
            return defaultTCEntry;
        }

        // get the GLOBUS_LOCATION PROFILE
        String globusLocation = null;
        for (Iterator it = envs.iterator(); it.hasNext(); ) {
            Profile p = (Profile) it.next();
            if (p.getProfileKey().equals("GLOBUS_LOCATION")) {
                globusLocation = p.getProfileValue();
                break;
            }
        }

        // if home is still null
        if (globusLocation == null) {
            // cannot create default TC
            mLogger.log(
                    "Unable to create a default entry for "
                            + Separator.combine(namespace, name, version)
                            + " as GLOBUS_LOCATION is not set in Site Catalog",
                    LogManager.WARNING_MESSAGE_LEVEL);
            // set the flag back to true
            return defaultTCEntry;
        }

        // remove trailing / if specified
        globusLocation =
                (globusLocation.charAt(globusLocation.length() - 1) == File.separatorChar)
                        ? globusLocation.substring(0, globusLocation.length() - 1)
                        : globusLocation;

        // construct the path to it
        StringBuffer path = new StringBuffer();
        path.append(globusLocation)
                .append(File.separator)
                .append("bin")
                .append(File.separator)
                .append("globus-url-copy");

        defaultTCEntry = new TransformationCatalogEntry(namespace, name, version);

        defaultTCEntry.setPhysicalTransformation(path.toString());
        defaultTCEntry.setResourceId(site);
        defaultTCEntry.setType(TCType.INSTALLED);
        defaultTCEntry.addProfiles(envs);
        defaultTCEntry.setSysInfo(this.mSiteStore.lookup(site).getSysInfo());

        // register back into the transformation catalog
        // so that we do not need to worry about creating it again
        try {
            mTCHandle.insert(defaultTCEntry, false);
        } catch (Exception e) {
            // just log as debug. as this is more of a performance improvement
            // than anything else
            mLogger.log(
                    "Unable to register in the TC the default entry "
                            + defaultTCEntry.getLogicalTransformation()
                            + " for site "
                            + site,
                    e,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }
        mLogger.log(
                "Created entry with path " + defaultTCEntry.getPhysicalTransformation(),
                LogManager.DEBUG_MESSAGE_LEVEL);
        return defaultTCEntry;
    }

    /**
     * Returns the environment profiles that are required for the default entry to sensibly work.
     *
     * @param site the site where the job is going to run.
     * @return List of environment variables, else null in case where the required environment
     *     variables could not be found.
     */
    protected List getEnvironmentVariables(String site) {
        List result = new ArrayList(2);

        // create the CLASSPATH from home
        String globus = mSiteStore.getEnvironmentVariable(site, "GLOBUS_LOCATION");
        if (globus == null) {
            mLogger.log(
                    "GLOBUS_LOCATION not set in site catalog for site " + site,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            return null;
        }

        // check for LD_LIBRARY_PATH
        String ldpath = mSiteStore.getEnvironmentVariable(site, "LD_LIBRARY_PATH");
        if (ldpath == null) {
            // construct a default LD_LIBRARY_PATH
            ldpath = globus;
            // remove trailing / if specified
            ldpath =
                    (ldpath.charAt(ldpath.length() - 1) == File.separatorChar)
                            ? ldpath.substring(0, ldpath.length() - 1)
                            : ldpath;

            ldpath = ldpath + File.separator + "lib";
            mLogger.log(
                    "Constructed default LD_LIBRARY_PATH " + ldpath,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }

        // we have both the environment variables
        result.add(new Profile(Profile.ENV, "GLOBUS_LOCATION", globus));
        result.add(new Profile(Profile.ENV, "LD_LIBRARY_PATH", ldpath));

        return result;
    }

    /**
     * Returns the namespace of the derivation that this implementation refers to.
     *
     * @return the namespace of the derivation.
     */
    protected String getDerivationNamespace() {
        return this.DERIVATION_NAMESPACE;
    }

    /**
     * Returns the logical name of the derivation that this implementation refers to.
     *
     * @return the name of the derivation.
     */
    protected String getDerivationName() {
        return this.DERIVATION_NAME;
    }

    /**
     * Returns the version of the derivation that this implementation refers to.
     *
     * @return the version of the derivation.
     */
    protected String getDerivationVersion() {
        return this.DERIVATION_VERSION;
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
        sb.append(" -f ").append(job.getStdIn());

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
        File f = new File(mPOptions.getSubmitDirectory(), job.getStdIn());
        // add condor key transfer_input_files to transfer the file
        job.condorVariables.addIPFileForTransfer(f.getAbsolutePath());
        job.setStdIn("");
    }

    /**
     * Writes to a FileWriter stream the stdin which goes into the magic script via standard input
     *
     * @param job the transfer job.
     * @param writer the writer to the stdin file.
     * @param files Collection of <code>FileTransfer</code> objects containing the information about
     *     sourceam fin and destURL's.
     * @param stagingSite the site where the data will be populated by first level staging jobs.
     * @param jobClass the job Class for the newly added job. Can be one of the following: stage-in
     *     stage-out inter-pool transfer
     * @throws Exception
     */
    protected void writeStdInAndAssociateCredentials(
            TransferJob job, FileWriter writer, Collection files, String stagingSite, int jobClass)
            throws Exception {
        for (Iterator it = files.iterator(); it.hasNext(); ) {
            FileTransfer ft = (FileTransfer) it.next();
            NameValue<String, String> source = ft.getSourceURL();
            // we want to leverage multiple dests if possible
            NameValue<String, String> dest = ft.getDestURL(true);
            StringBuffer entry = new StringBuffer();
            entry.append("#")
                    .append(source.getKey())
                    .append(" ")
                    .append(dest.getKey())
                    .append("\n")
                    .append(source.getValue())
                    .append(" ")
                    .append(dest.getValue())
                    .append("\n");
            writer.write(entry.toString());
            writer.flush();

            // associate any credential required , both with destination
            // and the source urls
            job.addCredentialType(source.getKey(), source.getValue());
            job.addCredentialType(dest.getKey(), dest.getValue());
        }
    }

    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name.
     */
    protected String getCompleteTCName() {
        return Separator.combine(
                GUC.TRANSFORMATION_NAMESPACE, GUC.TRANSFORMATION_NAME, GUC.TRANSFORMATION_VERSION);
    }
}
