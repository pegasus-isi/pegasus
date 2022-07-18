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
package edu.isi.pegasus.planner.transfer.sls;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerCache;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.transfer.SLS;
import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * This uses the Condor File Transfer mechanism for the second level staging.
 *
 * <p>It will work only if the Pegasus Style profile ( pegasus::style ) has a value of condor.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Condor implements SLS {

    /** A short description of the transfer implementation. */
    public static final String DESCRIPTION = "Condor File Transfer Mechanism";

    /** The handle to the site catalog. */
    //    protected PoolInfoProvider mSiteHandle;
    protected SiteStore mSiteStore;

    /** The handle to the properties. */
    protected PegasusProperties mProps;

    /** The handle to the planner options. */
    protected PlannerOptions mPOptions;

    /** The handle to the logging manager. */
    protected LogManager mLogger;

    /**
     * A SimpleFile Replica Catalog, that tracks all the files that are being materialized as part
     * of workflow execution.
     */
    private PlannerCache mPlannerCache;

    /**
     * Handle to the Transfer SLS implementation that we use for supporting deep LFN renaming on
     * stage-in of input files in the PegasusLite Script of the job
     */
    private Transfer mTransferSLSHandle;

    /** The default constructor. */
    public Condor() {}

    /**
     * Initializes the SLS implementation.
     *
     * @param bag the bag of objects. Contains access to catalogs etc.
     */
    public void initialize(PegasusBag bag) {
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
        mLogger = bag.getLogger();
        mSiteStore = bag.getHandleToSiteStore();
        mPlannerCache = bag.getHandleToPlannerCache();
        mTransferSLSHandle = new Transfer();
        mTransferSLSHandle.initialize(bag);
    }

    /**
     * Returns a boolean whether the SLS implementation does a condor based modification or not. By
     * condor based modification we mean whether it uses condor specific classads to achieve the
     * second level staging or not.
     *
     * @return false
     */
    public boolean doesCondorModifications() {
        return true;
    }

    /**
     * Constructs a command line invocation for a job, with a given sls file. The SLS maybe null. In
     * the case where SLS impl does not read from a file, it is advised to create a file in
     * generateSLSXXX methods, and then read the file in this function and put it on the command
     * line.
     *
     * @param job the job that is being sls enabled
     * @param slsFile the slsFile can be null
     * @return invocation string
     */
    public String invocationString(Job job, File slsFile) {
        return this.mTransferSLSHandle.invocationString(job, slsFile);
    }

    /**
     * Returns a boolean indicating whether it will an input file for a job to do the transfers.
     * Transfer reads from stdin the file transfers that it needs to do.
     *
     * @param job the job being detected.
     * @return true if any input file is a deep LFN, else false
     */
    @Override
    public boolean needsSLSInputTransfers(Job job) {
        for (PegasusFile pf : job.getInputFiles()) {
            String lfn = pf.getLFN();
            if (lfn.contains(File.separator)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a boolean indicating whether it will an output file for a job to do the transfers.
     * Transfer reads from stdin the file transfers that it needs to do.
     *
     * @param job the job being detected.
     * @return false
     */
    public boolean needsSLSOutputTransfers(Job job) {
        return false;
    }

    /**
     * Returns the LFN of sls input file.
     *
     * @param job Job
     * @return the name of the sls input file.
     */
    public String getSLSInputLFN(Job job) {
        return null;
    }

    /**
     * Returns the LFN of sls output file.
     *
     * @param job Job
     * @return the name of the sls input file.
     */
    public String getSLSOutputLFN(Job job) {
        return null;
    }

    /**
     * Generates a second level staging file of the input files to the worker node directory. For
     * CondorFile IO this usually returns null, unless there are deep lfn's associated with the
     * files to be transferred. In this case, this function allows us to get around the missing
     * transfer_input_remaps functionality in HTCondor
     *
     * @param job job for which the file is being created
     * @param fileName name of the file that needs to be written out.
     * @param stagingSiteServer the file server on the staging site to be used for retrieval of
     *     files i.e the get operation
     * @param stagingSiteDirectory directory on the head node of the compute site.
     * @param workerNodeDirectory worker node directory
     * @return a Collection of FileTransfer objects listing the transfers that need to be done.
     * @see #needsSLSInputTransfers( Job)
     */
    public Collection<FileTransfer> determineSLSInputTransfers(
            Job job,
            String fileName,
            FileServer stagingSiteServer,
            String stagingSiteDirectory,
            String workerNodeDirectory) {

        Set files = job.getInputFiles();
        Collection<FileTransfer> result = new LinkedList();
        String destDir = workerNodeDirectory;
        for (Iterator it = files.iterator(); it.hasNext(); ) {
            PegasusFile pf = (PegasusFile) it.next();
            String lfn = pf.getLFN();

            if (lfn.equals(ENV.X509_USER_PROXY_KEY)) {
                // ignore the proxy file for time being
                // as we picking it from the head node directory
                continue;
            }

            // PM-1875 only when a lfn is deep LFN, we create file transfer pairs
            // that use pegasus-transfer to rename the files in the condor
            // scratch directory that is the same as the bash variable
            // $pegasus_lite_work_dir in the PegaussLite script at job runtime
            boolean deepLFN = lfn.contains(File.separator);
            if (!deepLFN) {
                continue;
            }
            // In CondorIO case, condor file io has already  gotten the job the compute site
            // before PegasusLitejob starts
            result.add(
                    fileTransferForCopyOfInputs(
                            pf,
                            job.getSiteHandle(),
                            destDir,
                            // PM-1875 we expand escape varialbe if job run in container
                            job.getContainer() != null));
        }
        return result;
    }

    /**
     * Generates a second level staging file of the input files to the worker node directory.
     *
     * @param job the job for which the file is being created
     * @param fileName the name of the file that needs to be written out.
     * @param stagingSiteServer the file server on the staging site to be used for retrieval of
     *     files i.e the put operation
     * @param stagingSiteDirectory the directory on the head node of the compute site.
     * @param workerNodeDirectory the worker node directory
     * @return a Collection of FileTransfer objects listing the transfers that need to be done.
     * @see #needsSLSOutputTransfers( Job)
     */
    public Collection<FileTransfer> determineSLSOutputTransfers(
            Job job,
            String fileName,
            FileServer stagingSiteServer,
            String stagingSiteDirectory,
            String workerNodeDirectory) {

        return null;
    }

    /**
     * Modifies a job for the first level staging to headnode.This is to add any files that needs to
     * be staged to the head node for a job specific to the SLS implementation. If any file needs to
     * be added, a <code>FileTransfer</code> object should be created and added as an input or an
     * output file.
     *
     * @param job the job
     * @param submitDir the submit directory
     * @param slsInputLFN the sls input file if required, that is used for staging in from the head
     *     node to worker node directory.
     * @param slsOutputLFN the sls output file if required, that is used for staging in from the
     *     head node to worker node directory.
     * @return boolean
     */
    public boolean modifyJobForFirstLevelStaging(
            Job job, String submitDir, String slsInputLFN, String slsOutputLFN) {

        return true;
    }

    /**
     * Modifies a compute job for second level staging. Adds the appropriate condor classads. It
     * assumes that all the files are being moved to and from the submit directory directly. Ignores
     * any headnode parameters passed.
     *
     * @param job the job to be modified.
     * @param stagingSiteURLPrefix the url prefix for the server on the staging site
     * @param stagingSiteDirectory the directory on the staging site, where the nput data is read
     *     from and the output data written out.
     * @param workerNodeDirectory the directory in the worker node tmp
     * @return boolean indicating whether job was successfully modified or not.
     */
    public boolean modifyJobForWorkerNodeExecution(
            Job job,
            String stagingSiteURLPrefix,
            String stagingSiteDirectory,
            String workerNodeDirectory) {

        // sanity check on style of the job
        // handle the -w option that asks kickstart to change
        // directory before launching an executable.
        String style = (String) job.vdsNS.get(Pegasus.STYLE_KEY);
        if (style == null
                || !(style.equals(Pegasus.CONDOR_STYLE)
                        || style.equals(Pegasus.GLIDEIN_STYLE)
                        || style.equals(Pegasus.CONDORC_STYLE)
                        || style.equals(Pegasus.CREAMCE_STYLE)
                        || style.equals(Pegasus.PANDA_STYLE))) {

            mLogger.log(
                    "Invalid style " + style + " for the job " + job.getName(),
                    LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }

        // remove any directory. let condor figure it out
        job.condorVariables.removeKey("remote_initialdir");

        // set the initial dir to the headnode directory
        // as this is the directory where we are staging
        // the input and output data
        job.condorVariables.construct("initialdir", stagingSiteDirectory);

        Collection<String> files = new LinkedList();

        // iterate through all the input files
        for (Iterator it = job.getInputFiles().iterator(); it.hasNext(); ) {
            PegasusFile pf = (PegasusFile) it.next();
            String lfn = pf.getLFN();

            // sanity check case
            // sanityCheckForDeepLFN(job.getID(), lfn, "input");

            ReplicaCatalogEntry cacheLocation = null;

            String pfn = null;
            if (pf.doBypassStaging()) {
                // we retrieve the URL from the Planner Cache as a get URL
                // bypassed URL's are stored as GET urls in the cache and
                // associated with the compute site
                // we need a GET URL. we don't know what site is associated with
                // the source URL. Get the first matching one
                // PM-698
                cacheLocation = mPlannerCache.lookup(lfn, FileServerType.OPERATION.get);
            }
            if (cacheLocation == null) {
                // nothing in the cache
                // construct the location with respect to the staging site
                // we add just the lfn as we are setting initialdir
                pfn = lfn;
            } else {
                // construct the URL wrt to the planner cache location
                pfn = cacheLocation.getPFN();
                if (pfn.startsWith(PegasusURL.FILE_URL_SCHEME)) {
                    // we let other url's pass through to ensure
                    pfn = new PegasusURL(pfn).getPath();
                }
            }

            // add an input file for transfer
            files.add(pfn);
        }
        job.condorVariables.addIPFileForTransfer(files);

        files = new LinkedList();
        // iterate and add output files for transfer back
        for (Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ) {
            PegasusFile pf = (PegasusFile) it.next();
            String lfn = pf.getLFN();

            // sanity check case
            // sanityCheckForDeepLFN(job.getID(), lfn, "output");

            // ignore any input files of FileTransfer as they are first level
            // staging put in by Condor Transfer refiner
            if (pf instanceof FileTransfer) {
                continue;
            }

            // add an output file for transfer
            files.add(lfn);

            // check for deep lfn
            if (lfn.contains(File.separator)) {
                // PM-1875 if the output file is a deep LFN then add
                // transfer_output_remaps key, with name as basename
                // and new name as the deep LFN
                job.condorVariables.addOPFileForTransferRemap(new File(lfn).getName(), lfn);
            }
        }
        job.condorVariables.addOPFileForTransfer(files);

        return true;
    }

    /**
     * Returns a textual description of the transfer mode.
     *
     * @return a short textual description
     */
    public String getDescription() {
        return "SLS backend using Condor File Transfers to the worker node";
    }

    /**
     * Creates a file transfer object that results in a file copy in the job working directory to
     * the deep LFN.This results in two copies of the file in the HTCondor assigned job directory
     *
     * @param pf the PegasusFile that needs to be copied
     * @param siteHandle the compute site where job runs
     * @param destDir the destination directory on the worker node
     * @param escapeEnvVariable whether to escape environment variable in the generated source and
     *     destination
     * @return generated FileTransfer
     */
    protected FileTransfer fileTransferForCopyOfInputs(
            PegasusFile pf, String siteHandle, String destDir, boolean escapeEnvVariable) {
        FileTransfer ft = new FileTransfer();

        String lfn = pf.getLFN();
        ft.setLFN(pf.getLFN());
        // ensure that right type gets associated, especially
        // whether a file is a checkpoint file or not
        ft.setType(pf.getType());

        // the source URL is the basename of the file in the directory
        // on the worker node .
        StringBuffer sourceURL = new StringBuffer();
        sourceURL.append(PegasusURL.FILE_URL_SCHEME).append("//");
        if (escapeEnvVariable) {
            // PM-1875 when a job is run through a container, then data stage-in
            // happens inside the container. so we need to ensure
            // the variable does not expanded on the host OS where pegasuslite
            // script runs
            sourceURL.append("\\");
        }
        sourceURL
                .append("$pegasus_lite_work_dir")
                .append(File.separator)
                .append(new File(lfn).getName());
        // In CondorIO case, condor file io has already  gotten the job the compute site
        // before PegasusLitejob starts
        ft.addSource(siteHandle, sourceURL.toString());

        // the destination is the complete LFN in $pegasus_lite_work_dir ($PWD)
        // directory
        StringBuffer destURL = new StringBuffer();
        destURL.append(PegasusURL.FILE_URL_SCHEME)
                .append("//")
                .append(destDir)
                .append(File.separator)
                .append(lfn);
        ft.addDestination(siteHandle, destURL.toString());
        return ft;
    }

    /**
     * Creates a file transfer object that results in a symlink in the job working directory to the
     * source file copied over by the HTCondor file transfer.
     *
     * @param pf the PegasusFile that needs to be copied
     * @param siteHandle the compute site where job runs
     * @param destDir the destination directory on the worker node
     * @return generated FileTransfer
     */
    protected FileTransfer fileTransferForSymlinkOfInputs(
            PegasusFile pf, String siteHandle, String destDir) {
        FileTransfer ft = new FileTransfer();

        String lfn = pf.getLFN();
        ft.setLFN(pf.getLFN());
        // ensure that right type gets associated, especially
        // whether a file is a checkpoint file or not
        ft.setType(pf.getType());

        // for symlinking with deep LFN's we do relative paths in the
        // HTCondor assigned job workdir
        destDir = ".";

        // the source URL is the basename of the file in the directory
        // on the worker node . However, symlinking happens w.r.t to the destination dir
        // so if we need to link f.a -> deep/f.a, the command triggered should
        // be ln -s ../f.a deep/f.a
        StringBuilder sourceURL = new StringBuilder();
        StringBuilder sourceDir = new StringBuilder();
        int index = 0;
        while ((index = lfn.indexOf(File.separatorChar, index)) != -1) {
            sourceDir.append("..").append(File.separator);
            index += 1;
        }
        if (sourceDir.length() == 0) {
            // empty source dir computed. means just a flat lfn
            sourceDir.append(".").append(File.separator);
        }

        sourceURL
                .append(PegasusURL.FILE_URL_SCHEME)
                .append("//")
                .append(sourceDir)
                .append(new File(lfn).getName());

        ft.addSource(siteHandle, sourceURL.toString());

        // the destination is the deep LFN
        StringBuffer destURL = new StringBuffer();
        destURL.append(PegasusURL.SYMLINK_URL_SCHEME)
                .append("//")
                .append(destDir)
                .append(File.separator)
                .append(lfn);
        ft.addDestination(siteHandle, destURL.toString());
        return ft;
    }
}
