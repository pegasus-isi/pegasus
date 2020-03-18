/**
 * Copyright 2007-2017 University Of Southern California
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
package edu.isi.pegasus.planner.code.gridstart.container.impl;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.gridstart.Integrity;
import edu.isi.pegasus.planner.code.gridstart.PegasusLite;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.selector.ReplicaSelector;
import edu.isi.pegasus.planner.transfer.SLS;
import edu.isi.pegasus.planner.transfer.sls.SLSFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** @author Karan Vahi */
public abstract class Abstract implements ContainerShellWrapper {

    public static final String SEPARATOR = "########################";
    public static final char SEPARATOR_CHAR = '#';
    public static final int MESSAGE_STRING_LENGTH = 80;

    public static final String PEGASUS_LITE_MESSAGE_PREFIX = "[Pegasus Lite]";

    public static final String CONTAINER_MESSAGE_PREFIX = "[Container]";

    /** The LogManager object which is used to log all the messages. */
    protected LogManager mLogger;

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The submit directory where the submit files are being generated for the workflow. */
    protected String mSubmitDir;

    /** the planner options. */
    protected PlannerOptions mPOptions;

    /** A map indexed by the execution site and value is the path to chmod on that site. */
    private Map<String, String> mChmodOnExecutionSiteMap;

    /** Handle to the site catalog store. */
    // protected PoolInfoProvider mSiteHandle;
    protected SiteStore mSiteStore;

    /** Handle to Transformation Catalog. */
    private TransformationCatalog mTCHandle;

    /**
     * This member variable if set causes the destination URL for the symlink jobs to have
     * symlink:// url if the pool attributed associated with the pfn is same as a particular jobs
     * execution pool.
     */
    protected boolean mUseSymLinks;

    /** The handle to the SLS implementor */
    protected SLSFactory mSLSFactory;

    /** Whether to do integrity checking or not. */
    protected boolean mDoIntegrityChecking;

    private Integrity mIntegrityHandler;

    /**
     * Appends a fragment to the pegasus lite script that logs a message to stderr
     *
     * @param sb string buffer
     * @param prefix
     * @param message the message
     */
    protected static void appendStderrFragment(StringBuilder sb, String prefix, String message) {
        // prefix + 1 + message
        int len = prefix.length() + 1 + message.length();
        if (len > Abstract.MESSAGE_STRING_LENGTH) {
            throw new RuntimeException(
                    "Message string for ContainerShellWrapper exceedss "
                            + Abstract.MESSAGE_STRING_LENGTH
                            + " characters");
        }

        int pad = (Abstract.MESSAGE_STRING_LENGTH - len) / 2;
        sb.append("echo -e \"\\n");
        for (int i = 0; i <= pad; i++) {
            sb.append(Abstract.SEPARATOR_CHAR);
        }
        sb.append(prefix).append(" ").append(message).append(" ");
        for (int i = 0; i <= pad; i++) {
            sb.append(Abstract.SEPARATOR_CHAR);
        }
        sb.append("\"  1>&2").append("\n");
    }

    public Abstract() {}

    /**
     * Initiailizes the Container shell wrapper
     *
     * @param bag
     * @param dag
     */
    public void initialize(PegasusBag bag, ADag dag) {
        mLogger = bag.getLogger();
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
        mSiteStore = bag.getHandleToSiteStore();
        mTCHandle = bag.getHandleToTransformationCatalog();
        mUseSymLinks = mProps.getUseOfSymbolicLinks();
        mSubmitDir = mPOptions.getSubmitDirectory();
        mChmodOnExecutionSiteMap = new HashMap<String, String>();
        mSLSFactory = new SLSFactory();
        mSLSFactory.initialize(bag);
        mDoIntegrityChecking = mProps.doIntegrityChecking();
        mIntegrityHandler = new Integrity();
        mIntegrityHandler.initialize(bag, dag);
    }

    /**
     * Convers the collection of files into an input format suitable for the transfer executable
     *
     * @param files Collection of <code>FileTransfer</code> objects.
     * @param linkage file type of transfers
     * @return the blurb containing the files in the input format for the transfer executable
     */
    protected StringBuffer convertToTransferInputFormat(
            Collection<FileTransfer> files, PegasusFile.LINKAGE linkage) {
        StringBuffer sb = new StringBuffer();

        sb.append("[\n");

        int num = 1;
        for (FileTransfer ft : files) {

            if (num > 1) {
                sb.append(" ,\n");
            }
            Collection<String> sourceSites = ft.getSourceSites();
            sb.append(" { \"type\": \"transfer\",\n");
            sb.append("   \"linkage\": ").append("\"").append(linkage).append("\"").append(",\n");
            sb.append("   \"lfn\": ").append("\"").append(ft.getLFN()).append("\"").append(",\n");
            sb.append("   \"id\": ").append(num).append(",\n");

            // PM-1321 , PM-1298 commented because transfers are inside
            // the container. We should fail if symlink does not exist
            /*if( !ft.verifySymlinkSource() ){
                sb.append("   \"verify_symlink_source\": false").append(",\n");
            }
            */

            sb.append("   \"src_urls\": [");

            boolean notFirst = false;
            for (String sourceSite : sourceSites) {
                // traverse through all the URL's on that site
                for (ReplicaCatalogEntry url : ft.getSourceURLs(sourceSite)) {
                    if (notFirst) {
                        sb.append(",");
                    }
                    String prio = (String) url.getAttribute(ReplicaSelector.PRIORITY_KEY);

                    sb.append("\n     {");
                    sb.append(" \"site_label\": \"").append(sourceSite).append("\",");
                    sb.append(" \"url\": \"").append(url.getPFN()).append("\",");
                    if (prio != null) {
                        sb.append(" \"priority\": ").append(prio).append(",");
                    }
                    sb.append(" \"checkpoint\": \"").append(ft.isCheckpointFile()).append("\"");
                    sb.append(" }");
                    notFirst = true;
                }
            }

            sb.append("\n   ],\n");
            NameValue nv = ft.getDestURL();
            sb.append("   \"dest_urls\": [\n");
            sb.append("     {");
            sb.append(" \"site_label\": \"").append(nv.getKey()).append("\",");
            sb.append(" \"url\": \"").append(nv.getValue()).append("\"");
            // PM-1300 tag that we are transferring a container
            if (ft.isTransferringContainer()) {
                sb.append(",");
                sb.append(" \"type\": \"").append(ft.typeToString()).append("\"");
            }
            sb.append(" }\n");
            sb.append("   ]");
            sb.append(" }\n"); // end of this transfer

            num++;
        }

        sb.append("]\n");

        return sb;
    }

    /**
     * Complains for a missing head node file server on a site for a job
     *
     * @param jobname the name of the job
     * @param site the site
     */
    void complainForHeadNodeFileServer(String jobname, String site) {
        StringBuffer error = new StringBuffer();
        error.append("[PegasusLite] ");
        if (jobname != null) {
            error.append("For job (").append(jobname).append(").");
        }
        error.append(
                        " File Server not specified for head node scratch shared filesystem for site: ")
                .append(site);
        throw new RuntimeException(error.toString());
    }

    /**
     * Takes the job and generates a pegasus-transfer invocation that handles the input files and
     * executable, but not the container itself
     *
     * @param job
     * @return
     */
    protected StringBuilder inputFilesToPegasusLite(Job job) {
        StringBuilder sb = new StringBuilder();
        boolean isCompute = job.getJobType() == Job.COMPUTE_JOB;
        SiteCatalogEntry stagingSiteEntry = null;

        FileServer stagingSiteServerForRetrieval = null;
        String stagingSiteDirectory = null;
        String workerNodeDir = null;
        if (isCompute) {
            stagingSiteEntry = mSiteStore.lookup(job.getStagingSiteHandle());
            if (stagingSiteEntry == null) {
                this.complainForHeadNodeFileServer(job.getID(), job.getStagingSiteHandle());
            }
            stagingSiteServerForRetrieval =
                    stagingSiteEntry.selectHeadNodeScratchSharedFileServer(
                            FileServer.OPERATION.get);
            if (stagingSiteServerForRetrieval == null) {
                this.complainForHeadNodeFileServer(job.getID(), job.getStagingSiteHandle());
            }

            stagingSiteDirectory = mSiteStore.getInternalWorkDirectory(job, true);
            workerNodeDir = getWorkerNodeDirectory(job);
        }

        SLS sls = mSLSFactory.loadInstance(job);

        if (isCompute
                && // PM-971 for non compute jobs we don't do any sls transfers
                sls.needsSLSInputTransfers(job)) {
            // generate the sls file with the mappings in the submit exectionSiteDirectory
            Collection<FileTransfer> files =
                    sls.determineSLSInputTransfers(
                            job,
                            sls.getSLSInputLFN(job),
                            stagingSiteServerForRetrieval,
                            stagingSiteDirectory,
                            workerNodeDir);

            // PM-779 split the checkpoint files from the input files
            // as we want to stage them separately
            Collection<FileTransfer> inputFiles = new LinkedList();
            Collection<FileTransfer> containerFile = new LinkedList();
            Collection<FileTransfer> chkpointFiles = new LinkedList();
            for (FileTransfer ft : files) {
                if (ft.isCheckpointFile()) {
                    chkpointFiles.add(ft);
                } else if (ft.isContainerFile()) {
                    containerFile.add(ft);
                } else {
                    inputFiles.add(ft);
                }
            }

            // stage the input files first
            if (!inputFiles.isEmpty()) {
                appendStderrFragment(sb, "", "Staging in input data and executables");
                sb.append("# stage in data and executables").append('\n');
                sb.append(sls.invocationString(job, null));
                if (mUseSymLinks) {
                    // PM-1135 allow the transfer executable to symlink input file urls
                    // PM-1197 we have to disable symlink if a job is set to
                    // be launched via a container
                    sb.append(" --symlink ");
                }
                sb.append(" 1>&2").append(" << 'eof'").append('\n');
                sb.append(convertToTransferInputFormat(inputFiles, PegasusFile.LINKAGE.input));
                sb.append("eof").append('\n');
                sb.append('\n');
            }

            // PM-779 checkpoint files need to be setup to never fail
            String checkPointFragment =
                    checkpointFilesToPegasusLite(
                            job, sls, chkpointFiles, PegasusFile.LINKAGE.input);
            if (!checkPointFragment.isEmpty()) {
                appendStderrFragment(sb, "", "Staging in checkpoint files");
                sb.append("# stage in checkpoint files ").append('\n');
                sb.append(checkPointFragment);
            }

            // associate any credentials if required with the job
            associateCredentials(job, files);
        }

        if (job.userExecutablesStagedForJob()) {
            appendStderrFragment(sb, "", "Setting the xbit for executables staged");
            sb.append("# set the xbit for any executables staged").append('\n');
            for (Iterator it = job.getInputFiles().iterator(); it.hasNext(); ) {
                PegasusFile pf = (PegasusFile) it.next();
                if (pf.getType() == PegasusFile.EXECUTABLE_FILE) {
                    sb.append("if [ ! -x " + pf.getLFN() + " ]; then\n");
                    sb.append("    ");
                    sb.append(getPathToChmodExecutable(job.getSiteHandle()));
                    sb.append(" +x ");
                    sb.append(pf.getLFN()).append("\n");
                    sb.append("fi\n");
                }
            }
            sb.append('\n');
        }
            
        return sb;
    }

    /**
     * Takes the job and generates a pegasus-transfer invocation that handles the output files and
     * executable, but not the container itself
     *
     * @param job
     * @return
     */
    protected StringBuilder outputFilesToPegasusLite(Job job) {
        StringBuilder sb = new StringBuilder();
        boolean isCompute = job.getJobType() == Job.COMPUTE_JOB;
        SiteCatalogEntry stagingSiteEntry = null;

        FileServer stagingSiteServerForRetrieval = null;
        String stagingSiteDirectory = null;
        String workerNodeDir = null;
        if (isCompute) {
            stagingSiteEntry = mSiteStore.lookup(job.getStagingSiteHandle());
            if (stagingSiteEntry == null) {
                this.complainForHeadNodeFileServer(job.getID(), job.getStagingSiteHandle());
            }
            stagingSiteServerForRetrieval =
                    stagingSiteEntry.selectHeadNodeScratchSharedFileServer(
                            FileServer.OPERATION.get);
            if (stagingSiteServerForRetrieval == null) {
                this.complainForHeadNodeFileServer(job.getID(), job.getStagingSiteHandle());
            }

            stagingSiteDirectory = mSiteStore.getInternalWorkDirectory(job, true);
            workerNodeDir = getWorkerNodeDirectory(job);
        }

        SLS sls = mSLSFactory.loadInstance(job);

        if (isCompute
                && // PM-971 for non compute jobs we don't do any sls transfers
                sls.needsSLSOutputTransfers(job)) {

            FileServer stagingSiteServerForStore =
                    stagingSiteEntry.selectHeadNodeScratchSharedFileServer(
                            FileServer.OPERATION.put);
            if (stagingSiteServerForStore == null) {
                this.complainForHeadNodeFileServer(job.getID(), job.getStagingSiteHandle());
            }

            String stagingSiteDirectoryForStore = mSiteStore.getInternalWorkDirectory(job, true);

            // construct the postjob that transfers the output files
            // back to head node exectionSiteDirectory
            // to fix later. right now post constituentJob only created is pre constituentJob
            // created
            Collection<FileTransfer> files =
                    sls.determineSLSOutputTransfers(
                            job,
                            sls.getSLSOutputLFN(job),
                            stagingSiteServerForStore,
                            stagingSiteDirectoryForStore,
                            workerNodeDir);

            // PM-779 split the checkpoint files from the output files
            // as we want to stage them separately
            Collection<FileTransfer> outputFiles = new LinkedList();
            Collection<FileTransfer> chkpointFiles = new LinkedList();
            for (FileTransfer ft : files) {
                if (ft.isCheckpointFile()) {
                    chkpointFiles.add(ft);
                } else {
                    outputFiles.add(ft);
                }
            }

            // PM-779 checkpoint files need to be setup to never fail
            String checkPointFragment =
                    checkpointFilesToPegasusLite(
                            job, sls, chkpointFiles, PegasusFile.LINKAGE.output);
            if (!checkPointFragment.isEmpty()) {
                appendStderrFragment(sb, "", "Staging out checkpoint files");
                sb.append("# stage out checkpoint files ").append('\n');
                sb.append(checkPointFragment);
            }

            if (!outputFiles.isEmpty()) {
                // generate the stage out fragment for staging out outputs
                String postJob = sls.invocationString(job, null);
                appendStderrFragment(sb, "", "Staging out output files");
                sb.append("# stage out").append('\n');
                sb.append(postJob);

                sb.append(" 1>&2").append(" << 'eof'").append('\n');
                sb.append(convertToTransferInputFormat(outputFiles, PegasusFile.LINKAGE.output));
                sb.append("eof").append('\n');
                sb.append('\n');
            }

            // associate any credentials if required with the job
            associateCredentials(job, files);
        }

        return sb;
    }

    /**
     * Enables a job for integrity checking
     *
     * @param job
     * @param prefix
     * @return
     */
    protected StringBuilder enableForIntegrity(Job job, String prefix) {
        StringBuilder sb = new StringBuilder();
        boolean isCompute = job.getJobType() == Job.COMPUTE_JOB;
        // PM-1190 we do integrity checks only for compute jobs
        if (mDoIntegrityChecking && isCompute) {
            // we cannot enable integrity checking for DAX or dag jobs
            // as the prescript is not run as a full condor job
            if (!(job instanceof DAXJob || job instanceof DAGJob)) {
                appendStderrFragment(sb, prefix, "Checking file integrity for input files");
                sb.append("# do file integrity checks").append('\n');
                String filesToVerify =
                        mIntegrityHandler.addIntegrityCheckInvocation(sb, job.getInputFiles());
                if (filesToVerify.length() > 0) {
                    sb.append(" 1>&2").append(" << 'eof'").append('\n');
                    sb.append(filesToVerify);
                    sb.append('\n');
                    sb.append("eof").append('\n');
                }

                // check if planner knows of any checksums from the replica catalog
                // and generate an input meta file!
                File metaFile =
                        mIntegrityHandler.generateChecksumMetadataFile(
                                job.getFileFullPath(mSubmitDir, ".in.meta"), job.getInputFiles());

                // modify job for transferring the .meta files
                if (!mIntegrityHandler.modifyJobForIntegrityChecks(
                        job, metaFile, this.mSubmitDir)) {
                    throw new RuntimeException("Unable to modify job for integrity checks");
                }
                sb.append("\n");
            }
        }

        return sb;
    }

    /**
     * Convenience method to slurp in contents of a file into memory.
     *
     * @param directory the directory where the file resides
     * @param file the file to be slurped in.
     * @return StringBuffer containing the contents
     */
    protected StringBuffer slurpInFile(String directory, String file) throws IOException {
        StringBuffer result = new StringBuffer();
        // sanity check
        if (file == null) {
            return result;
        }

        BufferedReader in = new BufferedReader(new FileReader(new File(directory, file)));

        String line = null;

        while ((line = in.readLine()) != null) {
            // System.out.println( line );
            result.append(line).append('\n');
        }

        in.close();

        return result;
    }

    /**
     * Takes in the checkpoint files, and generates a pegasus-transfer invocation that should
     * succeed in case of errors
     *
     * @param job the job being wrapped with PegasusLite
     * @param sls associated SLS implementation
     * @param files the checkpoint files
     * @param fileType type of files to be transferred
     * @return string representation of the PegasusLite fragment
     */
    private String checkpointFilesToPegasusLite(
            Job job, SLS sls, Collection<FileTransfer> files, PegasusFile.LINKAGE fileType) {
        StringBuilder sb = new StringBuilder();
        if (!files.isEmpty()) {
            sb.append("set +e ").append("\n");
            sb.append(sls.invocationString(job, null));
            sb.append(" 1>&2").append(" << 'eof'").append('\n');
            sb.append(convertToTransferInputFormat(files, fileType));
            sb.append("eof").append('\n');
            sb.append("ec=$?").append('\n');
            sb.append("set -e").append('\n');
            sb.append("if [ $ec -ne 0 ]; then").append('\n');
            sb.append(
                            "    echo \" Ignoring failure while transferring chkpoint files. Exicode was $ec\" 1>&2")
                    .append('\n');
            sb.append("fi").append('\n');
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Returns the path to the chmod executable for a particular execution site by looking up the
     * transformation executable.
     *
     * @param site the execution site.
     * @return the path to chmod executable
     */
    protected String getPathToChmodExecutable(String site) {
        String path;

        // check if the internal map has anything
        path = mChmodOnExecutionSiteMap.get(site);

        if (path != null) {
            // return the cached path
            return path;
        }

        List entries;
        try {
            // try to look up the transformation catalog for the path
            entries =
                    mTCHandle.lookup(
                            PegasusLite.XBIT_TRANSFORMATION_NS,
                            PegasusLite.XBIT_TRANSFORMATION,
                            PegasusLite.XBIT_TRANSFORMATION_VERSION,
                            site,
                            TCType.INSTALLED);
        } catch (Exception e) {
            // non sensical catching
            mLogger.log(
                    "Unable to retrieve entries from TC " + e.getMessage(),
                    LogManager.ERROR_MESSAGE_LEVEL);
            return null;
        }

        TransformationCatalogEntry entry =
                (entries == null)
                        ? null
                        : // try using a default one
                        (TransformationCatalogEntry) entries.get(0);

        if (entry == null) {
            // construct the path the default path.
            // construct the path to it
            StringBuffer sb = new StringBuffer();
            sb.append(File.separator)
                    .append("bin")
                    .append(File.separator)
                    .append(PegasusLite.XBIT_EXECUTABLE_BASENAME);
            path = sb.toString();
        } else {
            path = entry.getPhysicalTransformation();
        }

        mChmodOnExecutionSiteMap.put(site, path);

        return path;
    }

    /**
     * Returns the directory in which the job executes on the worker node.
     *
     * @param job
     * @return the full path to the directory where the job executes
     */
    public String getWorkerNodeDirectory(Job job) {
        Container c = job.getContainer();
        if (c == null) {
            return "$PWD";
        } else {
            if (c.getType().equals(Container.TYPE.docker)) {
                return Docker.CONTAINER_WORKING_DIRECTORY;
            } else if (c.getType().equals(Container.TYPE.singularity)) {
                return Singularity.CONTAINER_WORKING_DIRECTORY;
            }
            if (c.getType().equals(Container.TYPE.shifter)) {
                return Shifter.CONTAINER_WORKING_DIRECTORY;
            } else {
                Container.TYPE type = c.getType();
                throw new RuntimeException("Unsupported Container of type " + type);
            }
        }
    }

    /**
     * Associates credentials with the job corresponding to the files that are being transferred.
     *
     * @param job the job for which credentials need to be added.
     * @param files the files that are being transferred.
     */
    private void associateCredentials(Job job, Collection<FileTransfer> files) {
        for (FileTransfer ft : files) {
            NameValue source = ft.getSourceURL();
            job.addCredentialType(source.getKey(), source.getValue());
            NameValue dest = ft.getDestURL();
            job.addCredentialType(dest.getKey(), dest.getValue());
        }
    }
}
