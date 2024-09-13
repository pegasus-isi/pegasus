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
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerCache;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Condor;
import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * An implementation that leverages
 *
 * @author Karan Vahi
 */
public class CondorSingularity extends AbstractContainer {

    /** The directory in the container to be used as working directory */
    public static final String CONTAINER_WORKING_DIRECTORY = "/srv";

    private static String WORKER_PACKAGE_SETUP_SNIPPET = null;

    private PlannerCache mPlannerCache;

    /**
     * Initiailizes the Container shell wrapper
     *
     * @param bag
     * @param dag
     */
    @Override
    public void initialize(PegasusBag bag, ADag dag) {
        super.initialize(bag, dag);
        // PM-1950 force to make sure that transfers are set to
        // happen outside the container from Pegasus perspective
        // Condor will launch the PegasusLite job inside the container only
        this.mTransfersOnHostOS = true;
        mPlannerCache = bag.getHandleToPlannerCache();
    }

    /**
     * Returns the snippet to wrap a single job execution In this implementation we don't wrap with
     * any container, just plain shell invocation is returned.
     *
     * @param job
     * @return
     */
    @Override
    public String wrap(Job job) {
        StringBuilder sb = new StringBuilder();

        sb.append("set -e").append("\n");

        // PM-1818 for the debug mode set -x
        if (this.mPegasusMode == PegasusProperties.PEGASUS_MODE.debug) {
            sb.append("set -x").append('\n');
        }

        // set environment variables required for the job to run
        // inside the container
        sb.append(this.constructJobEnvironmentInContainer(job));

        // Step 1: setup transfers for inputs on the HOST OS
        // if so configured
        if (this.mTransfersOnHostOS) {
            sb.append("pegasus_lite_section_start stage_in").append('\n');
            sb.append(super.inputFilesToPegasusLite(job));
            sb.append(super.enableForIntegrity(job, ""));
            sb.append("pegasus_lite_section_end stage_in").append('\n');
        }

        // Step 2: within the pegasus lite script create a wrapper
        // to launch job in the container. wrapper is required to
        // deploy pegasus worker package in the container and launch the user job
        // NOTHING TO DO, as job is launched  by HTCondor inside the container

        // Step 3
        // NOTHING TO DO, as job is launched  by HTCondor inside the container

        // Step 4
        sb.append(containerRun(job));
        sb.append("\n");
        sb.append("job_ec=$(($job_ec + $?))").append("\n").append("\n");

        // Step 5
        // remove the docker container
        // NOTHING TO DO, as job is launched  by HTCondor inside the container
        // and HTCondor removes it

        // Step 6: setup transfers for outputs on the HOST OS
        // if so configured
        if (this.mTransfersOnHostOS) {
            sb.append("pegasus_lite_section_start stage_out").append('\n');
            sb.append(super.outputFilesToPegasusLite(job));
            sb.append("pegasus_lite_section_end stage_out").append('\n');
        }

        // now add some extra stuff
        job.condorVariables.construct("universe", Condor.CONTAINER_UNIVERSE);
        job.condorVariables.construct("container_image", determineContainerImageURL(job));

        return sb.toString();
    }

    /**
     * Creates the snippet for container initialization, in the script that is launched on the host
     * OS, by PegasusLite
     *
     * @param job
     * @return the bash snippet
     */
    @Override
    public StringBuilder containerInit(Job job) {
        StringBuilder sb = new StringBuilder();
        return sb;
    }

    /**
     * Constructs the snippet for executing the container, in the PegasusLite script launched on the
     * HOST OS
     *
     * @param job
     * @return the bash snippet
     */
    @Override
    public StringBuilder containerRun(Job job) {
        StringBuilder sb = new StringBuilder();
        /*
        Container c = job.getContainer();
        // PM-1888 the pegasus-lite-common.sh determines what
        // is the singularity executable to use. We prefer apptainer if
        // it exists
        sb.append(this.wrapContainerInvocationWithLauncher(job, "$singularity_exec"));
        sb.append(" ").append("exec").append(" ");

        // do not mount home - this might not exists when running under for example the nobody user
        sb.append("--no-home").append(" ");

        // PM-1621 add --nv option if user has gpus requested with the job
        if (job.vdsNS.containsKey(Pegasus.GPUS_KEY)
                || job.condorVariables.containsKey(Condor.REQUEST_GPUS_KEY)) {
            sb.append("--nv").append(" ");
        }

        // exec --bind $PWD:/srv
        sb.append("--bind $PWD:").append(CONTAINER_WORKING_DIRECTORY).append(" ");

        // PM-1298 mount any host directories if specified
        for (Container.MountPoint mp : c.getMountPoints()) {
            sb.append("--bind ").append(mp).append(" ");
        }

        // PM-1626 incorporate any user specified extra arguments
        String extraArgs = job.vdsNS.getStringValue(Pegasus.CONTAINER_ARGUMENTS_KEY);
        if (extraArgs != null) {
            sb.append(extraArgs);
            sb.append(" ");
        }

        // we are running directly against image file. no loading
        sb.append(c.getLFN()).append(" ");
        */

        // the script that sets up pegasus worker package and execute
        // user application
        // sb.append("/srv/").append(this.getJobLaunchScriptName(job));

        if (job instanceof AggregatedJob) {
            try {
                // for clustered jobs we embed the contents of the input
                // file in the shell wrapper itself
                sb.append(job.getRemoteExecutable()).append(" ").append(job.getArguments());
                sb.append(" << CLUSTER").append('\n');

                // PM-833 figure out the job submit directory
                String jobSubmitDirectory =
                        new File(job.getFileFullPath(mSubmitDir, ".in")).getParent();

                sb.append(slurpInFile(jobSubmitDirectory, job.getStdIn()));
                sb.append("CLUSTER").append('\n');
            } catch (IOException ioe) {
                throw new RuntimeException(
                        "[Pegasus-Lite] Error while "
                                + this.describe()
                                + " wrapping job "
                                + job.getID(),
                        ioe);
            }
        } else {
            sb.append(job.getRemoteExecutable())
                    .append(" ")
                    .append(job.getArguments())
                    .append("\n");
        }

        return sb;
    }

    /**
     * Constructs the snippet for removing the container, in the PegasusLite script launched on the
     * HOST OS
     *
     * @param job
     * @return the bash snippet
     */
    @Override
    public StringBuilder containerRemove(Job job) {
        StringBuilder sb = new StringBuilder();

        return sb;
    }

    /**
     * Return the description
     *
     * @return
     */
    @Override
    public String describe() {
        return "Singularity";
    }

    /**
     * Construct the snippet that generates the shell script responsible for setting up the worker
     * package in the container.
     *
     * @return
     */
    @Override
    protected String constructContainerWorkerPackagePreamble() {
        // quasi singleton?
        if (WORKER_PACKAGE_SETUP_SNIPPET == null) {
            WORKER_PACKAGE_SETUP_SNIPPET = super.constructContainerWorkerPackagePreamble();
        }
        return WORKER_PACKAGE_SETUP_SNIPPET;
    }

    /**
     * Returns the bash snippet containing the environment variables to be set for a job inside the
     * container.This snippet is embedded in the <job>-cont.sh file that is written out in
     * PegasusLite on the worker dir, and is launched inside the container.
     *
     * @param job
     * @return the bash snippet
     */
    @Override
    public String constructJobEnvironmentInContainer(Job job) {
        StringBuilder sb = new StringBuilder();
        /* dont need it when condor is handling the container
        sb.append("# tmp dirs are handled by Singularity - don't use the ones from the host\n");
        sb.append("unset TEMP\n");
        sb.append("unset TMP\n");
        sb.append("unset TMPDIR\n");
        sb.append("\n");
        */

        // set the job environment variables explicitly in the -cont.sh file
        sb.append("# setting environment variables for job").append('\n');
        // PM-1950 we don't set any home in container universe
        // let HTCondor set to whatever it does

        sb.append("\n");
        sb.append(this.constructJobEnvironmentFromContainer(job.getContainer()));

        return sb.toString();
    }

    /**
     * Return the directory inside the container where the user job is launched from
     *
     * @return String
     */
    @Override
    public String getContainerWorkingDirectory() {
        return CondorSingularity.CONTAINER_WORKING_DIRECTORY;
    }

    /**
     * Determine the the container image url to put into the condor class ad container_image, taking
     * into account whether the container has bypass turned ON or not.
     *
     * @param job
     * @return the image url
     */
    protected String determineContainerImageURL(Job job) {
        Container c = job.getContainer();
        if (c == null) {
            throw new RuntimeException("Container is null for job-" + job.getID());
        }
        StringBuilder url = new StringBuilder();
        String containerLFN = c.getLFN();

        // PM-1975 traverse through job input files to get PegasusFile matching container lfn
        // to get true determination for whether the container has to be bypassed or not
        // since container bypass can be set via properties or on the container object
        PegasusFile containerPF = null;
        for (PegasusFile pf : job.getInputFiles()) {
            if (pf.getLFN().equals(containerLFN)) {
                containerPF = pf;
                break;
            }
        }

        if (containerPF == null) {
            throw new RuntimeException(
                    "Unable to find PegasusFile in the job input files for job-" + job.getID());
        }

        if (containerPF.doBypassStaging()) {
            Collection<ReplicaCatalogEntry> cacheLocations =
                    mPlannerCache.lookupAllEntries(
                            containerLFN, job.getSiteHandle(), FileServerType.OPERATION.get);
            if (cacheLocations.isEmpty()) {
                mLogger.log(
                        constructMessage(
                                job,
                                containerLFN,
                                "Unable to find location of lfn in planner(get) cache with input staging bypassed"),
                        LogManager.WARNING_MESSAGE_LEVEL);
            }
            // construct the URL wrt to the planner cache location
            // PM-1975 go through all the candidates returned from the planner cache
            for (ReplicaCatalogEntry cacheLocation : cacheLocations) {
                url.append(cacheLocation.getPFN());
                break;
            }
        } else {
            url.append(mPOptions.getSubmitDirectory()).append(File.separator).append(containerLFN);
        }
        return url.toString();
    }

    /**
     * Helper message to construct a log message
     *
     * @param job
     * @param lfn
     * @param message
     * @return
     */
    private String constructMessage(Job job, String lfn, String message) {
        return this.constructMessage(job.getID(), lfn, message);
    }

    /**
     * Helper message to construct a log message
     *
     * @param jobID
     * @param lfn
     * @param message
     * @return
     */
    private String constructMessage(String jobID, String lfn, String message) {
        StringBuilder sb = new StringBuilder();
        sb.append("For").append(" ").append("(").append(jobID);
        if (lfn != null) {
            sb.append(",").append(lfn).append(")");
        }
        sb.append(" ").append(message);
        return sb.toString();
    }
}
