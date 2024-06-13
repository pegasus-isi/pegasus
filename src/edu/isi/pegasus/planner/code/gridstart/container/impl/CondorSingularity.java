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

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * An implementation that leverages
 *
 * @author Karan Vahi
 */
public class CondorSingularity extends AbstractContainer {

    /** The directory in the container to be used as working directory */
    public static final String CONTAINER_WORKING_DIRECTORY = "/srv";

    private static String WORKER_PACKAGE_SETUP_SNIPPET = null;

    /**
     * Initiailizes the Container shell wrapper
     *
     * @param bag
     * @param dag
     */
    @Override
    public void initialize(PegasusBag bag, ADag dag) {
        super.initialize(bag, dag);
        // force to make sure that transfers are set to
        // happen inside the container
        this.mTransfersOnHostOS = false;
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
        String wrapped = super.wrap(job);
        // now add some extra stuff
        job.condorVariables.construct("universe", "container");
        job.condorVariables.construct("container_image", job.getContainer().getLFN());
        return wrapped;
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
        sb.append("/srv/").append(this.getJobLaunchScriptName(job));

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
        sb.append("# tmp dirs are handled by Singularity - don't use the ones from the host\n");
        sb.append("unset TEMP\n");
        sb.append("unset TMP\n");
        sb.append("unset TMPDIR\n");
        sb.append("\n");

        // set the job environment variables explicitly in the -cont.sh file
        sb.append("# setting environment variables for job").append('\n');
        sb.append("HOME=/srv").append('\n');
        sb.append("export HOME").append('\n');

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
}
