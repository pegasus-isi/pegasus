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

import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.File;
import java.io.IOException;

/**
 * An interface to determine how a job gets wrapped to be launched on various containers, as a
 * shell-script snippet that can be embedded in PegasusLite
 *
 * @author vahi
 */
public class Singularity extends AbstractContainer {

    /** The directory in the container to be used as working directory */
    public static final String CONTAINER_WORKING_DIRECTORY = "/srv";

    private static String WORKER_PACKAGE_SETUP_SNIPPET = null;

    /**
     * Initiailizes the Container shell wrapper
     *
     * @param bag
     * @param dag
     */
    public void initialize(PegasusBag bag, ADag dag) {
        super.initialize(bag, dag);
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
        Container c = job.getContainer();
        sb.append("singularity_init").append(" ").append(c.getLFN());
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
        Container c = job.getContainer();
        // PM-1888 the pegasus-lite-common.sh determines what
        // is the singularity executable to use. We prefer apptainer if
        // it exists
        sb.append("$singularity_exec exec").append(" ");

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

        // the script that sets up pegasus worker package and execute
        // user application
        sb.append("/srv/").append(this.getJobLaunchScriptName(job)).append(" ");

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
     * Return the container package snippet. Construct the snippet that generates the shell script
     * responsible for setting up the worker package in the container and launch the job in the
     * container.
     *
     * @param job the job
     * @param scriptName basename of the script
     * @return
     */
    @Override
    public String constructJobLaunchScriptInContainer(Job job, String scriptName) {
        if (WORKER_PACKAGE_SETUP_SNIPPET == null) {
            WORKER_PACKAGE_SETUP_SNIPPET = this.constructContainerWorkerPackagePreamble();
        }
        StringBuilder sb = new StringBuilder();
        Container c = job.getContainer();
        sb.append("\n");
        appendStderrFragment(
                sb,
                Abstract.PEGASUS_LITE_MESSAGE_PREFIX,
                "Writing out script to launch user task in container");
        sb.append("\n");
        sb.append("cat <<EOF > ").append(scriptName).append("\n");
        // basic shell as some containers only has dash and not bash
        sb.append("#!/bin/sh").append("\n");
        appendStderrFragment(
                sb, Abstract.CONTAINER_MESSAGE_PREFIX, "Now in pegasus lite container script");
        sb.append("set -e").append("\n");
        sb.append("\n");

        // set environment variables required for the job to run
        // inside the container
        sb.append(this.constructJobEnvironmentInContainer(job));

        // update and include runtime environment variables such as credentials
        sb.append("EOF\n");
        sb.append("container_env ")
                .append(Singularity.CONTAINER_WORKING_DIRECTORY)
                .append(" >> ")
                .append(scriptName)
                .append("\n");

        sb.append("cat <<EOF2 >> ").append(scriptName).append("\n");

        // PM-1214 worker package setup in container should happen after
        // the environment variables have been set.
        sb.append(WORKER_PACKAGE_SETUP_SNIPPET);

        sb.append(super.inputFilesToPegasusLite(job));

        // PM-1305 the integrity check should happen in the container
        sb.append(super.enableForIntegrity(job, Abstract.CONTAINER_MESSAGE_PREFIX));

        sb.append("set +e").append('\n'); // PM-701
        sb.append("job_ec=0").append("\n");

        appendStderrFragment(sb, Abstract.CONTAINER_MESSAGE_PREFIX, "Launching user task");
        sb.append("\n");
        // sb.append( "\\$kickstart \"\\${original_args[@]}\" ").append( "\n" );

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
                        "[Pegasus-Lite] Error while Singularity wrapping job " + job.getID(), ioe);
            }
        } else {
            sb.append(job.getRemoteExecutable())
                    .append(" ")
                    .append(job.getArguments())
                    .append("\n");
        }

        sb.append("set -e").append('\n'); // PM-701
        sb.append(super.outputFilesToPegasusLite(job));

        appendStderrFragment(
                sb, Abstract.CONTAINER_MESSAGE_PREFIX, "Exiting pegasus lite container script");
        sb.append("EOF2").append("\n");
        sb.append("\n");
        sb.append("\n");

        return sb.toString();
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
        return Singularity.CONTAINER_WORKING_DIRECTORY;
    }
}
