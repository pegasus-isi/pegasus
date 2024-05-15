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

import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.File;

/**
 * An interface to determine how a job gets wrapped to be launched on various containers, as a
 * shell-script snippet that can be embedded in PegasusLite
 *
 * @author vahi
 */
public class Shifter extends AbstractContainer {

    /** The directory in the container to be used as working directory */
    public static final String CONTAINER_WORKING_DIRECTORY = "/scratch";

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
        sb.append("shifter_init").append(" ").append(c.getLFN());
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

        // assume shifter is available in path
        sb.append(this.wrapContainerInvocationWithLauncher(job, "shifter"));
        sb.append(" ").append("--image").append(" ").append(c.getLFN()).append(" ");

        // PM-1298 mount any host directories if specified
        sb.append("--volume ");
        for (Container.MountPoint mp : c.getMountPoints()) {
            sb.append(mp).append(";");
        }
        // directory where job is run is mounted as scratch
        sb.append("$PWD:").append(CONTAINER_WORKING_DIRECTORY).append(" ");

        sb.append("--workdir=").append(CONTAINER_WORKING_DIRECTORY).append(" ");

        // PM-1626 incorporate any user specified extra arguments
        String extraArgs = job.vdsNS.getStringValue(Pegasus.CONTAINER_ARGUMENTS_KEY);
        if (extraArgs != null) {
            sb.append(extraArgs);
            sb.append(" ");
        }

        // the script that sets up pegasus worker package and execute
        // user application
        sb.append("./").append(this.getJobLaunchScriptName(job)).append(" ");

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
    public String describe() {
        return "Shifter@NERSC";
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

        // set the job environment variables explicitly in the -cont.sh file
        sb.append("# setting environment variables for job").append('\n');
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
        return Shifter.CONTAINER_WORKING_DIRECTORY;
    }

    /**
     * Computes the image URL to use for passing to the shifter command
     *
     * @param c
     * @return
     */
    private String computeShifterImageName(Container c) {
        StringBuilder sb = new StringBuilder();
        PegasusURL url = c.getImageURL();
        String protocol = url.getProtocol();
        if (protocol != null && !protocol.equalsIgnoreCase("shifter")) {
            sb.append(protocol).append(":");
        }
        String path = url.getPath();
        if (path.startsWith(File.separator)) {
            sb.append(path.substring(1));
        } else {
            sb.append(path);
        }
        return sb.toString();
    }
}
