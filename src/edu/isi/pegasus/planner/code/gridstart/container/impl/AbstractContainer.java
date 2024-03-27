/**
 * Copyright 2007-2024 University Of Southern California
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
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * An abstract base class for all the Container implementations
 * 
 * @author Karan Vahi
 */
public abstract class AbstractContainer extends Abstract{
   
    /**
     * Construct the snippet that generates the shell script responsible for setting up the worker
     * package in the container.
     *
     * @return
     */
    protected String constructContainerWorkerPackagePreamble() {
        StringBuilder sb = new StringBuilder();

        sb.append("pegasus_lite_version_major=$pegasus_lite_version_major").append("\n");
        sb.append("pegasus_lite_version_minor=$pegasus_lite_version_minor").append("\n");
        sb.append("pegasus_lite_version_patch=$pegasus_lite_version_patch").append("\n");

        sb.append("pegasus_lite_enforce_strict_wp_check=$pegasus_lite_enforce_strict_wp_check")
                .append("\n");
        sb.append(
                        "pegasus_lite_version_allow_wp_auto_download=$pegasus_lite_version_allow_wp_auto_download")
                .append("\n");
        sb.append("pegasus_lite_inside_container=true").append("\n");

        // PM-1875 we need to export the pegasus_lite_work_dir variable to
        // ensure pegasus-transfer picks from the environment
        sb.append("export pegasus_lite_work_dir=")
                .append(getContainerWorkingDirectory())
                .append("\n")
                .append("\n");

        sb.append("cd ").append(getContainerWorkingDirectory()).append("\n");

        sb.append(". ./pegasus-lite-common.sh").append("\n");
        sb.append("pegasus_lite_init").append("\n");

        sb.append("\n");
        appendStderrFragment(
                sb,
                Abstract.CONTAINER_MESSAGE_PREFIX,
                "Figuring out Pegasus worker package to use");
        sb.append("# figure out the worker package to use").append("\n");

        sb.append("pegasus_lite_worker_package").append("\n");

        sb.append("printf \"PATH in container is set to is set to \\$PATH\\n\"")
                .append("  1>&2")
                .append("\n");
        sb.append("\n");

        return sb.toString();
    }
    
    /**
     * Return the directory inside the container where the user job is launched 
     * from 
     * 
     * @return String
     */
    public abstract String getContainerWorkingDirectory();
    
}
