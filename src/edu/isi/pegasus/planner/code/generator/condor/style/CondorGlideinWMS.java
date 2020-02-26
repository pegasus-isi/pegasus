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
package edu.isi.pegasus.planner.code.generator.condor.style;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;

/**
 * Jobs targeting glidinWMS pools. Basically standard Condor jobs with special requirements and
 * ranks
 *
 * @author Mats Rynge
 * @version $Revision: 2090 $
 */
public class CondorGlideinWMS extends Condor {

    /** The name of the style being implemented. */
    public static final String STYLE_NAME = "CondorGlideinWMS";

    /** The default constructor. */
    public CondorGlideinWMS() {
        super();
    }

    /**
     * @param job the job on which the style needs to be applied.
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(Job job) throws CondorStyleException {

        // default is vanilla universe for glideinwms jobs
        String universe =
                job.condorVariables.containsKey(Condor.UNIVERSE_KEY)
                        ? (String) job.condorVariables.get(Condor.UNIVERSE_KEY)
                        : Condor.VANILLA_UNIVERSE;
        job.condorVariables.construct(Condor.UNIVERSE_KEY, universe);

        // glideinWMS jobs are basic Condor jobs
        super.apply(job);

        universe = (String) job.condorVariables.get(Condor.UNIVERSE_KEY);

        if (universe.equalsIgnoreCase(Condor.VANILLA_UNIVERSE)
                || universe.equalsIgnoreCase(Condor.STANDARD_UNIVERSE)
                || universe.equalsIgnoreCase(Condor.PARALLEL_UNIVERSE)) {

            job.condorVariables.construct("should_transfer_files", "YES");
            String wtto = (String) job.condorVariables.get(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY);
            if (wtto == null) {
                // default value
                job.condorVariables.construct(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY, "ON_EXIT");
            } else {
                // PM-1350 prefer the value specified by the user
                job.condorVariables.construct(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY, wtto);
            }

            // job requirements - steer the jobs to the glideins at the right site
            String req =
                    "(IS_MONITOR_VM == False)"
                            + " && (Arch != \"\") && (OpSys != \"\") && (Disk != -42)"
                            + " && (Memory > 1) && (FileSystemDomain != \"\")";
            job.condorVariables.construct("requirements", req);

            // rank - steer jobs to the newest available glideins - this is so we can
            // identify and remove unused glideins
            job.condorVariables.construct("rank", "DaemonStartTime");
        }
    }
}
