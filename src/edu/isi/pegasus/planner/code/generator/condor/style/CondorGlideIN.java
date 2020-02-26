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
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;

/**
 * Enables a job to be submitted to nodes that are logically part of the local pool, but physically
 * are not.
 *
 * <p>This style is applied for jobs to be run - on the nodes that have been glided into the local
 * pool - on the nodes that have been flocked to the local pool (NOT TESTED)
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CondorGlideIN extends Abstract {

    /** The name of the style being implemented. */
    public static final String STYLE_NAME = "CondorGlideIN";

    /** The default constructor. */
    public CondorGlideIN() {
        super();
    }

    /**
     * Applies the style to the job to be run in a condor glide in environment. condor style to the
     * job. Changes the job so that it results in generation of a submit file that can be directly
     * submitted to the underlying condor scheduler on the submit host, without going through
     * CondorG and the jobs run only on the nodes that have been glided in from a particular remote
     * pool. Please note that GlideIn only works if all the application jobs are being run via
     * kickstart, as it relies heavily on the ability of the launcher to change the directory before
     * running the application job on the remote end.
     *
     * <p>This applies to the case of - nodes glided in to a local pool - jobs flocking to remote
     * pools?
     *
     * @param job the job on which the style needs to be applied.
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(Job job) throws CondorStyleException {

        //           Removed for JIRA PM-543
        //      String execSiteWorkDir = mSiteStore.getInternalWorkDirectory( job );
        //        String workdir = (String) job.globusRSL.removeKey( "directory" ); // returns old
        // value
        //        workdir = (workdir == null)?execSiteWorkDir:workdir;
        String workdir = job.getDirectory();

        String universe =
                job.condorVariables.containsKey(Condor.UNIVERSE_KEY)
                        ? (String) job.condorVariables.get(Condor.UNIVERSE_KEY)
                        :
                        // default is vanilla universe for glidein style
                        Condor.VANILLA_UNIVERSE;

        if (universe.equalsIgnoreCase(Condor.VANILLA_UNIVERSE)
                || universe.equalsIgnoreCase(Condor.STANDARD_UNIVERSE)
                || universe.equalsIgnoreCase(Condor.PARALLEL_UNIVERSE)) {
            // the glide in/ flocking case
            // submitting directly to condor

            // set the vds change dir key to trigger -w
            // to kickstart invocation for all non transfer jobs
            if (!(job instanceof TransferJob)) {
                job.vdsNS.checkKeyInNS(Pegasus.CHANGE_DIR_KEY, "true");
                // set remote_initialdir for the job only for non transfer jobs
                // this is removed later when kickstart is enabling.
                if (workdir != null) {
                    job.condorVariables.construct("remote_initialdir", workdir);
                    // PM-961 also associate the value as an environment variable
                    job.envVariables.construct(ENV.PEGASUS_SCRATCH_DIR_KEY, workdir);
                }
            }
            // we want the stdout and stderr to be transferred back
            // by Condor to the submit host always
            job.condorVariables.construct("should_transfer_files", "YES");
            String wtto = (String) job.condorVariables.get(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY);
            if (wtto == null) {
                // default value
                job.condorVariables.construct(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY, "ON_EXIT");
            } else {
                // PM-1350 prefer the value specified by the user
                job.condorVariables.construct(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY, wtto);
            }
            // isGlobus = false;
        } else {
            // Is invalid state
            throw new CondorStyleException(errorMessage(job, STYLE_NAME, universe));
        }

        // the condor universe that is determined
        // should be set back in the job.
        job.condorVariables.construct(Condor.UNIVERSE_KEY, universe);

        applyCredentialsForRemoteExec(job);
    }
}
