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

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Globus;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.util.Map;

/**
 * This implementation enables a job to be submitted via CondorG to remote grid sites. This is the
 * default style, that is applied to all the jobs in the concrete workflow.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CondorG extends Abstract {

    /** The default Constructor. */
    public CondorG() {
        super();
    }

    /** The name of the style being implemented. */
    public static final String STYLE_NAME = "CondorG";

    /**
     * Applies the globus style to the job. Changes the job so that it results in generation of a
     * condor style submit file that can be submitted via CondorG to a remote jobmanager. This is
     * the default case.
     *
     * @param job the job on which the style needs to be applied.
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(Job job) throws CondorStyleException {
        //           Removed for JIRA PM-543
        //
        //        String execSiteWorkDir = mSiteStore.getInternalWorkDirectory( job );
        //        String workdir = (String) job.globusRSL.removeKey( "directory" ); // returns old
        // value
        //        workdir = (workdir == null) ? execSiteWorkDir : workdir;
        String workdir = job.getDirectory();

        String universe =
                job.condorVariables.containsKey(Condor.UNIVERSE_KEY)
                        ? (String) job.condorVariables.get(Condor.UNIVERSE_KEY)
                        :
                        // default is VANILLA universe for globus style
                        Condor.VANILLA_UNIVERSE;

        if (universe.equalsIgnoreCase(Condor.STANDARD_UNIVERSE)) {
            // construct the appropriate jobtype RSL
            job.globusRSL.construct("jobtype", "condor");
        } else if (universe.equalsIgnoreCase(Condor.VANILLA_UNIVERSE)) {
            // the default case where no universe specified
            // or a vanilla universe specified

            // by default pegasus creates globus universe jobs
            // sinfo.condorVariables.construct("universe",Condor.GLOBUS_UNIVERSE);
            // since condor 6.7.6 we have the notion of grid universe
            // and grid types.
            job.condorVariables.construct(Condor.UNIVERSE_KEY, Condor.GRID_UNIVERSE);

            StringBuffer gridResource = new StringBuffer();

            // default type is gt2
            SiteCatalogEntry s = mSiteStore.lookup(job.getSiteHandle());
            GridGateway g = s.selectGridGateway(job.getGridGatewayJobType());

            if (g == null) {
                throw new CondorStyleException(
                        "No valid grid gateway found for site "
                                + job.getSiteHandle()
                                + " for job "
                                + job.getID());
            }

            gridResource.append(g.getType()).append(" ").append(g.getContact());

            // System.out.println( "Grid Resource for job " + job.getName() + " is " +
            // gridResource.toString() );
            job.condorVariables.construct(Condor.GRID_RESOURCE_KEY, gridResource.toString());
        } else {
            // running jobs in scheduler universe
            // or some other universe
            // through CondorG does not make sense.
            // Is invalid state
            throw new CondorStyleException(errorMessage(job, STYLE_NAME, universe));
        }

        job.condorVariables.construct("remote_initialdir", workdir);
        if (workdir != null) {
            // PM-961 also associate the value as an environment variable
            job.envVariables.construct(ENV.PEGASUS_SCRATCH_DIR_KEY, workdir);
        }

        // associate the proxy to be used
        // we always say a proxy is required for CondorG submission
        // PM-731
        job.setSubmissionCredential(CredentialHandler.TYPE.x509);
        applyCredentialsForRemoteExec(job);

        // PM-962 handle resource requirements expressed as pegasus profiles
        // and populate them as globus profiles if required
        handleResourceRequirements(job);
    }

    /**
     * Looks into the job to check if any of the Resource requirements are expressed as pegasus
     * profiles, and converts them to globus profiles if corresponding globus profile is not
     * present.
     *
     * @param job
     */
    public void handleResourceRequirements(Job job) {

        Pegasus profiles = job.vdsNS;
        Globus rsl = job.globusRSL;

        // sanity check
        if (profiles == null || profiles.isEmpty()) {
            return;
        }

        // handle runtime key as a special case
        if (profiles.containsKey(Pegasus.RUNTIME_KEY)) {
            long runtime = Long.parseLong(profiles.getStringValue(Pegasus.RUNTIME_KEY));
            if (!rsl.containsKey(Globus.MAX_WALLTIME_KEY)) {
                // take the ceiling value
                long runtimeM = (long) Math.ceil(runtime / 60.0);
                rsl.construct(Globus.MAX_WALLTIME_KEY, Long.toString(runtimeM));
            }
        }

        // we only take value of Pegasus profile if corresponding
        // globus profile is not set
        for (Map.Entry<String, String> entry : Globus.rslToPegasusProfiles().entrySet()) {
            String rslKey = entry.getKey();
            String pegasusKey = entry.getValue();

            if (!rsl.containsKey(rslKey) && profiles.containsKey(pegasusKey)) {
                // one to one mapping
                rsl.construct(rslKey, profiles.getStringValue(pegasusKey));
            }
        }
    }
}
