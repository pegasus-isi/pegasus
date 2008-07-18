/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.griphyn.cPlanner.code.generator.condor.style;

import org.griphyn.cPlanner.code.generator.condor.CondorStyle;
import org.griphyn.cPlanner.code.generator.condor.CondorStyleException;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.namespace.Condor;

/**
 * This implementation enables a job to be submitted via CondorG to remote
 * grid sites. This is the default style, that is applied to all the jobs
 * in the concrete workflow.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class CondorG extends Abstract {

    /**
     * The default Constructor.
     */
    public CondorG() {
        super();
    }

    /**
     * The name of the style being implemented.
     */
    public static final String STYLE_NAME = "CondorG";



    /**
     * Applies the globus style to the job. Changes the job so that it results
     * in generation of a condor style submit file that can be submitted
     * via CondorG to a remote jobmanager. This is the default case.
     *
     * @param job  the job on which the style needs to be applied.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply( SubInfo job ) throws CondorStyleException {
        String execSiteWorkDir = mSiteStore.getWorkDirectory( job );
        String workdir = (String) job.globusRSL.removeKey( "directory" ); // returns old value
        workdir = (workdir == null) ? execSiteWorkDir : workdir;

        String universe = job.condorVariables.containsKey(Condor.UNIVERSE_KEY)?
            (String)job.condorVariables.get(Condor.UNIVERSE_KEY):
            //default is VANILLA universe for globus style
            Condor.VANILLA_UNIVERSE;

        if( universe.equalsIgnoreCase( Condor.STANDARD_UNIVERSE ) ){
            //construct the appropriate jobtype RSL
            job.globusRSL.construct( "jobtype", "condor" );
        }
        else if(universe.equalsIgnoreCase( Condor.VANILLA_UNIVERSE )){
            //the default case where no universe specified
            //or a vanilla universe specified

            //by default pegasus creates globus universe jobs
            //sinfo.condorVariables.construct("universe",Condor.GLOBUS_UNIVERSE);
            //since condor 6.7.6 we have the notion of grid universe
            //and grid types.
            job.condorVariables.construct( Condor.UNIVERSE_KEY,Condor.GRID_UNIVERSE );
            //the job type by default is GT2 unless overriden
            //through profiles
            if(!job.condorVariables.containsKey( Condor.GRID_JOB_TYPE_KEY ) ){
                job.condorVariables.construct( Condor.UNIVERSE_KEY,
                                               Condor.GLOBUS_UNIVERSE );
            }
            job.condorVariables.construct( "globusscheduler",
                                           job.globusScheduler );
        }
        else{
            //running jobs in scheduler universe
            //or some other universe
            //through CondorG does not make sense.
            //Is invalid state
            throw new CondorStyleException( errorMessage( job, STYLE_NAME, universe ) );
        }
        //remote_initialdir might be needed to removed
        //later if running for a LCG site
        //bwSubmit.println("remote_initialdir = " + workdir);
        job.condorVariables.construct( "remote_initialdir", workdir );


    }

}
