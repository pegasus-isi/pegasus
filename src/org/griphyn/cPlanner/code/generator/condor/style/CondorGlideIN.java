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

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;

import org.griphyn.cPlanner.namespace.VDS;
import org.griphyn.cPlanner.namespace.Condor;

/**
 * Enables a job to be submitted to nodes that are logically part of the local pool,
 * but physically are not.
 *
 * This style is applied for jobs to be run
 *        - on the nodes that have been glided into the local pool
 *        - on the nodes that have been flocked to the local pool (NOT TESTED)
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class CondorGlideIN extends Abstract {

    /**
     * The name of the style being implemented.
     */
    public static final String STYLE_NAME = "CondorGlideIN";


    /**
     * The default constructor.
     */
    public CondorGlideIN() {
        super();
    }

    /**
     * Applies the style to the job to be run in a condor glide in environment.
     * condor style to the job. Changes the job so that it results
     * in generation of a submit file that can be directly submitted to the
     * underlying condor scheduler on the submit host, without
     * going through CondorG and the jobs run only on the nodes that have
     * been glided in from a particular remote pool.
     * Please  note that GlideIn only works if all the application jobs are
     * being run via kickstart, as it relies heavily on the ability of the
     * launcher to change the directory before running the application job
     * on the remote end.
     *
     * This applies to the case of
     *      - nodes glided in to a local pool
     *      - jobs flocking to remote pools?
     *
     * @param job  the job on which the style needs to be applied.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply( SubInfo job ) throws CondorStyleException{

        String execSiteWorkDir = mSiteStore.getWorkDirectory( job );
        String workdir = (String) job.globusRSL.removeKey( "directory" ); // returns old value
        workdir = (workdir == null)?execSiteWorkDir:workdir;

        String universe = job.condorVariables.containsKey( Condor.UNIVERSE_KEY )?
                              (String)job.condorVariables.get( Condor.UNIVERSE_KEY ):
                              //default is vanilla universe for glidein style
                              Condor.VANILLA_UNIVERSE;

        if( universe.equalsIgnoreCase( Condor.VANILLA_UNIVERSE ) ||
            universe.equalsIgnoreCase( Condor.STANDARD_UNIVERSE ) ){
            //the glide in/ flocking case
            //submitting directly to condor

            //set the vds change dir key to trigger -w
            //to kickstart invocation for all non transfer jobs
            if( !( job instanceof TransferJob ) ){
                job.vdsNS.checkKeyInNS( VDS.CHANGE_DIR_KEY, "true" );
                //set remote_initialdir for the job only for non transfer jobs
                //this is removed later when kickstart is enabling.
                job.condorVariables.construct( "remote_initialdir", workdir );
            }
            //we want the stdout and stderr to be transferred back
            //by Condor to the submit host always
            job.condorVariables.construct( "should_transfer_files", "YES" );
            job.condorVariables.construct( "when_to_transfer_output", "ON_EXIT" );
            //isGlobus = false;
        }
        else{
            //Is invalid state
            throw new CondorStyleException( errorMessage( job, STYLE_NAME, universe ) );
        }


    }


}
