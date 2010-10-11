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


package edu.isi.pegasus.planner.code.generator.condor.style;

import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;

import edu.isi.pegasus.planner.namespace.Pegasus;

/**
 * Enables a job to be directly submitted to the condor pool of which the
 * submit host is a part of.
 * This style is applied for jobs to be run
 *        - on the submit host in the scheduler universe (local pool execution)
 *        - on the local condor pool of which the submit host is a part of
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Condor extends Abstract {

    //some constants imported from the Condor namespace.
    public static final String UNIVERSE_KEY =
                             edu.isi.pegasus.planner.namespace.Condor.UNIVERSE_KEY;

    public static final String VANILLA_UNIVERSE =
                         edu.isi.pegasus.planner.namespace.Condor.VANILLA_UNIVERSE;

    public static final String SCHEDULER_UNIVERSE =
                         edu.isi.pegasus.planner.namespace.Condor.SCHEDULER_UNIVERSE;

    public static final String STANDARD_UNIVERSE =
                         edu.isi.pegasus.planner.namespace.Condor.STANDARD_UNIVERSE;

    public static final String LOCAL_UNIVERSE =
                         edu.isi.pegasus.planner.namespace.Condor.LOCAL_UNIVERSE;

    public static final String PARALLEL_UNIVERSE =
                         edu.isi.pegasus.planner.namespace.Condor.PARALLEL_UNIVERSE;

    
    public static final String TRANSFER_EXECUTABLE_KEY =
                         edu.isi.pegasus.planner.namespace.Condor.TRANSFER_EXECUTABLE_KEY;

    //

    /**
     * The name of the style being implemented.
     */
    public static final String STYLE_NAME = "Condor";

    /**
     * The default constructor.
     */
    public Condor() {
        super();
    }

    /**
     * Applies the condor style to the job. Changes the job so that it results
     * in generation of a condor style submit file that can be directly
     * submitted to the underlying condor scheduler on the submit host, without
     * going through CondorG. This applies to the case of
     *      - local site execution
     *      - submitting directly to the condor pool of which the submit host
     *        is a part of.
     *
     * @param job  the job on which the style needs to be applied.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(SubInfo job) throws CondorStyleException{
        String execSiteWorkDir = mSiteStore.getWorkDirectory(job);
        String workdir = (String) job.globusRSL.removeKey("directory"); // returns old value
        workdir = (workdir == null)?execSiteWorkDir:workdir;

        String universe = job.condorVariables.containsKey( Condor.UNIVERSE_KEY )?
                              (String)job.condorVariables.get( Condor.UNIVERSE_KEY):
                              //default is Scheduler universe for condor style
                              Condor.LOCAL_UNIVERSE;


        //boolean to indicate whether to use remote_initialdir or not
        //remote_initialdir does not work for standard universe
        boolean useRemoteInitialDir = !universe.equals( Condor.STANDARD_UNIVERSE );
        
        //extra check for standard universe
        if( universe.equals( Condor.STANDARD_UNIVERSE ) ){
            //standard universe should be only applied for compute jobs
            int type = job.getJobType();
            if (!( type == SubInfo.COMPUTE_JOB || type == SubInfo.STAGED_COMPUTE_JOB)) {
                //set universe to vanilla universe
                universe = Condor.VANILLA_UNIVERSE;
            }
        }
        
        //set the universe for the job
        // Karan Jan 28, 2008
        job.condorVariables.construct( "universe", universe );

        if( universe.equalsIgnoreCase( Condor.VANILLA_UNIVERSE )  ||
            universe.equalsIgnoreCase( Condor.STANDARD_UNIVERSE ) ||
            universe.equalsIgnoreCase( Condor.PARALLEL_UNIVERSE ) ){
            //the glide in/ flocking case
            //submitting directly to condor
            //check if it is a glide in job.
            //vanilla jobs are glide in jobs?
            //No they are not.

            //set the vds change dir key to trigger -w
            //to kickstart invocation for all non transfer jobs
            if(!(job instanceof TransferJob)){
                job.vdsNS.checkKeyInNS(Pegasus.CHANGE_DIR_KEY, "true");
                //set remote_initialdir for the job only for non transfer jobs
                //this is removed later when kickstart is enabling.

                if( useRemoteInitialDir ){
                    job.condorVariables.construct("remote_initialdir", workdir);
                }else{
                    job.condorVariables.construct("initialdir", workdir);
                }
            }
            else{
                //we need to set s_t_f and w_t_f_o to ensure
                //that condor transfers the proxy to the remote end
                //also the keys below are mutually exclusive to initialdir keys.
                job.condorVariables.construct("should_transfer_files", "YES");
                job.condorVariables.construct("when_to_transfer_output",
                                              "ON_EXIT");
            }
            //isGlobus = false;
        }
        else if(universe.equalsIgnoreCase(Condor.SCHEDULER_UNIVERSE) || universe.equalsIgnoreCase( Condor.LOCAL_UNIVERSE )){

//        Disabled check for execution site to be local.
//        Was required for the sipht case where the cdir job
//        needed to run in the local universe
//        Karan Vahi January 16, 2008

//            if(job.executionPool.equalsIgnoreCase("local")){

                //scheduler universe only makes sense for
                //local site.
                // For the "local" pool the universe should be "scheduler".
                // Thus, jobs run immediately on the submit host, instead of
                // being Condor-delayed in the pool to which the SH belongs.

// Jobs can run on local or scheduler universe as overridden in profiles
// Karan Vahi January 16,2008
//                job.condorVariables.construct("universe",Condor.SCHEDULER_UNIVERSE);

                String ipFiles = job.condorVariables.getIPFilesForTransfer();
                
                //check if the job can be run in the workdir or not
                //and whether intial dir is populated before hand or not.
                if(job.runInWorkDirectory() && !job.condorVariables.containsKey("initialdir")){
                    //for local jobs we need initialdir
                    //instead of remote_initialdir
                    job.condorVariables.construct("initialdir", workdir);
                    
                    if( ipFiles !=  null ){
                        //log a warning message
                        StringBuffer sb = new StringBuffer();
                        sb.append( "Condor File Transfer Mechanism does not work in universe " ).
                           append( universe ).append( "IP Files " ).append( ipFiles ).
                           append( " for job " ).append( job.getID() ).append( " wont be transferred" );
                        mLogger.log( sb.toString(), LogManager.WARNING_MESSAGE_LEVEL );
                        job.condorVariables.removeIPFilesForTransfer();
                    }
                }
                
                // Let Condor figure out the current working directory on submit host
                // bwSubmit.println("initialdir = " + workdir);
                
                //check explicitly for any input files transferred via condor
                //file transfer mechanism
                if( ipFiles != null ){
                    //log a debug message before removing the files
                    StringBuffer sb = new StringBuffer();
                    sb.append( "Removing the following ip files from condor file tx for job " ).
                       append( job.getID() ).append( " " ).append( ipFiles );
                    mLogger.log(  sb.toString(), LogManager.DEBUG_MESSAGE_LEVEL ); 
                    job.condorVariables.removeIPFilesForTransfer();
                }
                
                //check for transfer_executable and remove if set
                //transfer_executable does not work in local/scheduler universe
                if( job.condorVariables.containsKey( Condor.TRANSFER_EXECUTABLE_KEY )){
                    job.condorVariables.removeKey( Condor.TRANSFER_EXECUTABLE_KEY );
                    job.condorVariables.removeKey( "should_transfer_files" );
                    job.condorVariables.removeKey( "when_to_transfer_output" );
                }
                
                //for local or scheduler universe we never should have
                //should_transfer_file or w_t_f
                //the keys can appear if a user in site catalog for local sites
                //specifies these keys for the vanilla universe jobs
                if( job.condorVariables.containsKey( "should_transfer_files" ) || 
                        job.condorVariables.containsKey( "when_to_transfer_output" )){
                    job.condorVariables.removeKey( "should_transfer_files" );
                    job.condorVariables.removeKey( "when_to_transfer_output" );
                }
//           }
//           else{
//               //invalid state. throw an exception??
//                //Is invalid state
//                throw new CondorStyleException( errorMessage( job, STYLE_NAME, universe ) );
//            }

        }
        else{
            //Is invalid state
            throw new CondorStyleException( errorMessage( job, STYLE_NAME, universe ) );
        }


    }

}
