/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.code.generator.condor.style;

import org.griphyn.cPlanner.code.generator.condor.CondorStyle;
import org.griphyn.cPlanner.code.generator.condor.CondorStyleException;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;

import org.griphyn.cPlanner.namespace.VDS;

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
                             org.griphyn.cPlanner.namespace.Condor.UNIVERSE_KEY;

    public static final String VANILLA_UNIVERSE =
                         org.griphyn.cPlanner.namespace.Condor.VANILLA_UNIVERSE;

    public static final String SCHEDULER_UNIVERSE =
                         org.griphyn.cPlanner.namespace.Condor.SCHEDULER_UNIVERSE;

    public static final String STANDARD_UNIVERSE =
                         org.griphyn.cPlanner.namespace.Condor.STANDARD_UNIVERSE;

    public static final String LOCAL_UNIVERSE =
                         org.griphyn.cPlanner.namespace.Condor.LOCAL_UNIVERSE;

    public static final String PARALLEL_UNIVERSE =
                         org.griphyn.cPlanner.namespace.Condor.PARALLEL_UNIVERSE;


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
        String execSiteWorkDir = mSCHandle.getExecPoolWorkDir(job);
        String workdir = (String) job.globusRSL.removeKey("directory"); // returns old value
        workdir = (workdir == null)?execSiteWorkDir:workdir;

        String universe = job.condorVariables.containsKey( this.UNIVERSE_KEY )?
                              (String)job.condorVariables.get( this.UNIVERSE_KEY):
                              //default is Scheduler universe for condor style
                              Condor.SCHEDULER_UNIVERSE;


        //set the universe for the job
        // Karan Jan 28, 2008
        job.condorVariables.construct( "universe", universe );

        if( universe.equalsIgnoreCase( this.VANILLA_UNIVERSE )  ||
            universe.equalsIgnoreCase( this.STANDARD_UNIVERSE ) ||
            universe.equalsIgnoreCase( this.PARALLEL_UNIVERSE ) ){
            //the glide in/ flocking case
            //submitting directly to condor
            //check if it is a glide in job.
            //vanilla jobs are glide in jobs?
            //No they are not.

            //set the vds change dir key to trigger -w
            //to kickstart invocation for all non transfer jobs
            if(!(job instanceof TransferJob)){
                job.vdsNS.checkKeyInNS(VDS.CHANGE_DIR_KEY, "true");
                //set remote_initialdir for the job only for non transfer jobs
                //this is removed later when kickstart is enabling.

                job.condorVariables.construct("initialdir", workdir);
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

                //check if the job can be run in the workdir or not
                //and whether intial dir is populated before hand or not.
                if(job.runInWorkDirectory() && !job.condorVariables.containsKey("initialdir")){
                    //for local jobs we need initialdir
                    //instead of remote_initialdir
                    job.condorVariables.construct("initialdir", workdir);
                }
                // Let Condor figure out the current working directory on submit host
                // bwSubmit.println("initialdir = " + workdir);


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
