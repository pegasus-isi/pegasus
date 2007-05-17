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
package org.griphyn.cPlanner.code.gridstart;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.code.GridStart;
import org.griphyn.cPlanner.code.POSTScript;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.AggregatedJob;

import org.griphyn.cPlanner.namespace.VDS;
import org.griphyn.cPlanner.namespace.Dagman;

import java.io.File;

import java.util.Collection;
import java.util.Iterator;

/**
 * This class ends up running the job directly on the grid, without wrapping
 * it in any other launcher executable.
 * It ends up connecting the jobs stdio and stderr to condor commands to
 * ensure they are sent back to the submit host.
 *
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision: 1.4 $
 */

public class NoGridStart implements GridStart {

    /**
     * The basename of the class that is implmenting this. Could have
     * been determined by reflection.
     */
    public static final String CLASSNAME = "NoGridStart";

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "none";


    /**
     * The LogManager object which is used to log all the messages.
     */
    private LogManager mLogger;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The submit directory where the submit files are being generated for
     * the workflow.
     */
    private String mSubmitDir;

    /**
     * The argument string containing the arguments with which the exitcode
     * is invoked on kickstart output.
     */
    private String mExitParserArguments;

    /**
     * Initializes the GridStart implementation.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param submitDir  the submit directory where the submit file for the job
     *                   has to be generated.
     * @param dag        the concrete dag so far.
     */
    public void initialize( PegasusProperties properties, String submitDir, ADag dag ){
        mSubmitDir = submitDir;
        mProps     = properties;
        mLogger    = LogManager.getInstance();
        mExitParserArguments = getExitCodeArguments();
    }

    /**
     * Enables a collection of jobs and puts them into an AggregatedJob.
     * The assumption here is that all the jobs are being enabled by the same
     * implementation. It enables the jobs and puts them into the AggregatedJob
     * that is passed to it.
     *
     * @param aggJob the AggregatedJob into which the collection has to be
     *               integrated.
     * @param jobs   the collection of jobs (SubInfo) that need to be enabled.
     *
     * @return the AggregatedJob containing the enabled jobs.
     * @see #enable(SubInfo,boolean)
     */
    public  AggregatedJob enable(AggregatedJob aggJob,Collection jobs){
        for (Iterator it = jobs.iterator(); it.hasNext(); ) {
            SubInfo job = (SubInfo)it.next();
            //always pass isGlobus true as always
            //interested only in executable strargs
            this.enable(job, true);
            aggJob.add(job);
        }
        return aggJob;
    }


    /**
     * Enables a job to run on the grid by launching it directly. It ends
     * up running the executable directly without going through any intermediate
     * launcher executable. It connects the stdio, and stderr to underlying
     * condor mechanisms so that they are transported back to the submit host.
     *
     * @param job  the <code>SubInfo</code> object containing the job description
     *             of the job that has to be enabled on the grid.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     *
     * @return boolean true if enabling was successful,else false in case when
     *         the path to kickstart could not be determined on the site where
     *         the job is scheduled.
     */
    public boolean enable(SubInfo job, boolean isGlobusJob) {
        //take care of relative submit directory if specified
        String submitDir = mSubmitDir + mSeparator;
//        String submitDir = getSubmitDirectory( mSubmitDir , job) + mSeparator;

        //the executable path and arguments are put
        //in the Condor namespace and not printed to the
        //file so that they can be overriden if desired
        //later through profiles and key transfer_executable
        construct(job,"executable", job.executable);

        //sanity check for the arguments
        if(job.strargs != null && job.strargs.length() > 0){
            construct(job, "arguments", job.strargs);
        }

        // handle stdin
        if (job.stdIn.length() > 0) {
            construct(job,"input",submitDir + job.stdIn);
            if (isGlobusJob) {
                //this needs to be true as you want the stdin
                //to be transfered to the remote execution
                //pool, as in case of the transfer script.
                //it needs to be set if the stdin is already
                //prepopulated at the remote side which
                //it is not.
                construct(job,"transfer_input","true");
            }
        }

        if (job.stdOut.length() > 0) {
            //handle stdout
            construct(job,"output",job.stdOut);
            if (isGlobusJob) {
                construct(job,"transfer_output","false");
            }
        } else {
            // transfer output back to submit host, if unused
            construct(job,"output",submitDir + job.jobName + ".out");
            if (isGlobusJob) {
                construct(job,"transfer_output","true");
            }
        }

        if (job.stdErr.length() > 0) {
            //handle stderr
            construct(job,"error",job.stdErr);
            if (isGlobusJob) {
                construct(job,"transfer_error","false");
            }
        } else {
            // transfer error back to submit host, if unused
            construct(job,"error",submitDir + job.jobName + ".err");
            if (isGlobusJob) {
                construct(job,"transfer_error","true");
            }
        }

        return true;
    }

    /**
     * Indicates whether the enabling mechanism can set the X bit
     * on the executable on the remote grid site, in addition to launching
     * it on the remote grid stie
     *
     * @return false, as no wrapper executable is being used.
     */
    public  boolean canSetXBit(){
        return false;
    }

    /**
     * Returns the value of the vds profile with key as VDS.GRIDSTART_KEY,
     * that would result in the loading of this particular implementation.
     * It is usually the name of the implementing class without the
     * package name.
     *
     * @return the value of the profile key.
     * @see org.griphyn.cPlanner.namespace.VDS#GRIDSTART_KEY
     */
    public  String getVDSKeyValue(){
        return this.CLASSNAME;
    }


    /**
     * Returns a short textual description in the form of the name of the class.
     *
     * @return  short textual description.
     */
    public String shortDescribe(){
        return this.SHORT_NAME;
    }

    /**
     * Returns the SHORT_NAME for the POSTScript implementation that is used
     * to be as default with this GridStart implementation.
     *
     * @return  the identifier for the NoPOSTScript POSTScript implementation.
     *
     * @see POSTScript#shortDescribe()
     */
    public String defaultPOSTScript(){
        return NoPOSTScript.SHORT_NAME;
    }


    /**
     * Returns a boolean indicating whether to remove remote directory
     * information or not from the job. This is determined on the basis of the
     * style key that is associated with the job.
     *
     * @param job the job in question.
     *
     * @return boolean
     */
    private boolean removeDirectoryKey(SubInfo job){
        String style = job.vdsNS.containsKey(VDS.STYLE_KEY) ?
                       null :
                       (String)job.vdsNS.get(VDS.STYLE_KEY);

        //is being run. Remove remote_initialdir if there
        //condor style associated with the job
        //Karan Nov 15,2005
        return (style == null)?
                false:
                style.equalsIgnoreCase(VDS.CONDOR_STYLE);

    }

    /**
     * Constructs a condor variable in the condor profile namespace
     * associated with the job. Overrides any preexisting key values.
     *
     * @param job   contains the job description.
     * @param key   the key of the profile.
     * @param value the associated value.
     */
    private void construct(SubInfo job, String key, String value){
        job.condorVariables.construct(key,value);
    }

    /**
     * Returns a string containing the arguments with which the exitcode
     * needs to be invoked.
     *
     * @return the argument string.
     */
    private String getExitCodeArguments(){
        return mProps.getPOSTScriptArguments();
    }


}
