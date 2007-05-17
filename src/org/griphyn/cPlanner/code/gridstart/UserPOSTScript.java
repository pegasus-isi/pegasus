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

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.namespace.Dagman;

import org.griphyn.cPlanner.code.POSTScript;

import java.io.File;

/**
 * A user defined post script. By default, the postscript is given the name
 * of the job output file on the submit host, to work upon. Additional arguments
 * to the post script can be specified via properties or profiles.
 *
 * The postscript is only constructed if the job already contains the
 * Dagman profile key passed.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class UserPOSTScript implements POSTScript {

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "user";

    /**
     * The LogManager object which is used to log all the messages.
     */
    protected LogManager mLogger;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;


    /**
     * The path to the user postscript on the submit host.
     *
     */
    protected String mPOSTScriptPath;

    /**
     * The default constructor.
     */
    public UserPOSTScript(){
        mLogger = LogManager.getInstance();
    }


    /**
     * Initialize the POSTScript implementation.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param path       the path to the POSTScript on the submit host.
     * @param submitDir  the submit directory where the submit file for the job
     *                   has to be generated.
     *
     * @throws RuntimeException in case of path being null.
     */
    public void initialize( PegasusProperties properties,
                            String path,
                            String submitDir ){

        mProps     = properties;
        mPOSTScriptPath = path;

        if ( path == null ){
            throw new RuntimeException( "Path to user specified postscript not given" );
        }
    }



    /**
     * Constructs the postscript that has to be invoked on the submit host
     * after the job has executed on the remote end. The postscript works on the
     * stdout of the remote job, that has been transferred back to the submit
     * host by Condor.
     * <p>
     * The postscript is constructed and populated as a profile
     * in the DAGMAN namespace.
     *
     *
     * @param job  the <code>SubInfo</code> object containing the job description
     *             of the job that has to be enabled on the grid.
     * @param key  the <code>DAGMan</code> profile key that has to be inserted.
     *
     * @return boolean true if postscript was generated,else false.
     */
    public boolean construct(SubInfo job, String key) {
        boolean constructed = false;

        //see if any specific postscript was specified for this job
        //get the value user specified for the job
        String postscript = mPOSTScriptPath;

        job.dagmanVariables.construct( Dagman.OUTPUT_KEY,
                                       (String) job.condorVariables.get( "output" )
                                       );

        //arguments are taken care of automatically in DagMan namespace

        constructed = true;
        //put in the postscript
        mLogger.log("Postscript constructed is " + postscript,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        job.dagmanVariables.checkKeyInNS( key, postscript );

//        else{
//            //Karan Nov 15,2005 VDS BUG FIX 128
//            //Always remove POST_SCRIPT_ARGUMENTS
//            job.dagmanVariables.removeKey(Dagman.POST_SCRIPT_ARGUMENTS_KEY);
//        }

        return constructed;
    }


    /**
     * Returns a short textual description of the implementing class.
     *
     * @return  short textual description.
     */
    public String shortDescribe(){
        return this.SHORT_NAME;
    }


    /**
     * Returns the path to exitcode that is to be used on the kickstart
     * output.
     *
     * @return the path to the exitcode script to be invoked.
     */
    public String getExitCodePath(){
        StringBuffer sb = new StringBuffer();
        sb.append(mProps.getPegasusHome()).append(File.separator).append("bin");
        sb.append(File.separator).append("exitcode");

        return sb.toString();
    }


}