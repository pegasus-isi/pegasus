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

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.namespace.VDS;


/**
 * An application specific launcher, that is used to launch DataCutter jobs. To
 * use this Gridstart mode, the following Pegasus Profiles need to be associated
 * with the job.
 *
 * <pre>
 *     gridstart         = DCLauncher
 *     gridstart.path    = path to the DCLauncher script.
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DCLauncher
    extends NoGridStart {


    /**
     * The basename of the class that is implmenting this. Could have
     * been determined by reflection.
     */
    public static final String CLASSNAME = "DCLauncher";

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "DCLauncher";


    /**
     * The default constructor.
     */
    public DCLauncher() {
        super();
    }

    /**
    * Enables a job to run on the grid by launching it via the DataCutter executable.
    * It ends up running the executable directly without going through any intermediate
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
       boolean result = super.enable( job, isGlobusJob );

       //figure out the path to the datacutter executable
       String gridStartPath = job.vdsNS.getStringValue( VDS.GRIDSTART_PATH_KEY );
       //sanity check
       if (gridStartPath == null){
           mLogger.log( "The path to gridstart " + this.SHORT_NAME + " not specified",
                        LogManager.ERROR_MESSAGE_LEVEL );
           mLogger.log( "Set the Pegasus Profile key " + VDS.GRIDSTART_PATH_KEY ,
                        LogManager.ERROR_MESSAGE_LEVEL );
           return false;
       }

       //construct the arguments for the dc launcher.
       StringBuffer arguments = new StringBuffer();
       arguments.append( job.getRemoteExecutable() ).append( " " ).append( job.getArguments() );
       construct( job, "arguments", arguments.toString() );
       //the executable for the job is now the DC launcher
       job.setRemoteExecutable( gridStartPath );
       construct( job, "executable", gridStartPath );

       return result;
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



}
