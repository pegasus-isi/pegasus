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

package org.griphyn.cPlanner.transfer.sls;


import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.transfer.SLS;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;

import java.io.File;
import java.io.IOException;
import java.io.FileWriter;

import java.util.Iterator;
import java.util.Set;



/**
 * This uses the Condor File Transfer mechanism for the second level staging.
 *
 * It will work only if the Pegasus Style profile ( pegasus::style ) has a value
 * of condor.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Condor   implements SLS {


    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION = "Condor File Transfer Mechanism";

    /**
     * The handle to the site catalog.
     */
//    protected PoolInfoProvider mSiteHandle;
    protected SiteStore mSiteStore;


    /**
     * The handle to the properties.
     */
    protected PegasusProperties mProps;

    /**
     * The handle to the planner options.
     */
    protected PlannerOptions mPOptions;

    /**
     * The handle to the logging manager.
     */
    protected LogManager mLogger;


    /**
     * The default constructor.
     */
    public Condor() {
    }


    /**
     * Initializes the SLS implementation.
     *
     * @param bag the bag of objects. Contains access to catalogs etc.
     */
    public void initialize( PegasusBag bag ) {
        mProps      = bag.getPegasusProperties();
        mPOptions   = bag.getPlannerOptions();
        mLogger     = bag.getLogger();
//        mSiteHandle = bag.getHandleToSiteCatalog();
        mSiteStore = bag.getHandleToSiteStore();
    }

    /**
     * Returns a boolean whether the SLS implementation does a condor based
     * modification or not. By condor based modification we mean whether it
     * uses condor specific classads to achieve the second level staging or not.
     *
     * @return false
     */
    public boolean doesCondorModifications(){
        return true;
    }

    /**
     * Constructs a command line invocation for a job, with a given sls file.
     * The SLS maybe null. In the case where SLS impl does not read from a file,
     * it is advised to create a file in generateSLSXXX methods, and then read
     * the file in this function and put it on the command line.
     *
     * @param job          the job that is being sls enabled
     * @param slsFile      the slsFile can be null
     *
     * @return invocation string
     */
    public String invocationString( SubInfo job, File slsFile ){
        return null;
    }





    /**
     * Returns a boolean indicating whether it will an input file for a job
     * to do the transfers. Transfer reads from stdin the file transfers that
     * it needs to do. Always returns true, as we need to transfer the proxy
     * always.
     *
     * @param job the job being detected.
     *
     * @return false
     */
    public boolean needsSLSInput( SubInfo job ) {
        return false;
    }

    /**
     * Returns a boolean indicating whether it will an output file for a job
     * to do the transfers. Transfer reads from stdin the file transfers that
     * it needs to do.
     *
     * @param job the job being detected.
     *
     * @return false
     */
    public boolean needsSLSOutput( SubInfo job ) {
        return false;
    }

    /**
     * Returns the LFN of sls input file.
     *
     * @param job SubInfo
     *
     * @return the name of the sls input file.
     */
    public String getSLSInputLFN( SubInfo job ){
        return null;
    }


    /**
     * Returns the LFN of sls output file.
     *
     * @param job SubInfo
     *
     * @return the name of the sls input file.
     */
    public String getSLSOutputLFN( SubInfo job ){
        return null;
    }


    /**
     * Generates a second level staging file of the input files to the worker
     * node directory.
     *
     * @param job           job for which the file is being created
     * @param fileName      name of the file that needs to be written out.
     * @param submitDir     submit directory where it has to be written out.
     * @param headNodeDirectory    directory on the head node of the compute site.
     * @param workerNodeDirectory  worker node directory
     *
     * @return null as no SLS file is generated.
     */
    public File generateSLSInputFile( SubInfo job,
                                      String fileName,
                                      String submitDir,
                                      String headNodeDirectory,
                                      String workerNodeDirectory ) {

        return null;
    }

    /**
     * Generates a second level staging file of the input files to the worker
     * node directory.
     *
     * @param job the job for which the file is being created
     * @param fileName the name of the file that needs to be written out.
     * @param submitDir the submit directory where it has to be written out.
     * @param headNodeDirectory the directory on the head node of the
     *   compute site.
     * @param workerNodeDirectory the worker node directory
     *
     * @return null as no SLS file is generated.
     *
     */
    public File generateSLSOutputFile(SubInfo job, String fileName,
                                      String submitDir,
                                      String headNodeDirectory,
                                      String workerNodeDirectory) {


        return null;

    }


    /**
     * Modifies a job for the first level staging to headnode.This is to add
     * any files that needs to be staged to the head node for a job specific
     * to the SLS implementation. If any file needs to be added, a <code>FileTransfer</code>
     * object should be created and added as an input or an output file.
     *
     *
     * @param job           the job
     * @param submitDir     the submit directory
     * @param slsInputLFN   the sls input file if required, that is used for
     *                      staging in from the head node to worker node directory.
     * @param slsOutputLFN  the sls output file if required, that is used
     *                      for staging in from the head node to worker node directory.
     * @return boolean
     */
    public boolean modifyJobForFirstLevelStaging( SubInfo job,
                                                  String submitDir,
                                                  String slsInputLFN,
                                                  String slsOutputLFN ) {


       return true;

    }

    /**
     * Modifies a compute job for second level staging. Adds the appropriate
     * condor classads. It assumes that all the files are being moved to and from
     * the submit directory directly. Ignores any headnode parameters passed.
     *
     *
     * @param job the job to be modified.
     * @param headNodeURLPrefix the url prefix for the server on the headnode
     * @param headNodeDirectory the directory on the headnode, where the
     *   input data is read from and the output data written out.
     * @param workerNodeDirectory the directory in the worker node tmp
     *
     * @return boolean indicating whether job was successfully modified or
     *   not.
     *
     */
    public boolean modifyJobForWorkerNodeExecution( SubInfo job,
                                                    String headNodeURLPrefix,
                                                    String headNodeDirectory,
                                                    String workerNodeDirectory ) {



        //sanity check on style of the job
        //handle the -w option that asks kickstart to change
        //directory before launching an executable.
        String style = (String)job.vdsNS.get( VDS.STYLE_KEY );
        if( style == null || !( style.equals( VDS.CONDOR_STYLE ) || style.equals( VDS.GLIDEIN_STYLE ) ) ){
            mLogger.log( "Invalid style for the job " + job.getName(),
                         LogManager.ERROR_MESSAGE_LEVEL );
            return false;
        }

        //remove any directory. let condor figure it out
        job.condorVariables.removeKey( "initialdir" );

        //set the initial dir to the headnode directory
        job.condorVariables.construct( "initialdir", headNodeDirectory );

        //iterate through all the input files
        for( Iterator it = job.getInputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = ( PegasusFile )it.next();

            //ignore any input files of FileTransfer as they are first level
            //staging put in by Condor Transfer refiner
            if( pf instanceof FileTransfer ){
                continue;
            }


            String lfn = pf.getLFN();
            //add an input file for transfer
            //job.condorVariables.addIPFileForTransfer( headNodeDirectory + File.separator + lfn );
            //we add just the lfn as we are setting initialdir
            job.condorVariables.addIPFileForTransfer( lfn );
        }

        //iterate and add output files for transfer back
        for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = ( PegasusFile )it.next();
            String lfn = pf.getLFN();

            //ignore any input files of FileTransfer as they are first level
            //staging put in by Condor Transfer refiner
            if( pf instanceof FileTransfer ){
                continue;
            }


            //add an input file for transfer
            job.condorVariables.addOPFileForTransfer( lfn );
        }


        return true;

    }

}
