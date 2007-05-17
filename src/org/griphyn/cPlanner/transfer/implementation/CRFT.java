/*
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
package org.griphyn.cPlanner.transfer.implementation;

import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.FileTransfer;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.VDS;
import org.griphyn.cPlanner.namespace.Condor;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.util.Separator;

import java.io.File;
import java.io.FileWriter;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * The implementation that creates transfer jobs referring to the c rft client
 * executable distributed with the VDS.
 *
 * <p>
 * The rft client is invoked on the submit host. Hence there should be an
 * entry in the transformation catalog for logical transformation
 * <code>CRFT</code> at site <code>local</code>. The transformation should point
 * to the client that is distributed with RFT in GT4.
 *
 * <p>
 * The user can tweak the options to the globus-crft client by specifying the properties
 * in the properties files with the prefix <code>pegasus.transfer.crft</code>.
 * The following table lists all the properties with their prefixes stripped off,
 * that the user can specify in the properties files. The default value is used
 * if the user does not specify a particular property. If a value is not specified,
 * the particular option is not generated.
 *
 * <p>
 * For the properties which have a default value of no default and the user not
 * providing a value for the property, the option is not propogated further to the
 * client underneath.  In that case, it is upto the client to construct the
 * appropriate value for that property/option.
 *
 * <p>
 * <table border="1">
 * <tr align="left"><th>property</th><th>default value</th><th>description</th></tr>
 * <tr align="left"><th>endpoint</th>
 *  <td>no default (required option)</td>
 *  <td>The endpoint to contact when creating a service.</td>
 * </tr>
 * <tr align="left"><th>concurrent</th>
 *  <td>no default</td>
 *  <td>The number of simultaneous transfers.</td>
 * </tr>
 * <tr align="left"><th>parallel</th>
 *  <td>no default</td>
 *  <td>The number of parallel sockets to use with each transfer.</td>
 * </tr>
 * <tr align="left"><th>tcp-bs</th>
 *  <td>no default</td>
 *  <td>specifies the size (in bytes) of the TCP buffer to be used by the
 * underlying ftp data channels</td>
 * </tr>
 * <tr align="left"><th>verbose</th>
 *  <td>true</td>
 *  <td>to generate more verbose output, helpful for debugging.</td>
 * </tr>
 * </table>
 *
 * <p>
 * It leads to the creation of the setup chmod jobs to the workflow, that appear
 * as parents to compute jobs in case the transfer implementation does not
 * preserve the X bit on the file being transferred. This is required for
 * staging of executables as part of the workflow. The setup jobs are only added
 * as children to the stage in jobs.
 *
 * <p>
 * In order to use the transfer implementation implemented by this class,
 * <pre>
 *        - the property pegasus.transfer.*.impl must be set to value CRFT.
 * </pre>
 *
 * <p>
 * There should be an entry in the transformation catalog with the fully qualified
 * name as <code>globus::crft</code> for all the sites where workflow is run,
 * or on the local site in case of third party transfers.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CRFT extends AbstractMultipleFTPerXFERJob {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "globus";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "crft";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "globus";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "crft";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION = "C based blocking RFT client";

    /**
     * The prefix for all the properties this mode requires.
     */
    public static final String PROPERTIES_PREFIX = "pegasus.transfer.crft.";

    /**
     * The key name that denotes the endpoint to contact when creating a
     * service.
     */
    public static final String END_POINT_KEY = "endpoint";

    /**
     * The key name that denotes to create a RFT service.
     */
    public static final String CREATE_KEY = "create";

    /**
     * The key name that denotes to start a RFT service.
     */
    public static final String SUBMIT_KEY = "submit";

    /**
     * The key name that denotes to monitor the request. Makes the client block.
     */
    public static final String MONITOR_KEY = "monitor";

    /**
     * The key name that denotes the TCP buffer size in bytes.
     */
    public static final String TCP_BUFFER_SIZE_KEY = "tcp-bs";

    /**
     * The key name that denotes whether to do verbose or not.
     */
    public static final String VERBOSE_KEY = "verbose";

    /**
     * The key name that denotes the number of files to be transferred at any
     * given time.
     */
    public static final String CONCURRENT_KEY = "concurrent";

    /**
     * The key name that denotes the number of parallel sockets to use for each
     * transfer.
     */
    public static final String PARALLEL_KEY = "parallel";

    /**
     * The key name that points to the transfer file that is containing the
     * source and destination urls.
     */
    public static final String TRANSFER_FILE_KEY = "transfer-file";

    /**
     * The options delimiter that is prepended before all the options.
     */
    private static final String OPTIONS_DELIMITER = "--";

    /**
     * The end point for the service.
     */
    private String mEndPoint;

    /**
     * The properties object holding all the RFT specific properties specified
     * by the user in the properties file.
     */
    private Properties mCRFTProps;



    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param properties  the properties object.
     * @param options     the options passed to the Planner.
     */
    public CRFT(PegasusProperties properties,PlannerOptions options){
        super(properties,options);
        mCRFTProps = mProps.matchingSubset(PROPERTIES_PREFIX,false);

        mEndPoint = mCRFTProps.getProperty(END_POINT_KEY);
        //sanity check
        if(mEndPoint == null || mEndPoint.length() == 0){
            String message = "Need to specify a non empty end point using " +
                             "the property " + PROPERTIES_PREFIX + END_POINT_KEY;
            throw new RuntimeException(message);
        }

    }

    /**
     * Calls out to the super class method to create the main structure of the job.
     * In addition, for the CRFT adds the specific condor magic that allows for
     * the  transfer of the input file correctly to working directory.
     *
     *@param job         the SubInfo object for the job, in relation to which
     *                    the transfer node is being added. Either the transfer
     *                    node can be transferring this jobs input files to
     *                    the execution pool, or transferring this job's output
     *                    files to the output pool.
     * @param files       collection of <code>FileTransfer</code> objects
     *                    representing the data files and staged executables to be
     *                    transferred.
     * @param execFiles   subset collection of the files parameter, that identifies
     *                    the executable files that are being transferred.
     * @param txJobName   the name of transfer node.
     * @param jobClass    the job Class for the newly added job. Can be one of the
     *                    following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     *
     * @return  the created TransferJob.
     */
    public TransferJob createTransferJob(SubInfo job,
                                         Collection files,
                                         Collection execFiles,
                                         String txJobName,
                                         int jobClass) {

        TransferJob txJob = super.createTransferJob(job,files,execFiles,
                                                     txJobName,jobClass);
        File f = new File(mPOptions.getSubmitDirectory(),txJob.stdIn);
        //add condor key transfer_input_files to transfer the file
        txJob.condorVariables.addIPFileForTransfer(f.getAbsolutePath());
        /*
        //and other required condor keys
        txJob.condorVariables.checkKeyInNS(Condor.TRANSFER_IP_FILES_KEY,
                                           f.getAbsolutePath());
        txJob.condorVariables.construct("should_transfer_files","YES");
        txJob.condorVariables.construct("when_to_transfer_output","ON_EXIT");
        */

        //the stdin file needs to be transferred as a file not as stdin
        txJob.stdIn = "";

        //we want the transfer job to be run in the
        //directory that Condor or GRAM decided to run
        txJob.condorVariables.removeKey("remote_initialdir");

        return txJob;
    }

    /**
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. This always returns true, indicating
     * transfers can only be done in a third party transfer mode.
     *
     * A value of false does not preclude third party transfers. They still can
     * be done, by setting the property "pegasus.transfer.*.thirdparty.sites".
     *
     * @return false
     */
    public boolean useThirdPartyTransferAlways(){
        return false;
    }

    /**
     * Returns a boolean indicating whether the transfer protocol being used by
     * the implementation preserves the X Bit or not while staging.
     *
     * @return boolean
     */
    public boolean doesPreserveXBit(){
        return true;
    }



    /**
     * Adds the dirmanager job to the workflow, that do a chmod on the executable
     * files that are being staged. It is empty as RFT preserves X bit permission
     * while staging files.
     *
     * @param computeJobName the name pf the computeJob for which the files are
     *                       being staged.
     * @param txJobName      the name of the transfer job that is staging the files.
     * @param execFiles      the executable files that are being staged.
     * @param transferClass  the class of transfer job
     *
     * @return boolean indicating whether any XBitJobs were succesfully added or
     *         not.
     */
    public boolean addSetXBitJobs(String computeJobName,
                                  String txJobName,
                                  Collection execFiles,
                                  int transferClass){
        return false;
    }


    /**
     * Constructs the arguments to the transfer executable that need to be
     * passed to the executable referred to in this transfer mode. Since the
     * rft client is run on the submit host, the path to the input file
     * to the rft client is given, instead of passing it through condor
     * files.
     * In addition , it SETS THE STDIN of the transfer job to null, as the
     * input file is not being sent to the remote sides. There should be a
     * generic function prepareIPFile to do this.
     *
     * @param job   the object containing the transfer node.
     *
     * @return  the argument string
     */
    protected String generateArgumentString(TransferJob job){
        File f = new File(mPOptions.getSubmitDirectory(),job.stdIn);


        StringBuffer sb = new StringBuffer();

        //construct the few default options
        sb.append(OPTIONS_DELIMITER).append(MONITOR_KEY).append(" ")
          .append(OPTIONS_DELIMITER).append(CREATE_KEY).append(" ")
          .append(OPTIONS_DELIMITER).append(SUBMIT_KEY).append(" ")
          .append(OPTIONS_DELIMITER).append(VERBOSE_KEY).append(" ");

        sb.append(construct(END_POINT_KEY,
                            mEndPoint));

        //construct the optional long opts
        sb.append(construct(PARALLEL_KEY,
                            mCRFTProps.getProperty(PARALLEL_KEY)));
        sb.append(construct(CONCURRENT_KEY,
                            mCRFTProps.getProperty(CONCURRENT_KEY)));
        sb.append(construct(TCP_BUFFER_SIZE_KEY,
                            mCRFTProps.getProperty(TCP_BUFFER_SIZE_KEY)));

        //construct the transfer file
        sb.append(construct(TRANSFER_FILE_KEY,f.getName()));

        //setting the stdin to null. we no longer need it.
        //if left specified, condor would try to transfer
        //it via GASS
        //Commented by Karan Feb 23, 06. We need the path to the stdin still.
        //job.stdIn = "";
        return sb.toString();
    }


    /**
     * Writes to a file on the submit host, that is passed to the rft-client
     * as input. The rft-client is always run on the submit host, and hence
     * can access the file.
     *
     *
     * @param writer    the writer to the stdin file.
     * @param files    Collection of <code>FileTransfer</code> objects containing
     *                 the information about sourceam fin and destURL's.
     *
     *
     * @throws Exception
     */
    protected void writeJumboStdIn(FileWriter writer, Collection files) throws
        Exception {

        //iterating thru all the FileTransfers
        writer.write("#Source and Destination URLS\n");
        for (Iterator it = files.iterator();it.hasNext();) {
            FileTransfer ft = (FileTransfer)it.next();
            //the FileTransfer object writes out in T2 compatible format
            writer.write(ft.getSourceURL().getValue());
            writer.write(" ");
            writer.write(ft.getDestURL().getValue());
            writer.write("\n");
        }
        writer.flush();
    }


    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public  String getDescription(){
        return this.DESCRIPTION;
    }

    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry(String siteHandle){
        List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.getTCEntries(this.TRANSFORMATION_NAMESPACE,
                                               this.TRANSFORMATION_NAME,
                                               this.TRANSFORMATION_VERSION,
                                               siteHandle,
                                               TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " + getCompleteTCName()
                + " :" + e.getMessage(),LogManager.ERROR_MESSAGE_LEVEL);
        }

        //see if any record is returned or not
        return(tcentries == null)?
               null:
              (TransformationCatalogEntry) tcentries.get(0);
    }

    /**
     * Returns the namespace of the derivation that this implementation
     * refers to.
     *
     * @return the namespace of the derivation.
     */
    protected String getDerivationNamespace(){
        return this.DERIVATION_NAMESPACE;
    }


    /**
     * Returns the logical name of the derivation that this implementation
     * refers to.
     *
     * @return the name of the derivation.
     */
    protected String getDerivationName(){
        return this.DERIVATION_NAME;
    }

    /**
     * Returns the version of the derivation that this implementation
     * refers to.
     *
     * @return the version of the derivation.
     */
    protected String getDerivationVersion(){
        return this.DERIVATION_VERSION;
    }

    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name.
     */
    protected String getCompleteTCName(){
        return Separator.combine(this.TRANSFORMATION_NAMESPACE,
                                 this.TRANSFORMATION_NAME,
                                 this.TRANSFORMATION_VERSION);
    }

    /**
     * A helper method to generate a required argument option for the client.
     * It is generated only if a non null value is passed.
     *
     * @param option  the long version of the option.
     * @param value   the value for the option
     *
     * @return the constructed string.
     */
    private String construct(String option,String value){
        if(value == null || value.length() == 0){
            return "";
        }
        StringBuffer sb = new StringBuffer(16);
        sb.append(OPTIONS_DELIMITER).append(option).
            append(" ").append(value).append(" ");

        return sb.toString();
    }

}
