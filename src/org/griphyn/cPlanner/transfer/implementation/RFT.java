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
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.FileTransfer;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.util.Separator;

import java.io.FileWriter;
import java.io.File;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * The implementation that creates transfer jobs referring to the rft-client
 * distributed with GT4 to do transfers between various sites.
 * The rft-client connects to a RFT service running on a particular host.
 * <p>
 * The rft client is always invoked on the submit host. Hence there should be an
 * entry in the transformation catalog for logical transformation
 * <code>rft</code> at site <code>local</code>. The transformation should point
 * to the client that is distributed with RFT in GT4.
 * <p>
 * The user can tweak the options to the rft client by specifying the properties
 * in the properties files with the prefix <code>vds.transfer.rft</code>.
 * The following table lists all the properties with their prefixes stripped off,
 * that the user can specify in the properties files. The default value is used
 * if the user does not specify a particular property.
 * <p>
 * <table border="1">
 * <tr align="left"><th>property</th><th>default value</th><th>description</th></tr>
 * <tr align="left"><th>host</th>
 *  <td>localhost</td>
 *  <td>the host-ip of the container.</td>
 * </tr>
 * <tr align="left"><th>port</th>
 *  <td>8080</td>
 *  <td>the port at which the container is running.</td>
 * </tr>
 * <tr align="left"><th>binary</th>
 *  <td>true</td>
 *  <td>whether to do transfers in binary mode or not.</td>
 * </tr>
 * <tr align="left"><th>bs</th>
 *  <td>16000</td>
 *  <td>block size in bytes that is transferred.</td>
 * </tr>
 * <tr align="left"><th>tcpbs</th>
 *  <td>16000</td>
 *  <td>specifies the size (in bytes) of the TCP buffer to be used by the
 * underlying ftp data channels</td>
 * </tr>
 * <tr align="left"><th>notpt</th>
 *  <td>false</td>
 *  <td>whether to do normal transfers or not.</td>
 * </tr>
 * <tr align="left"><th>streams</th>
 *  <td>1</td>
 *  <td>specifies the number of parallel data connections that should be used.</td>
 * </tr>
 * <tr align="left"><th>DCAU</th>
 *  <td>true</td>
 *  <td>data channel authentication for ftp transfers.</td>
 * </tr>
 * <tr align="left"><th>processes</th>
 *  <td>1</td>
 *  <td>number of files that you want to transfer at any given point</td>
 * </tr>
 * <tr align="left"><th>retry</th>
 *  <td>3</td>
 *  <td>number of times RFT retries a transfer failed with a non-fatal error.</td>
 * </tr>
 * </table>
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
 *        - the property pegasus.transfer.*.impl must be set to value RFT.
 * </pre>
 *
 * <p>
 * There should be an entry in the transformation catalog with the fully qualified
 * name as <code>globus::rft</code> for all the sites where workflow is run,
 * or on the local site in case of third party transfers.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class RFT extends AbstractMultipleFTPerXFERJob {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "globus";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "rft";

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
    public static final String DERIVATION_NAME = "rft";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION = "RFT Java Client";

    /**
     * The prefix for all the properties this mode requires.
     */
    public static final String PROPERTIES_PREFIX = "pegasus.transfer.rft.";

    /**
     * The key name that denotes the host on which the RFT service is running.
     */
    public static final String HOST_KEY = "host";

    /**
     * The key name that denotes the port on which the RFT service is running.
     */
    public static final String PORT_KEY = "port";

    /**
     * The key name that denotes whether to do the transfers in binary mode or not.
     */
    public static final String BINARY_KEY = "binary";

    /**
     * The key name that denotes the block size in bytes that is to be
     * transferred.
     */
    public static final String BLOCK_SIZE_KEY = "bs";

    /**
     * The key name that denotes the TCP buffer size in bytes.
     */
    public static final String TCP_BUFFER_SIZE_KEY = "tcpbs";

    /**
     * The key name that denotes whether to use TPT (third party transfer) or not.
     */
    public static final String NO_TPT_KEY = "notpt";

    /**
     * The key name that denotes the number of parallel streams to be used.
     */
    public static final String STREAMS_KEY = "streams";

    /**
     * The key name that denotes whether to use Data Channel Authentication or not.
     */
    public static final String DCAU_KEY = "DCAU";

    /**
     * The key name that denotes the number of files to be transferred at any
     * given time.
     */
    public static final String PROCESSES_KEY = "processes";

    /**
     * The key name that denotes the maximum number of retries that are made
     * in case of failure.
     */
    public static final String RETRY_KEY = "retry";

    /**
     * The properties object holding all the RFT specific properties specified
     * by the user in the properties file.
     */
    private Properties mRFTProps;


    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param properties  the properties object.
     * @param options     the options passed to the Planner.
     */
    public RFT(PegasusProperties properties,PlannerOptions options){
        super(properties,options);
        mRFTProps = mProps.matchingSubset(PROPERTIES_PREFIX,false);
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
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. This always returns true, indicating
     * transfers can only be done in a third party transfer mode.
     *
     * @return true
     */
    public boolean useThirdPartyTransferAlways(){
        return true;
    }

    /**
     * Constructs the arguments to the transfer executable that need to be
     * passed to the executable referred to in this transfer mode. Since the
     * rft client is run on the submit host, the path to the input file
     * to the rft client is given, instead of passing it through condor
     * files.
     *
     * @param job   the object containing the transfer node.
     *
     * @return  the argument string
     */
    protected String generateArgumentString(TransferJob job){
        File f = new File(mPOptions.getSubmitDirectory(),job.stdIn);
        StringBuffer sb = new StringBuffer();
        sb.append(" -h ").append(mRFTProps.getProperty(HOST_KEY,"localhost")).
           append(" -r ").append(mRFTProps.getProperty(PORT_KEY,"8080")).
           append(" -f ").append(f.getAbsolutePath());

        return sb.toString();
    }


    /**
     * Resets the STDIN of the transfer job to null, as the
     * input file is not being sent to the remote sides. There should be a
     * generic function prepareIPFile to do this.
     *
     *
     * @job  the <code>TransferJob</code> that has been created.
     */
    public void postProcess( TransferJob job ){
        File f = new File( mPOptions.getSubmitDirectory(), job.getStdIn() );
        //add condor key transfer_input_files to transfer the file
        job.condorVariables.addIPFileForTransfer( f.getAbsolutePath() );
        job.setStdIn( "" );
    }


    /**
     * Writes to a file on the submit host, that is passed to the rft-client
     * as input. The rft-client is always run on the submit host, and hence
     * can access the file.
     *
     * @param writer    the writer to the stdin file.
     * @param files    Collection of <code>FileTransfer</code> objects containing
     *                 the information about sourceam fin and destURL's.
     *
     */
    protected void writeJumboStdIn(FileWriter writer, Collection files) throws
        Exception {
        //write out the fixed header
        writer.write("#RFT input file generated by VDS\n");
        writer.write("#true = binary false=ascii\n");
        writer.write(mRFTProps.getProperty(BINARY_KEY,"true"));
        writer.write("\n");
        writer.write("#Block Size in Bytes\n");
        writer.write(mRFTProps.getProperty(BLOCK_SIZE_KEY,"16000"));
        writer.write("\n");
        writer.write("#TCP Buffer Sizes in Bytes\n");
        writer.write(mRFTProps.getProperty(TCP_BUFFER_SIZE_KEY,"16000"));
        writer.write("\n");
        writer.write("#NO tpt (Third Party Transfer\n");
        writer.write(mRFTProps.getProperty(NO_TPT_KEY,"false"));
        writer.write("\n");
        writer.write("#Number of parallel streams\n");
        writer.write(mRFTProps.getProperty(STREAMS_KEY,"1"));
        writer.write("\n");
        writer.write("#Data Channel Authentication (DCAU)\n");
        writer.write(mRFTProps.getProperty(DCAU_KEY,"true"));
        writer.write("\n");
        writer.write("#Concurrency of the request\n");
        writer.write(mRFTProps.getProperty(PROCESSES_KEY,"1"));
        writer.write("\n");
        writer.write("#Grid Subject Name of Source Grid FTP Server\n");
        writer.write("null");
        writer.write("\n");
        writer.write("#Grid Subject Name of Destination Grid FTP Server\n");
        writer.write("null");
        writer.write("\n");
        writer.write("#Transfer all or none of the transfers\n");
        writer.write("true");
        writer.write("\n");
        writer.write("#Maximum number of retries\n");
        writer.write(mRFTProps.getProperty(RETRY_KEY,"3"));
        writer.write("\n");

        //iterating thru all the FileTransfers
        writer.write("#Source and Destination URLS\n");
        for (Iterator it = files.iterator();it.hasNext();) {
            FileTransfer ft = (FileTransfer)it.next();
            //the FileTransfer object writes out in T2 compatible format
            writer.write(ft.getSourceURL().getValue());
            writer.write("\n");
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
}
