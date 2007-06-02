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
package org.griphyn.cPlanner.transfer;

import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import java.util.Collection;

/**
 * The interface defines the functions that a particular Transfer Implementation
 * should implement. The functions deal with the creation of a TransferJob that
 * can transfer files using the transfer tool to which it refers to.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public interface Implementation {


    /**
     * The version number associated with this API of Code Generator.
     */
    public static final String VERSION = "1.3";


    /**
     * The universe that applies for the transfer jobs. Used for querying to the
     * Site Catalog.
     */
    public static final String TRANSFER_UNIVERSE = "transfer";


    /**
     * Sets the callback to the refiner, that has loaded this implementation.
     *
     * @param refiner  the transfer refiner that loaded the implementation.
     */
    public void setRefiner(Refiner refiner);

    /**
     * This constructs the SubInfo object for the transfer node. The transfer is
     * supposed to occur at job execution site. It should lead to the creation
     * of the setup chmod jobs to the workflow, that appear as parents to compute
     * jobs in case the transfer implementation does not preserve the X bit
     * on the file being transferred. This is required for staging of executables
     * as part of the workflow.
     *
     * @param job         the SubInfo object for the job, in relation to which
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
                                         int jobClass
                                         ) ;

    /**
     * Returns a boolean indicating whether the transfer protocol being used by
     * the implementation preserves the X Bit or not while staging. If it does
     * not, then it should extend the Abstract implementation of this interface,
     * that allows for adding of a setup job after the stagein job that changes
     * the X Bit.
     */
    public boolean doesPreserveXBit();


    /**
     * Adds the dirmanager job to the workflow, that do a chmod on the files
     * being staged.
     *
     * @param computeJob     the computeJob for which the files are
     *                       being staged.
     * @param txJobName      the name of the transfer job that is staging the files.
     * @param execFiles      the executable files that are being staged.
     * @param transferClass  the class of transfer job
     *
     * @return boolean indicating whether any XBitJobs were succesfully added or
     *         not.
     */
    public boolean addSetXBitJobs( SubInfo computeJob,
                                   String txJobName,
                                   Collection execFiles,
                                   int transferClass );


    /**
     * Adds the dirmanager job to the workflow, that do a chmod on the files
     * being staged.
     *
     * @param computeJob     the computeJob for which the files are
     *                       being staged.
     * @param txJobName      the name of the transfer job that is staging the files.
     * @param execFiles      the executable files that are being staged.
     * @param transferClass  the class of transfer job
     * @param xbitIndex      index to be used for creating the name of XBitJob.
     *
     * @return boolean indicating whether any XBitJobs were succesfully added or
     *         not.
     */
    public boolean addSetXBitJobs( SubInfo computeJob,
                                   String txJobName,
                                   Collection execFiles,
                                   int transferClass,
                                   int xbitIndex  );




    /**
     * Adds the dirmanager job to the workflow, that do a chmod on the executable
     * files that are being staged. It should be empty for the implementations
     * that preserve the X bit while staging files.
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
//    public boolean addSetXBitJobs(String computeJobName,
//                                  String txJobName,
//                                  Collection execFiles,
//                                  int transferClass);


    /**
     * Generates the name of the setXBitJob , that is unique for the given
     * workflow. If the implementation preserve the X bit, then it should
     * return null.
     *
     * @param name    the name of the compute job for which the executable is
     *                being staged.
     * @param counter the index for the setXBit job.
     *
     * @return the name of the setXBitJob, null in case the implementation
     *         preserves the XBit.
     */
    public String getSetXBitJobName(String name, int counter);

    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry(String siteHandle);

    /**
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. A value of false, results in the
     * direct or peer to peer transfers being done.
     * <p>
     * A value of false does not preclude third party transfers. They still can
     * be done, by setting the property "pegasus.transfer.*.thirdparty.sites".
     *
     * @return boolean indicating whether to always use third party transfers
     *         or not.
     *
     * @see PegasusProperties#getThirdPartySites(String)
     */
    public boolean useThirdPartyTransferAlways();

    /**
     * Applies priorities to the transfer jobs if a priority is specified
     * in the properties file.
     *
     * @param job   the transfer job .
     */
    public void applyPriority(TransferJob job);


    /**
     * Determines if there is a need to transfer proxy for the transfer
     * job or not.  If there is a need to transfer proxy, then the job is
     * modified to create the correct condor commands to transfer the proxy.
     * Proxy is usually transferred if the VDS profile TRANSFER_PROXY is set,
     * or the job is being run in the condor vanilla universe. The proxy is
     * transferred from the submit host (i.e site local). The location is
     * determined from the value of the X509_USER_PROXY profile key associated
     * in the env namespace.
     *
     * @param job   the transfer job .
     *
     * @return boolean true job was modified to transfer the proxy, else
     *                 false when job is not modified.
     */
    public boolean checkAndTransferProxy(TransferJob job);


    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public  String getDescription();

}
