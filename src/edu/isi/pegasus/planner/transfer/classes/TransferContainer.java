/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.transfer.classes;

import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * A container class for storing the name of the transfer job, the list of file transfers that the
 * job is responsible for.
 *
 * @author Karan Vahi
 */
public class TransferContainer {

    /** The name of the transfer job. */
    private String mTXName;

    /** The name of the registration job. */
    private String mRegName;

    /** Collection of compute jobs this transfer container is responsible for */
    private Collection<Job> mComputeJobs;

    /**
     * The collection of <code>FileTransfer</code> objects containing the transfers the job is
     * responsible for.
     */
    private Collection<FileTransfer> mFileTXList;

    /**
     * The collection of <code>FileTransfer</code> objects containing the files that need to be
     * registered.
     */
    private Collection<FileTransfer> mRegFiles;

    /** The type of the transfers the job is responsible for. */
    private int mTransferType;

    /** The default constructor. */
    public TransferContainer() {
        mTXName = null;
        mRegName = null;
        mFileTXList = new LinkedList();
        mRegFiles = new LinkedList();
        // need a set to filter out duplicates
        mComputeJobs = new HashSet();
        mTransferType = Job.STAGE_IN_JOB;
    }

    /**
     * Sets the name of the transfer job.
     *
     * @param name the name of the transfer job.
     */
    public void setTXName(String name) {
        mTXName = name;
    }

    /**
     * Sets the name of the registration job.
     *
     * @param name the name of the transfer job.
     */
    public void setRegName(String name) {
        mRegName = name;
    }

    /**
     * Adds a file transfer to the underlying collection.
     *
     * @param transfer the <code>FileTransfer</code> containing the information about a single
     *     transfer.
     */
    public void addTransfer(FileTransfer transfer) {
        mFileTXList.add(transfer);
    }

    /**
     * Adds a file transfer to the underlying collection.
     *
     * @param files collection of <code>FileTransfer</code>.
     */
    public void addTransfer(Collection<FileTransfer> files) {
        mFileTXList.addAll(files);
    }

    /**
     * Adds a single file for registration.
     *
     * @param file
     */
    public void addRegistrationFiles(FileTransfer file) {
        mRegFiles.add(file);
    }

    /**
     * Adds a Collection of File transfer to the underlying collection of files to be registered.
     *
     * @param files collection of <code>FileTransfer</code>.
     */
    public void addRegistrationFiles(Collection<FileTransfer> files) {
        mRegFiles.addAll(files);
    }

    /**
     * Add associated compute job name
     *
     * @param job
     */
    public void addComputeJob(Job job) {
        this.mComputeJobs.add(job);
    }

    /**
     * Sets the transfer type for the transfers associated.
     *
     * @param type type of transfer.
     */
    public void setTransferType(int type) {
        mTransferType = type;
    }

    /**
     * Returns the name of the transfer job.
     *
     * @return name of the transfer job.
     */
    public String getTXName() {
        return mTXName;
    }

    /**
     * Returns the name of the registration job.
     *
     * @return name of the registration job.
     */
    public String getRegName() {
        return mRegName;
    }

    /**
     * Returns the collection of transfers associated with this transfer container.
     *
     * @return a collection of <code>FileTransfer</code> objects.
     */
    public Collection<FileTransfer> getFileTransfers() {
        return mFileTXList;
    }

    /**
     * Returns the collection of registration files associated with this transfer container.
     *
     * @return a collection of <code>FileTransfer</code> objects.
     */
    public Collection<FileTransfer> getRegistrationFiles() {
        return mRegFiles;
    }

    /**
     * Returns the collection of compute jobs associated with this transfer container.
     *
     * @return a collection of <code>Job</code> objects.
     */
    public Collection<Job> getAssociatedComputeJobs() {
        return this.mComputeJobs;
    }
}
