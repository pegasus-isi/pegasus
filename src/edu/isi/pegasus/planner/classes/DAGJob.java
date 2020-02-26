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
package edu.isi.pegasus.planner.classes;

/**
 * This is a data class that stores the contents of the DAG job in a DAX conforming to schema 3.0 or
 * higher.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DAGJob extends Job {

    /** The prefix to be attached for the DAX jobs */
    public static final String JOB_PREFIX = "subdag_";

    /** The DAG LFN. */
    private String mDAGLFN;

    /** The DAG File that the job refers to. */
    private String mDAGFile;

    /** The directory in which the DAG needs to execute. */
    private String mDirectory;

    /** The default constructor. */
    public DAGJob() {
        super();
        mDAGFile = null;
        mDirectory = null;
        this.setJobType(Job.DAG_JOB);
    }

    /**
     * The overloaded construct that constructs a DAG job by wrapping around the <code>Job</code>
     * job.
     *
     * @param job the original job description.
     */
    public DAGJob(Job job) {
        super(job);
        mDAGFile = null;
        this.setJobType(Job.DAG_JOB);
    }

    /**
     * Sets the DAG file LFN
     *
     * @param lfn the LFN of the DAG file.
     */
    public void setDAGLFN(String lfn) {
        mDAGLFN = lfn;
    }

    /**
     * Returns the lfn for the DAGFile the job refers to.
     *
     * @return the lfn
     */
    public String getDAGLFN() {
        return mDAGLFN;
    }

    /**
     * Sets the DAG file
     *
     * @param file the path to the DAG file.
     */
    public void setDAGFile(String file) {
        mDAGFile = file;
    }

    /**
     * Returns the DAGFile the job refers to.
     *
     * @return dag file
     */
    public String getDAGFile() {
        return mDAGFile;
    }

    /**
     * Sets the directory in which the dag needs to execute.
     *
     * @param directory the directory where dag needs to execute
     */
    public void setDirectory(String directory) {
        mDirectory = directory;
    }

    /**
     * Returns the directory the job refers to.
     *
     * @return the directory.
     */
    public String getDirectory() {
        return mDirectory;
    }

    /**
     * Generates a name for the job that serves as the primary id for the job
     *
     * @param prefix any prefix that needs to be applied while constructing the job name
     * @return the id for the job
     */
    public String generateName(String prefix) {
        StringBuffer sb = new StringBuffer();

        // prepend a job prefix to job if required
        if (prefix != null) {
            sb.append(prefix);
        }

        String lfn = this.getDAGLFN();
        String lid = this.getLogicalID();
        if (lfn == null || this.getLogicalID() == null) {
            // sanity check
            throw new RuntimeException(
                    "Generate name called for job before setting the DAGLFN/Logicalid"
                            + lfn
                            + ","
                            + lid);
        }

        if (lfn.contains(".")) {
            lfn = lfn.substring(0, lfn.lastIndexOf("."));
        }

        sb.append(DAGJob.JOB_PREFIX).append(lfn).append("_").append(lid);

        return sb.toString();
    }

    /**
     * Returns a textual description of the DAG Job.
     *
     * @return the textual description.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer(super.toString());

        return sb.toString();
    }

    /**
     * Returns a new copy of the Object. The implementation is faulty. There is a shallow copy for
     * the profiles. That is the clone retains references to the original object.
     *
     * @return Object
     */
    public Object clone() {
        DAGJob newJob = new DAGJob((Job) super.clone());
        newJob.setDAGLFN(this.getDAGLFN());
        newJob.setDAGFile(this.getDAGFile());
        newJob.setDirectory(this.getDirectory());
        return newJob;
    }
}
