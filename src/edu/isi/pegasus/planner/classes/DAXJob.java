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

import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.common.PegasusProperties;

/**
 * This is a data class that stores the contents of the DAX job in a DAX conforming to schema 3.0 or
 * higher.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DAXJob extends Job {

    /** The prefix to be attached for the DAX jobs */
    public static final String JOB_PREFIX = "subdax_";

    /**
     * The name of the Replica Catalog Implementer that is used to write out the output ma file in
     * the submit directory.
     */
    public static final String OUTPUT_MAPPER_IMPLEMENTOR = "FlushedCache";

    /** suffix to use for generating the output file map */
    private static final String OUTPUT_MAP_SUFFIX = ".output.map";

    /** The DAX LFN. */
    private String mDAXLFN;

    /** The DAX File that the job refers to. */
    private String mDAXFile;

    /** The directory in which the DAX needs to execute. */
    private String mDirectory;

    /**
     * Handle to the to the backend that serves as an output mapper for the sub workflow that is run
     * as a result of executing this DAXJob
     */
    private ReplicaCatalog mOutputMapperBackend;

    /** The corresponding backend path */
    private String mOutputMapperBackendPath;

    /** The default constructor. */
    public DAXJob() {
        super();
        mDAXFile = null;
        mDirectory = null;
        this.setJobType(Job.DAX_JOB);
    }

    /**
     * The overloaded construct that constructs a DAX job by wrapping around the <code>Job</code>
     * job.
     *
     * @param job the original job description.
     */
    public DAXJob(Job job) {
        super(job);
        mDAXFile = null;
        this.setJobType(Job.DAX_JOB);
    }

    /**
     * Sets the DAX file LFN
     *
     * @param lfn the LFN of the DAX file.
     */
    public void setDAXLFN(String lfn) {
        mDAXLFN = lfn;
    }

    /**
     * Returns the lfn for the DAXFile the job refers to.
     *
     * @return the lfn
     */
    public String getDAXLFN() {
        return mDAXLFN;
    }

    /**
     * Sets the DAX file
     *
     * @param file the path to the DAX file.
     */
    public void setDAXFile(String file) {
        mDAXFile = file;
    }

    /**
     * Returns the DAXFile the job refers to.
     *
     * @return dag file
     */
    public String getDAXFile() {
        return mDAXFile;
    }

    /**
     * Add an output file location into the output mapper backend.
     *
     * @param bag
     * @param ft
     * @return
     */
    public boolean addOutputFileLocation(PegasusBag bag, FileTransfer ft) {
        // check if the backend is initialized
        if (this.mOutputMapperBackend == null) {
            intializeOutputMapperBackend(bag);
        }
        NameValue<String, String> nv = ft.getDestURL();
        int result = this.mOutputMapperBackend.insert(ft.getLFN(), nv.getValue(), nv.getKey());
        return result == 1;
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

        String lfn = this.getDAXLFN();
        String lid = this.getLogicalID();
        if (lfn == null || this.getLogicalID() == null) {
            // sanity check
            throw new RuntimeException(
                    "Generate name called for job before setting the DAXLFN/Logicalid"
                            + lfn
                            + ","
                            + lid);
        }

        if (lfn.contains(".")) {
            lfn = lfn.substring(0, lfn.lastIndexOf("."));
        }

        sb.append(DAXJob.JOB_PREFIX).append(lfn).append("_").append(lid);

        return sb.toString();
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
     * Returns the relative file path to the associated output mapper backend
     *
     * @return
     */
    public String getOutputMapperBackendPath() {
        return mOutputMapperBackendPath;
    }

    /**
     * Returns a textual description of the DAX Job.
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
        DAXJob newJob = new DAXJob((Job) super.clone());
        newJob.setDAXLFN(this.getDAXLFN());
        newJob.setDAXFile(this.getDAXFile());
        newJob.setDirectory(this.getDirectory());
        return newJob;
    }

    /** Close any output mapper associated with the file */
    public void closeOutputMapper() {
        if (this.mOutputMapperBackend != null) {
            this.mOutputMapperBackend.close();
        }
    }

    /**
     * Initializes the output map file for the DAX job
     *
     * @param bag
     */
    private void intializeOutputMapperBackend(PegasusBag bag) {
        this.mOutputMapperBackendPath =
                this.getFileFullPath(
                        bag.getPlannerOptions().getSubmitDirectory(), DAXJob.OUTPUT_MAP_SUFFIX);

        PegasusBag b = new PegasusBag();
        b.add(PegasusBag.PEGASUS_LOGMANAGER, bag.getLogger());

        // set the properties for initialization
        PegasusProperties p = PegasusProperties.nonSingletonInstance();
        // set the appropriate property to designate path to file
        p.setProperty(ReplicaCatalog.c_prefix, OUTPUT_MAPPER_IMPLEMENTOR);
        p.setProperty(
                ReplicaCatalog.c_prefix + "." + ReplicaCatalog.FILE_KEY, mOutputMapperBackendPath);

        b.add(PegasusBag.PEGASUS_PROPERTIES, p);
        try {
            mOutputMapperBackend = ReplicaFactory.loadInstance(b);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to initialize job output mapper in the Submit Directory  "
                            + mOutputMapperBackendPath,
                    e);
        }
    }
}
