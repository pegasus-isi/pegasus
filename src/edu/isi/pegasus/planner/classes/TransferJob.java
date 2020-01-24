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
 * This is a data class that stores the contents of the transfer job that transfers the data. Later
 * on stdin etc, would be stored in it.
 *
 * @author Karan Vahi vahi@isi.edu
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */
public class TransferJob extends Job {

    /**
     * The site at which the transfer jobs would have run, had it been running in a peer 2 peer
     * transfer mode (non third party mode).
     */
    private String mNonTPTSite;

    /** The default constructor. */
    public TransferJob() {
        super();
        mNonTPTSite = null;
    }

    /**
     * The overloaded construct that constructs a GRMS job by wrapping around the <code>Job</code>
     * job.
     *
     * @param job the original job description.
     */
    public TransferJob(Job job) {
        super(job);
        mNonTPTSite = null;
    }

    /**
     * Returns the site at which the job would have run if the transfer job was being run in non
     * third party mode. If the job is run in a non third party mode, the result should be the same
     * as the site where the transfer job has been scheduled.
     *
     * @return the site at which the job would have run in a non third party mode, null if not set.
     */
    public String getNonThirdPartySite() {
        return mNonTPTSite;
    }

    /**
     * Sets the non third party site for the transfer job. This is the site at which the job would
     * have run if the transfer job was being run in non third party mode.
     *
     * @param site the name of the site
     */
    public void setNonThirdPartySite(String site) {
        mNonTPTSite = site;
    }

    /**
     * Returns a textual description of the Transfer Job.
     *
     * @return the textual description.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer(super.toString());
        sb.append("\n").append(" Non TPT Site     :").append(getNonThirdPartySite());

        return sb.toString();
    }

    /**
     * Returns a new copy of the Object. The implementation is faulty. There is a shallow copy for
     * the profiles. That is the clone retains references to the original object.
     *
     * @return Object
     */
    public Object clone() {
        TransferJob newJob = new TransferJob((Job) super.clone());
        newJob.setNonThirdPartySite(this.getNonThirdPartySite());
        return newJob;
    }
}
