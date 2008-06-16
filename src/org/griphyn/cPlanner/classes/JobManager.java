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


package org.griphyn.cPlanner.classes;

/**
 * This is a data class that is used to store information about a jobmanager and
 * the information that it reports about a remote pool.
 *
 * <p>
 * The various attributes that can be associated with the the server are
 * displayed in the following table.
 *
 * <p>
 * <table border="1">
 * <tr align="left"><th>Attribute Name</th><th>Attribute Description</th></tr>
 * <tr align="left"><th>url</th>
 *  <td>the url string pointing to the jobmanager.</td>
 * </tr>
 * <tr align="left"><th>universe</th>
 *  <td>the VDS universe that is associated with this job. Can be transfer or
 *  vanilla or any other user defined type.</td>
 * </tr>
 * <tr align="left"><th>jobamanager type</th>
 *  <td>remote scheduler type to which the jobmanager talks to.</td>
 * </tr>
 * <tr align="left"><th>idle nodes</th>
 *  <td>the number of idle nodes on the remote resource.</td>
 * </tr>
 * <tr align="left"><th>total nodes</th>
 *  <td>the total number of nodes on the remote resource.</td>
 * </tr>
 * <tr align="left"><th>free memory</th>
 *  <td>the free memory.</td>
 * </tr>
 * <tr align="left"><th>total memory</th>
 *  <td>the total memory</td>
 * </tr>
 * <tr align="left"><th>jobs in queue</th>
 *  <td>the number of jobs in the queue on the remote scheduler.</td>
 * </tr>
 * <tr align="left"><th>running jobs</th>
 *  <td>the number of jobs currently running on the remote site.</td>
 * </tr>
 * <tr align="left"><th>max count</th>
 *  <td>the maximum number of jobs that can be run.</td>
 * </tr>
 * <tr align="left"><th>max cpu time</th>
 *  <td>the max walltime for the jobs on the remote resource.</td>
 * </tr>
 * <tr align="left"><th>os type/th>
 *  <td>the operating system type of the remote machines to which the jobmanager
 *  talks to.</td>
 * </tr>
 * <tr align="left"><th>architecture type</th>
 *  <td>the architecture type of the remote machines to which the jobmanager
 *  talks to.</td>
 * </tr>
 * </table>
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @author Karan Vahi vahi@isi.edu
 *
 * @version $Revision$
 */
public class JobManager {

    /**
     * Array storing the names of the attributes that are stored with the
     * jobmanager.
     */
    public static final String JOBMANAGERINFO[] = {
        "url", "universe", "globus-version", "type", "idle-nodes",
        "total-nodes", "free-mem", "total-mem", "jobs-in-queue", "running-jobs",
        "max-count", "max-cpu-time", "os", "arch"};


    /**
     * The jobmanager type associated with the compute jobs.
     */
    public static final String VANILLA_JOBMANAGER_TYPE = "vanilla";

    /**
     * The jobmanager type associated with the transfer jobs.
     */
    public static final String FORK_JOBMANAGER_TYPE = "transfer";

    /**
     * The constant to be passed to the accessor functions to get or set the url.
     */
    public static final int URL = 0;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * universe.
     */
    public static final int UNIVERSE = 1;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * globus version.
     */
    public static final int GLOBUS_VERSION = 2;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * jobmanager type.
     */
    public static final int JOBMANAGER_TYPE = 3;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * idle nodes.
     */
    public static final int IDLE_NODES = 4;

    /**
     * The constant to be passed to the accessor functions to get or set the total
     * number of nodes.
     */
    public static final int TOTAL_NODES = 5;

    /**
     * The constant to be passed to the accessor functions to get or set the free
     * memory .
     */
    public static final int FREE_MEM = 6;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * total memory.
     */
    public static final int TOTAL_MEM = 7;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * number of the jobs in the queue attribute.
     */
    public static final int JOBS_IN_QUEUE = 8;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * number of running jobs attribute.
     */
    public static final int RUNNING_JOBS = 9;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * maximum number of jobs that can be in the queue.
     */
    public static final int MAX_COUNT = 10;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * maxwalltime for the jobs.
     */
    public static final int MAX_CPU_TIME = 11;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * operating system type of the remote machines to which the jobmanager talks
     * to.
     */
    public static final int OS_TYPE = 12;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * architecture type of the remote machines to which the jobmanager talks to.
     */
    public static final int ARCH_TYPE = 13;

    /**
     * The total memory that the jobmanager reports.
     */
    private String mTotalMem;

    /**
     * The free memory that the jobmanager reports.
     */
    private String mFreeMem;

    /**
     * The total number of nodes that the jobmanager reports are on the remote
     * site.
     */
    private String mTotalNodes;

    /**
     * The total number of idle nodes that the jobmanager reports are on the
     * remote site.
     */
    private String mIdleNodes;

    /**
     * The maximum number of jobs that can be running on the remote site.
     */
    private String mMaxCount;

    /**
     * The max walltime of the jobs that are run on the remote site.
     */
    private String mMaxCPUTime;

    /**
     * The number of jobs in the remote queue at the remote site.
     */
    private String mJobsInQueue;

    /**
     * The number of jobs in the remote queue that are running at the remote site.
     */
    private String mRunningJobs;

    /**
     * The operating system type type of the remote machines to which the
     * jobmanager talks to.
     */
    private String mOSType;

    /**
     * The architecture type of the remote machines to which the jobmanager
     * talks to.
     */
    private String mArchType;

    /**
     * The type of remote scheduler to which the jobmanager talks to.
     */
    private String mJobManagerType;

    /**
     * The url to the jobmanager on the remote site.
     */
    private String mURL;

    /**
     * The VDS universe with which the jobmanager is associated with.
     */
    private String mUniverse;

    /**
     * The globus version that is installed on the remote site.
     */
    private String mGlobusVersion;

    /**
     *  Default constructor for the class.
     */
    public JobManager() {
        //  m_jobmanager_info = new HashMap(14);
        mTotalMem       = null;
        mFreeMem        = null;
        mTotalNodes     = null;
        mIdleNodes      = null;
        mMaxCount       = null;
        mMaxCPUTime     = null;
        mJobsInQueue    = null;
        mRunningJobs    = null;
        mOSType         = null;
        mArchType       = null;
        mJobManagerType = null;
        mURL            = null;
        mUniverse       = null;
        mGlobusVersion  = null;

    }

    /**
     * Sets an attribute associated with the jobmanager.
     *
     * @param key  the attribute key, which is one of the predefined keys.
     * @param value value of the attribute.
     *
     */
    public void setInfo(int key, String value)  {

        switch (key) {

            case 0:
                mURL = value == null ? null : new String(value);
                break;

            case 1:
                mUniverse = value == null ? null : new String(value);
                break;

            case 2:
                mGlobusVersion = value == null ? null :
                    new String(new GlobusVersion(value).
                               getGlobusVersion());
                break;

            case 3:
                mJobManagerType = value == null ? null : new String(value);
                break;

            case 4:
                mIdleNodes = value == null ? null : new String(value);
                break;

            case 5:
                mTotalNodes = value == null ? null : new String(value);
                break;

            case 6:
                mFreeMem = value == null ? null : new String(value);
                break;

            case 7:
                mTotalMem = value == null ? null : new String(value);
                break;

            case 8:
                mJobsInQueue = value == null ? null : new String(value);
                break;

            case 9:
                mRunningJobs = value == null ? null : new String(value);
                break;

            case 10:
                mMaxCount = value == null ? null : new String(value);
                break;

            case 11:
                mMaxCPUTime = value == null ? null : new String(value);
                break;

            case 12:
                mOSType = value == null ? null : new String(value);
                break;

            case 13:
                mArchType = value == null ? null : new String(value);
                break;

            default:
                throw new RuntimeException("Wrong key =" + key +
                    ". Please have one of the prefedefined jobmanager keys");
        }
    }

    /**
     * Returns the attribute value of a particular attribute of the jobmanager.
     *
     * @param key the key/attribute name.
     *
     * @return the attribute value
     */
    public String getInfo(int key) {

        switch (key) {

            case 0:
                return mURL;

            case 1:
                return mUniverse;

            case 2:
                return mGlobusVersion;

            case 3:
                return mJobManagerType;

            case 4:
                return mIdleNodes;

            case 5:
                return mTotalNodes;

            case 6:
                return mFreeMem;

            case 7:
                return mTotalMem;

            case 8:
                return mJobsInQueue;

            case 9:
                return mRunningJobs;

            case 10:
                return mMaxCount;

            case 11:
                return mMaxCPUTime;

            case 12:
                return mOSType;

            case 13:
                return mArchType;

            default:
                throw new RuntimeException("Wrong key=" + key +
                    ". Please have one of the prefedefined jobmanager keys");
        }

    }


    /**
     * Checks if an object is similar to the one referred to by this class.
     * We compare the primary key to determine if it is the same or not.
     *
     * @param o  the object to be compared for equality.
     *
     * @return true if the primary key (url) match.
     *         else false.
     */
    public boolean equals(Object o){
        JobManager jm = (JobManager)o;
        //for the time being only match on url.
        if(/*this.mUniverse == jm.mUniverse && */
           this.mURL.equals(jm.mURL)) {
           return true;
       }
        return false;
    }

    /**
     * Returns the textual description of the  contents of <code>JobManager</code>
     * object in the multiline format.
     *
     * @return the textual description in multiline format.
     */
    public String toMultiLine() {
        String output = "universe";
        if (mUniverse != null) {
            output += " " + mUniverse;
        }
        if (mURL != null) {
            output += " \"" + mURL + "\"";
        }
        if (mGlobusVersion != null) {
            output += " \"" + mGlobusVersion + "\"";
        }
        return output;
    }


    /**
     * Returns the textual description of the  contents of <code>JobManager</code>
     * object.
     *
     * @return the textual description.
     */
    public String toString() {
        String output = "jobmanager";
        if (mUniverse != null) {
            output += " " + mUniverse;
        }
        if (mURL != null) {
            output += " \""+mURL+"\"";
        }
        if (mGlobusVersion != null) {
            output += " \""+mGlobusVersion+"\"";
        }
        if (mUniverse != null) {
            output += " " + JOBMANAGERINFO[UNIVERSE] + "=" + mUniverse;
        }
        if (mURL != null) {
            output += " " + JOBMANAGERINFO[URL] + "=" + mURL;
        }
        if (mGlobusVersion != null) {
            output += " " + JOBMANAGERINFO[GLOBUS_VERSION] + "=" +
                mGlobusVersion;
        }
        if (mJobManagerType != null) {
            output += " " + JOBMANAGERINFO[JOBMANAGER_TYPE] + "=" +
                mJobManagerType;
        }
        if (mOSType != null) {
            output += " " + JOBMANAGERINFO[OS_TYPE] + "=" + mOSType;
        }
        if (mArchType != null) {
            output += " " + JOBMANAGERINFO[ARCH_TYPE] + "=" + mArchType;
        }
        if (mRunningJobs != null) {
            output += " " + JOBMANAGERINFO[RUNNING_JOBS] + "=" + mRunningJobs;
        }
        if (mJobsInQueue != null) {
            output += " " + JOBMANAGERINFO[JOBS_IN_QUEUE] + "=" +
                mJobsInQueue;
        }
        if (mMaxCPUTime != null) {
            output += " " + JOBMANAGERINFO[MAX_CPU_TIME] + "=" + mMaxCPUTime;
        }
        if (mMaxCount != null) {
            output += " " + JOBMANAGERINFO[MAX_COUNT] + "=" + mMaxCount;
        }
        if (mTotalNodes != null) {
            output += " " + JOBMANAGERINFO[TOTAL_NODES] + "=" + mTotalNodes;
        }
        if (mIdleNodes != null) {
            output += " " + JOBMANAGERINFO[IDLE_NODES] + "=" + mIdleNodes;
        }
        if (mTotalMem != null) {
            output += " " + JOBMANAGERINFO[TOTAL_MEM] + "=" + mTotalMem;
        }
        if (mFreeMem != null) {
            output += " " + JOBMANAGERINFO[FREE_MEM] + "=" + mFreeMem;
        }
        output += " )";

        //  System.out.println(output);
        return output;
    }

    /**
     * Returns the XML description of the  contents of <code>JobManager</code>
     * object.
     *
     * @return the xml description.
     */
    public String toXML() {
        String output = "<jobmanager";

        if (mUniverse != null) {
            output += " " + JOBMANAGERINFO[UNIVERSE] + "=\"" + mUniverse +
                "\"";
        }
        if (mURL != null) {
            output += " " + JOBMANAGERINFO[URL] + "=\"" + mURL + "\"";
        }
        if (mGlobusVersion != null) {

            GlobusVersion gv = new GlobusVersion(
                mGlobusVersion);
            output += " major=\"" + gv.getGlobusVersion(GlobusVersion.MAJOR) + "\"" +
                " minor=\"" + gv.getGlobusVersion(GlobusVersion.MINOR) + "\"" +
                " patch=\"" + gv.getGlobusVersion(GlobusVersion.PATCH) + "\"";

        }
        if (mJobManagerType != null) {
            output += " " + JOBMANAGERINFO[JOBMANAGER_TYPE] + "=\"" +
                mJobManagerType + "\"";
        }
        if (mOSType != null) {
            output += " " + JOBMANAGERINFO[OS_TYPE] + "=\"" + mOSType + "\"";
        }
        if (mArchType != null) {
            output += " " + JOBMANAGERINFO[ARCH_TYPE] + "=\"" + mArchType +
                "\"";
        }
        if (mRunningJobs != null) {
            output += " " + JOBMANAGERINFO[RUNNING_JOBS] + "=\"" +
                mRunningJobs + "\"";
        }
        if (mJobsInQueue != null) {
            output += " " + JOBMANAGERINFO[JOBS_IN_QUEUE] + "=\"" +
                mJobsInQueue + "\"";
        }
        if (mMaxCPUTime != null) {
            output += " " + JOBMANAGERINFO[MAX_CPU_TIME] + "=\"" +
                mMaxCPUTime + "\"";
        }
        if (mMaxCount != null) {
            output += " " + JOBMANAGERINFO[MAX_COUNT] + "=\"" + mMaxCount +
                "\"";
        }
        if (mTotalNodes != null) {
            output += " " + JOBMANAGERINFO[TOTAL_NODES] + "=\"" + mTotalNodes +
                "\"";
        }
        if (mIdleNodes != null) {
            output += " " + JOBMANAGERINFO[IDLE_NODES] + "=\"" + mIdleNodes +
                "\"";
        }
        if (mTotalMem != null) {
            output += " " + JOBMANAGERINFO[TOTAL_MEM] + "=\"" + mTotalMem +
                "\"";
        }
        if (mFreeMem != null) {
            output += " " + JOBMANAGERINFO[FREE_MEM] + "=\"" + mFreeMem +
                "\"";
        }
        output += " />";
        return output;
    }


}
