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
package edu.isi.pegasus.planner.selector.site.heft;

import edu.isi.pegasus.planner.partitioner.graph.Bag;

/**
 * A data class that implements the Bag interface and stores the extra information that is required
 * by the HEFT algorithm for each node.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class HeftBag implements Bag {

    /** Array storing the names of the attributes that are stored with the site. */
    public static final String HEFTINFO[] = {
        "avg-compute-time",
        "downward-rank",
        "upward-rank",
        "start-time",
        "end-time",
        "scheduled-site"
    };

    /**
     * The constant to be passed to the accessor functions to get or set the average compute time
     * for the node.
     */
    public static final Integer AVG_COMPUTE_TIME = new Integer(0);

    /**
     * The constant to be passed to the accessor functions to get or set the downward rank for a
     * node.
     */
    public static final Integer DOWNWARD_RANK = new Integer(1);

    /**
     * The constant to be passed to the accessor functions to get or set the upward rank for a node.
     */
    public static final Integer UPWARD_RANK = new Integer(2);

    /**
     * The constant to be passed to the accessor functions to get or set the actual start time for a
     * job.
     */
    public static final Integer ACTUAL_START_TIME = new Integer(3);

    /**
     * The constant to be passed to the accessor functions to get or set the actual end time for a
     * job.
     */
    public static final Integer ACTUAL_FINISH_TIME = new Integer(4);

    /** The site where the job is scheduled. */
    public static final Integer SCHEDULED_SITE = new Integer(5);

    /** The average compute time for a node. */
    private float mAvgComputeTime;

    /** The downward rank for a node. */
    private float mDownwardRank;

    /** The upward rank for a node. */
    private float mUpwardRank;

    /** The estimated start time for a job. */
    private long mStartTime;

    /** The estimated end time for a job. */
    private long mEndTime;

    /** The site where a job is scheduled to run. */
    private String mScheduledSite;

    /** The default constructor. */
    public HeftBag() {
        mAvgComputeTime = 0;
        mDownwardRank = 0;
        mUpwardRank = 0;
        mStartTime = 0;
        mEndTime = 0;
        mScheduledSite = "";
    }

    /**
     * Adds an object to the underlying bag corresponding to a particular key.
     *
     * @param key the key with which the value has to be associated.
     * @param value the value to be associated with the key.
     * @return boolean indicating if insertion was successful.
     */
    public boolean add(Object key, Object value) {
        boolean result = true;

        int k = getIntValue(key);

        float fv = 0;
        long lv = 0;

        // short cut for scheduled site
        if (k == this.SCHEDULED_SITE) {
            mScheduledSite = (String) value;
            return result;
        }

        // parse the value correctly
        switch (k) {
            case 0:
            case 1:
            case 2:
                fv = ((Float) value).floatValue();
                break;
            case 3:
            case 4:
                lv = ((Long) value).longValue();
                break;
            default:
        }

        switch (k) {
            case 0:
                this.mAvgComputeTime = fv;
                break;

            case 1:
                this.mDownwardRank = fv;
                break;

            case 2:
                this.mUpwardRank = fv;
                break;

            case 3:
                this.mStartTime = lv;
                break;

            case 4:
                this.mEndTime = lv;
                break;

            default:
                result = false;
        }

        return result;
    }

    /**
     * Returns true if the namespace contains a mapping for the specified key.
     *
     * @param key The key that you want to search for in the bag.
     * @return boolean
     */
    public boolean containsKey(Object key) {

        int k = -1;
        try {
            k = ((Integer) key).intValue();
        } catch (Exception e) {
        }

        return (k >= this.AVG_COMPUTE_TIME.intValue() && k <= this.UPWARD_RANK.intValue());
    }

    /**
     * Returns an objects corresponding to the key passed.
     *
     * @param key the key corresponding to which the objects need to be returned.
     * @return the object that is found corresponding to the key or null.
     */
    public Object get(Object key) {

        int k = getIntValue(key);

        switch (k) {
            case 0:
                return this.mAvgComputeTime;

            case 1:
                return this.mDownwardRank;

            case 2:
                return this.mUpwardRank;

            case 3:
                return this.mStartTime;

            case 4:
                return this.mEndTime;

            case 5:
                return this.mScheduledSite;

            default:
                throw new RuntimeException(
                        " Wrong Heft key. Please use one of the predefined key types " + key);
        }
    }

    /**
     * A convenience method to get the intValue for the object passed.
     *
     * @param key the key to be converted
     * @return the int value if object an integer, else -1
     */
    private int getIntValue(Object key) {

        int k = -1;
        try {
            k = ((Integer) key).intValue();
        } catch (Exception e) {
        }

        return k;
    }
}
