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

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * A data class that models a site as a collection of processors. The number of processors can only
 * be specified in the constructor.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Site {

    /** The number of processors making up a site. */
    private int mNumProcessors;

    /** A list of processors making up the site. */
    private List mProcessors;

    /** The index to the processor that is to be used for scheduling a job. */
    private int mCurrentProcessorIndex;

    /** The logical name assigned to the site. */
    private String mName;

    /**
     * The default constructor.
     *
     * @param name the name to be assigned to the site.
     */
    public Site(String name) {
        mName = name;
        mNumProcessors = 0;
        mProcessors = new LinkedList();
        mCurrentProcessorIndex = 0;
    }

    /**
     * The overloaded constructor.
     *
     * @param name the name to be assigned to the site.
     * @param num the number of processors.
     */
    public Site(String name, int num) {
        mName = name;
        mNumProcessors = num;
        mCurrentProcessorIndex = -1;
        mProcessors = new LinkedList();
    }

    /**
     * Returns the earliest time the site is available for scheduling a job. It is non insertion
     * based scheduling policy.
     *
     * @param start the time at which to start the search.
     * @return long
     */
    public long getAvailableTime(long start) {
        int num = 0;

        // each processor is checked for start of list
        long result = Long.MAX_VALUE;
        long current;
        ListIterator it;
        for (it = mProcessors.listIterator(); it.hasNext(); num++) {
            Processor p = (Processor) it.next();
            current = p.getAvailableTime(start);
            if (current < result) {
                // tentatively schedule a job on the processor
                result = current;
                mCurrentProcessorIndex = num;
            }
        }

        if (result > start && num < mNumProcessors) {
            // tentatively schedule a job to an unused processor as yet.
            result = start;
            mCurrentProcessorIndex = num++;
            // if using a normal iterator
            // could use addLast() method
            it.add(new Processor());
        }

        // sanity check
        if (result == Long.MAX_VALUE) {
            throw new RuntimeException("Unable to scheduled to site");
        }

        return result;
    }

    /**
     * Schedules a job to the site.
     *
     * @param start the start time of the job.
     * @param end the end time for the job
     */
    public void scheduleJob(long start, long end) {
        // sanity check
        if (mCurrentProcessorIndex == -1) {
            throw new RuntimeException(
                    "Invalid State. The job needs to be tentatively scheduled first!");
        }

        Processor p = (Processor) mProcessors.get(mCurrentProcessorIndex);
        p.scheduleJob(start, end);

        // reset the index
        mCurrentProcessorIndex = -1;
    }

    /**
     * Returns the name of the site.
     *
     * @return name of the site.
     */
    public String getName() {
        return mName;
    }

    /**
     * Returns the number of available processors.
     *
     * @return number of available processors.
     */
    public int getAvailableProcessors() {
        return this.mNumProcessors;
    }
}
