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
package edu.isi.pegasus.planner.selector.site;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * This ends up scheduling the jobs in a round robin manner. In order to avoid starvation, the jobs
 * are scheduled in a round robin manner per level, and the queue is initialised for each level.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class RoundRobin extends AbstractPerJob {

    /**
     * The current level in the abstract workflow. It is level that is designated by Chimera while
     * constructing the graph bottom up.
     */
    private int mCurrentLevel;

    /**
     * The list of pools that have been given by the user at run time or has been authenticated
     * against. At present these are the same as the list of pools that is passed for site selection
     * at each function.
     */
    private java.util.LinkedList mExecPools;

    /** The default constructor. Not to be used. */
    public RoundRobin() {
        mCurrentLevel = -1;
    }

    /**
     * Returns a brief description of the site selection techinque implemented by this class.
     *
     * @return String
     */
    public String description() {
        String st = "Round Robin Scheduling per level of the workflow";
        return st;
    }

    /**
     * Maps a job in the workflow to an execution site.
     *
     * @param job the job to be mapped.
     * @param sites the list of <code>String</code> objects representing the execution sites that
     *     can be used.
     */
    public void mapJob(Job job, List sites) {

        NameValue current;
        NameValue next;

        if (mExecPools == null) {
            initialiseList(sites);
        }

        if (job.level != mCurrentLevel) {
            // reinitialize stuff
            System.out.println("Job " + job.getID() + " Change in level to " + job.level);
            System.out.println("execution sites " + listToString(mExecPools));
            mCurrentLevel = job.level;

            for (ListIterator it = mExecPools.listIterator(); it.hasNext(); ) {
                ((NameValue) it.next()).setValue(0);
            }
        }

        // go around the list and schedule it to the first one where it can
        String mapping = null;
        for (ListIterator it = mExecPools.listIterator(); it.hasNext(); ) {
            // System.out.println( "List is " + listToString( mExecPools ) );

            current = (NameValue) it.next();
            // check if job can run on pool
            if (mTCMapper.isSiteValid(
                    job.namespace, job.logicalName, job.version, current.getName())) {
                mapping = current.getName();
                // update the the number of times used and place it at the
                // correct position in the list
                current.increment();

                // the current element stays at it's place if it is the only one
                // in the list or it's value is less than the next one.
                if (it.hasNext()) {
                    next = (NameValue) it.next();
                    if (current.getValue() <= next.getValue()) {
                        it.previous();
                        continue;
                    } else {
                        // current's value is now greater than the next
                        current = (NameValue) it.previous();
                        current = (NameValue) it.previous();
                    }
                }
                it.remove();
                // System.out.println( "List after removal of " + current + " is "  + listToString(
                // mExecPools ) );

                // now go thru the list and insert in the correct position
                while (it.hasNext()) {
                    next = (NameValue) it.next();

                    if (current.getValue() <= next.getValue()) {
                        // current has to be inserted before next
                        next = (NameValue) it.previous();
                        break;
                    }
                }
                // current goes to the current position or the end of the list
                it.add(current);
                break;
            } else {
                mLogger.log(
                        "Job " + job.getName() + " cannot be mapped to site " + current,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }
        }

        // means no pool has been found to which the job could be mapped to.
        mLogger.log(
                "[RoundRobin Site Selector] Mapped job " + job.getID() + " to site " + mapping,
                LogManager.DEBUG_MESSAGE_LEVEL);
        job.setSiteHandle(mapping);
    }

    /**
     * It initialises the internal list. A node in the list corresponds to a pool that can be used,
     * and has the value associated with it which is the number of jobs in the current level have
     * been scheduled to it.
     *
     * @param pools List
     */
    private void initialiseList(List pools) {
        if (mExecPools == null) {
            mExecPools = new java.util.LinkedList();

            Iterator it = pools.iterator();
            while (it.hasNext()) {
                mExecPools.add(new NameValue((String) it.next(), 0));
            }
        }
    }

    private String listToString(List elements) {
        StringBuilder sb = new StringBuilder();
        sb.append("size -> ").append(elements.size()).append(" ");
        for (Object element : elements) {
            sb.append(element).append(",");
        }
        return sb.toString();
    }

    /**
     * A inner name value class that associates a string with an int value. This is used to populate
     * the round robin list that is used by this scheduler.
     */
    class NameValue {
        /** Stores the name of the pair (the left handside of the mapping). */
        private String name;

        /** Stores the corresponding value to the name in the pair. */
        private int value;

        /** The default constructor which initialises the class member variables. */
        public NameValue() {
            name = new String();
            value = -1;
        }

        /**
         * Initialises the class member variables to the values passed in the arguments.
         *
         * @param name corresponds to the name in the NameValue pair
         * @param value corresponds to the value for the name in the NameValue pair
         */
        public NameValue(String name, int value) {
            this.name = name;
            this.value = value;
        }

        /**
         * The set method to set the value.
         *
         * @param value int
         */
        public void setValue(int value) {
            this.value = value;
        }

        /**
         * Returns the value associated with this pair.
         *
         * @return int
         */
        public int getValue() {
            return this.value;
        }

        /**
         * Returns the key of this pair, i.e the left hand side of the mapping.
         *
         * @return String
         */
        public String getName() {
            return this.name;
        }

        /** Increments the int value by one. */
        public void increment() {
            value += 1;
        }

        /**
         * Returns a copy of this object.
         *
         * @return Object
         */
        public Object clone() {
            NameValue nv = new NameValue(this.name, this.value);
            return nv;
        }

        /**
         * Writes out the contents of the class to a String in form suitable for displaying.
         *
         * @return String
         */
        public String toString() {
            String str = name + "-->" + value;

            return str;
        }
    }
}
