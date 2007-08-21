/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */

package org.griphyn.cPlanner.selector.site.heft;

/**
 * A data class that is used to simulate a processor on a site.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Processor {

    /**
     * The start time of the current scheduled job.
     */
    private long mStartTime;

    /**
     * The end time of the current scheduled job.
     */
    private long mEndTime;


    /**
     * The default constructor.
     */
    public Processor() {
        mStartTime = 0;
        mEndTime   = 0;
    }

    /**
     * Returns the earliest time the processor is available for scheduling
     * a job.  It is non insertion based scheduling policy.
     *
     * @param start     the time at which to start the search.
     *
     * @return long
     */
    public long getAvailableTime( long start ){
       return ( mEndTime > start )? mEndTime : start;
    }


    /**
     * Schedules a job on to a processor.
     *
     * @param start    the start time of the job.
     * @param end      the end time for the job
     */
    public void scheduleJob( long start, long end ){
        mStartTime = start;
        mEndTime   = end;
    }
}
