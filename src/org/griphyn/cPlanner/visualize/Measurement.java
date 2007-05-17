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

package org.griphyn.cPlanner.visualize;

import java.util.Date;

/**
 * An empty interface that is the super interface for all measuremnts we take
 * from the kickstart records.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public interface Measurement {

    /**
     * Returns the job for which the measurement was taken.
     *
     * @return the name of the job.
     */
    public String getJobName();

    /**
     * Returns the time at which the measurement was taken.
     *
     * @return  the Date object representing the time.
     */
    public Date getTime();

    /**
     * Returns the value of the measurement.
     *
     * @return the value.
     */
    public Object getValue();


    /**
     * Sets the job for which the measurement was taken.
     *
     * @param sets the name of the job.
     */
    public void setJobName( String name );

    /**
     * Sets the time at which the measurement was taken.
     *
     * @param time  the Date object representing the time.
     */
    public void setTime( Date time );

    /**
     * Sets the value of the measurement.
     *
     * @param value the value to be associated with measurement.
     */
    public void setValue( Object value );

}