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
