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

package org.griphyn.cPlanner.namespace.aggregator;


/**
 * An implementation of the Aggregator interface that always takes the
 * new profile value. Updates the old value with the new value.
 *
 * @author Karan Vahi
 * @version $Revision: 1.1 $
 */
public class Update extends Abstract{

    /**
     * Returns the minimum of two values.
     *
     * @param oldValue   the existing value for the profile.
     * @param newValue   the new value being added to the profile.
     * @param dflt       the default value to be used in case the values
     *                   are not of the correct type.
     *
     * @return the computed value as a String.
     */
    public String compute( String oldValue, String newValue, String dflt  ){
        //always return the new value. no sanity checks
        return newValue;
    }
}
