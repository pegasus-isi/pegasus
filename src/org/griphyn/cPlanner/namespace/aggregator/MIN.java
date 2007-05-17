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
 * An implementation of the Aggregator interface that takes the minimum of the
 * profile values. In the case of either of the profile values not valid
 * integers, the default value is picked up.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class MIN extends Abstract{

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
    public String compute( String oldValue, String newValue, String dflt ){
        int val1 = parseInt( oldValue, dflt );
        int val2 = parseInt( newValue, dflt );

        return ( val2 < val1 )? Integer.toString( val2 ) : Integer.toString( val1 );
    }

}
