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
 * An abstract implementation of the Profile Aggregators.
 *
 * @author Karan Vahi
 * @version $Revision: 1.1 $
 */
public abstract class Abstract implements Aggregator{



    /**
     * Formats the String value as an integer. If the String is NaN then the
     * default value is assigned.
     *
     * @param value the value to be converted to integer.
     * @param dflt  the default value to be used in case value is NaN or null.
     *
     * @return the integer value
     *
     * @throws NumberFormatException in the case when default value cannot be
     *                               converted to an int.
     */
    protected int parseInt( String value, String dflt ) throws NumberFormatException{
        int val = Integer.parseInt( dflt );

        //check for null and apply default
        if( value == null ) return val;

        //try to parse the value
        try{ val = Integer.parseInt( value ); } catch( Exception e ){ /*ignore for now*/ }

        return val;
    }

}

