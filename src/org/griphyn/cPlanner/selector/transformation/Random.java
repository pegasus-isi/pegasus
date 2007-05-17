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
package org.griphyn.cPlanner.selector.transformation;

import org.griphyn.cPlanner.selector.TransformationSelector;

import org.griphyn.cPlanner.common.LogManager;

import java.util.ArrayList;
import java.util.List;

/**
 * This implemenation of the TCSelector selects a random
 * TransformationCatalogEntry from a List of entries.
 *
 * @author Gaurang Mehta
 * @version $Revision: 1.3 $
 */
public class Random
    extends TransformationSelector {
    public Random() {
    }

    /**
     * This method randomly selects one of the records from numerous valid
     * Transformation Catalog Entries returned by the TCMapper.
     *
     * @param tcentries List TransformationCatalogEntry objects returned by the TCMapper.
     * @return TransformationCatalogEntry Single TransformationCatalogEntry object
     */
    public List getTCEntry( List tcentries ) {
        int no_of_entries = tcentries.size();
        int recSelected = new Double( Math.random() * no_of_entries ).intValue();
        String message = "Random TC Record selected is " + ( recSelected + 1 ) +
            " amongst " + no_of_entries + " possible";
        mLogger.log( message,LogManager.DEBUG_MESSAGE_LEVEL);
        List result = new ArrayList( 1 );
        result.add( tcentries.get( recSelected ) );
        return result;
    }

}
