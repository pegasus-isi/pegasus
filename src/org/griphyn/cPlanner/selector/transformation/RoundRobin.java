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

import java.util.LinkedList;
import java.util.List;

/**
 * This implementation of the Selector select a transformation from a list in a round robin fashion.
 *
 * @author Gaurang Mehta
 * @version $Revision: 1.3 $
 */
public class RoundRobin
    extends TransformationSelector {

    private LinkedList tclist;

    public RoundRobin() {

    }

    /**
     *
     * @param tcentries List
     * @return TransformationCatalogEntry
     */
    public List getTCEntry( List tcentries ) {

        return null;
    }
}
