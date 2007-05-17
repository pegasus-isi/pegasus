/**
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

package org.griphyn.cPlanner.partitioner;

import java.util.List;

/**
 * This interface defines the callback calls from the partitioners. The
 * partitioners call out to the appropriate callback methods as and when they
 * determine that a partition has been constructed.
 *
 *
 * @author Karan Vahi
 *
 * @version $Revision: 1.3 $
 */

public interface Callback {

    /**
     * Callback for when a partitioner determines that partition has been
     * constructed.
     *
     * @param partition the constructed partition.
     */
    public void cbPartition( Partition partition ) ;


    /**
     * Callback for when a partitioner determines the relations between partitions
     * that it has previously constructed.
     *
     * @param child    the id of a partition.
     * @param parents  the list of <code>String</code> objects that contain
     *                 the id's of the parents of the partition.
     */
    public void cbParents( String child, List parents );

    /**
     * Callback for the partitioner to signal that it is done with the processing.
     *
     *
     */
    public void cbDone();

}
