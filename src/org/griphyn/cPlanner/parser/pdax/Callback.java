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
package org.griphyn.cPlanner.parser.pdax;

import org.griphyn.cPlanner.partitioner.Partition;

/**
 * This interfaces defines the callback calls from PDAX parsing. A slim
 * and memory-efficient parser of PDAX is expected to call these callbacks.
 *
 * @author Karan Vahi
 * @version $Revision: 1.2 $
 */
public interface Callback {

    /**
     * Callback when the opening tag was parsed. This contains all
     * attributes and their raw values within a map. This callback can
     * also be used to initialize callback-specific resources.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cbDocument(java.util.Map attributes);

    /**
     * Callback for the partition . These partitions are completely
     * assembled, but each is passed separately.
     *
     * @param partition is the PDAX-style partition.
     */
    public void cbPartition(Partition partition);

    /**
     * Callback for child and parent relationships from section 3.
     *
     * @param child is the IDREF of the child element.
     * @param parents is a list of IDREFs of the included parents.
     */
    public void cbParents(String child, java.util.List parents);

    /**
     * Callback when the parsing of the document is done. While this state
     * could also be determined from the return of the invocation of the
     * parser, that return may be hidden in another place of the code.
     * This callback can be used to free callback-specific resources.
     */
    public void cbDone();

}