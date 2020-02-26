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

package org.griphyn.vdl.util;

import org.griphyn.vdl.classes.*;

/**
 * This class returns pre-defined type values given the corresponding strings defined in the XML
 * schema. It is used by <code>VDLContentHandler</code> to convert string-valued type in XML
 * document to number-valued type in java objects.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLContentHandler
 */
public class VDLType {
    /**
     * Get the container type, it is either SCALAR or LIST
     *
     * @param container is the containerType string
     * @return the constant corresponded to the container type
     * @see org.griphyn.vdl.classes.Value
     */
    public static int getContainerType(String container) {
        if (container.equals("list")) return Value.LIST;
        else return Value.SCALAR;
    }

    /**
     * Get the linkage type of a LFN, which can be INPUT, OUTPUT, INOUT and NONE.
     *
     * @param link is the linkType string
     * @return the constant corresponded to the string
     * @see org.griphyn.vdl.classes.LFN
     */
    public static int getLinkType(String link) {
        if (link.equals("input")) return LFN.INPUT;

        if (link.equals("output")) return LFN.OUTPUT;

        if (link.equals("inout")) return LFN.INOUT;

        return LFN.NONE;
    }

    /**
     * Get the constant for Condor universe
     *
     * @param universe is the string for condor universe
     * @return the constant corresponded to the string
     * @see org.griphyn.vdl.classes.Executable
     */
    public static int getUniverse(String universe) {
        if (universe.equals("standard")) return Executable.CONDOR_STANDARD;
        if (universe.equals("scheduler")) return Executable.CONDOR_SCHEDULER;
        if (universe.equals("globus")) return Executable.CONDOR_GLOBUS;
        if (universe.equals("pvm")) return Executable.CONDOR_PVM;
        if (universe.equals("mpi")) return Executable.CONDOR_MPI;

        return Executable.CONDOR_VANILLA;
    }
}
