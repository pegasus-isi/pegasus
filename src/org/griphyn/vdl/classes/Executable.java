/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
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
package org.griphyn.vdl.classes;

import java.util.*;

/**
 * This class is a leftover from an earlier version, and now solely here for the purposes of
 * providing the Condor universe constants.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Executable {
    /** Condor vanilla universe to run unmodified jobs. */
    public static final int CONDOR_VANILLA = 0;
    /** Condor standard universe to run condor_compiled jobs. */
    public static final int CONDOR_STANDARD = 1;
    /** Condor scheduler universe to run on the submit host. */
    public static final int CONDOR_SCHEDULER = 2;
    /** Condor globus universe to talk to a GRAM system. */
    public static final int CONDOR_GLOBUS = 3;
    /** Condor PVM universe to do what? */
    public static final int CONDOR_PVM = 4;
    /** Condor Java universe to do what? */
    public static final int CONDOR_JAVA = 5;
    /** Condor MPI universe to do what? */
    public static final int CONDOR_MPI = 6;

    /**
     * Predicate to determine, if an integer is within the valid range for Condor universes.
     *
     * @param x is the integer to test for in-intervall.
     * @return true, if the integer satisfies {@link Executable#CONDOR_VANILLA} &leq; x &leq; {@link
     *     Executable#CONDOR_MPI}, false otherwise.
     */
    public static boolean isInRange(int x) {
        return ((x >= Executable.CONDOR_VANILLA) && (x <= Executable.CONDOR_MPI));
    }

    /**
     * Converts an integer into the symbolic Condor universe represented by the constant.
     *
     * @param x is the integer with the universe to symbolically convert
     * @return a string with the symbolic universe name, or null, if the constant is out of range.
     */
    public static String toString(int x) {
        switch (x) {
            case Executable.CONDOR_VANILLA:
                return "vanilla";
            case Executable.CONDOR_STANDARD:
                return "standard";
            case Executable.CONDOR_SCHEDULER:
                return "scheduler";
            case Executable.CONDOR_GLOBUS:
                return "globus";
            case Executable.CONDOR_PVM:
                return "pvm";
            case Executable.CONDOR_JAVA:
                return "java";
            case Executable.CONDOR_MPI:
                return "mpi";
            default:
                return null;
        }
    }
}
