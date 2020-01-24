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
package org.griphyn.vdl.router;

import java.util.*;
import org.griphyn.vdl.classes.Derivation;
import org.griphyn.vdl.util.Logging;

/**
 * This implementation of the arbitration interface prefers the first derivation within the same
 * current working namespace. If there is no such derivation, returns the first one from the
 * collection.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision $
 */
public class PreferNamespace implements Arbiter {
    /**
     * Compares two strings, each of which may be null. If both are null, they are considered equal
     * by this function. This function relies on the fact that equals() can deal with null
     * arguments.
     *
     * @param a a string which may be null
     * @param b a string which may be null
     * @return true, if the strings equal, or if both are null.
     */
    private static boolean matchWithNull(String a, String b) {
        return (a == null ? b == null : a.equals(b));
    }

    /**
     * The interface method of the arbitration determines the most likely derivation to chose by
     * chosing the first within the same namespace. The cwns is the only element from the
     * environment used to determine the likely candidate.
     *
     * @param dvlist is a set of candidates
     * @param environment is a map describing the environment.
     * @return the chosen candidate from the input set
     */
    public Derivation arbit(java.util.Collection dvlist, java.util.Map environment) {
        // sanity check
        if (dvlist.size() == 0) return null;

        // linear search in list
        String cwns = (String) environment.get("cwns");
        Derivation result = null;
        for (Iterator i = dvlist.iterator(); i.hasNext(); ) {
            Derivation dv = (Derivation) i.next();
            if (matchWithNull(cwns, dv.getNamespace())) {
                result = dv;
                break;
            }
        }

        // still not found? Use first element -- and complain!
        if (result == null) {
            Logging.instance().log("route", 0, "Unable to match namespaces, using first element");
            result = (Derivation) dvlist.iterator().next();
        }

        // done
        return result;
    }
}
