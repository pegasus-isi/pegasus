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

/**
 * This interface defines an arbitration. If multiple derivations produce the same output file, one
 * derivation must be chosen over all others. There is not yet any mean to declare that none will
 * do.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision $
 */
public interface Arbiter {
    /**
     * The lone method of the arbitrarion receives some environmental information about the position
     * in the call stack. Currently, this information is of limited nature. Furthermore, it receives
     * the list of candidates. From this list, one candidate must be chosen, and returned to as the
     * chosen one.
     *
     * <p>The environmental description currently contains the following entries:
     *
     * <table border="0">
     * <tr><th>key</th><th>type</th><th>meaning</th></tr>
     * <tr><td>lfn</td><td>String</td><td>Output filename produced by all.</td></tr>
     * <tr><td>cwns</td><td>String</td><td>Current working namespace,
     *  may be null.</td></tr>
     * <tr><td>level</td><td>Integer</td><td>Current recursion depth.</td></tr>
     * </table>
     *
     * @param dvlist is a set of candidates
     * @param environment is a map describing the environment.
     * @return the chosen candidate from the input set
     */
    public Derivation arbit(java.util.Collection dvlist, java.util.Map environment);
}
