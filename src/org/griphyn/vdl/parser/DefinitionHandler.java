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
package org.griphyn.vdl.parser;

import org.griphyn.vdl.classes.*;

/**
 * This interface introduces a callback to be employed whenever a Definition is fully read into
 * memory, and ready to be processed. Any overwrite or dontcare mode is not part of this interface's
 * contract.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Definition
 * @see Transformation
 * @see Derivation
 */
public interface DefinitionHandler {
    /**
     * This method adds the given Definition to whatever storage is implemented underneath.
     *
     * @param d is the Definition that is ready to be stored.
     * @return true, if new version was stored and database modified, false, if the definition was
     *     rejected for any reason.
     */
    public boolean store(Definition d);
}
