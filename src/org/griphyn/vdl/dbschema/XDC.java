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
package org.griphyn.vdl.dbschema;

import java.sql.*;

/**
 * This common schema interface defines advanced search interfaces for VDC. The advanced methods
 * required permit wildcard searches, partial matches, and candidate list compilations that are not
 * part of the simpler {@link VDC} interface.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbschema.DatabaseSchema
 * @see org.griphyn.vdl.dbdriver
 */
public interface XDC extends Advanced, Annotation {
    public abstract java.util.List searchDefinition(String xpath) throws SQLException;

    public abstract java.util.List searchElements(String xpath) throws SQLException;
}
