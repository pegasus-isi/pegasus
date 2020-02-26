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
package org.griphyn.vdl.directive;

import java.io.IOException;
import java.lang.reflect.*;
import java.util.MissingResourceException;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.parser.*;

/**
 * The class dynamically loads a databaseschema
 *
 * @see org.griphyn.vdl.dbschema.DatabaseSchema
 */
public class Connect extends Directive {
    /** Constructor */
    public Connect() throws IOException, MissingResourceException {
        super();
    }

    /**
     * Connects the database backend. This is not done in the c'tor, because some apps don't need
     * this heavyweight instructions.
     *
     * @param schemaName is the name of the schema class to load. This better be the fully-qualified
     *     name in-sync with properties.
     * @return the schema class on success, null on non-exceptional failure. The result is to be
     *     cast to appropriate catalog classes.
     * @see org.griphyn.vdl.util.ChimeraProperties#getVDCSchemaName()
     * @see org.griphyn.vdl.util.ChimeraProperties#getPTCSchemaName()
     */
    public DatabaseSchema connectDatabase(String schemaName)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {
        DatabaseSchema result = null;
        m_logger.log("connect", 0, "Connecting the database backend");

        Object[] arg = new Object[1];
        arg[0] = new String();

        return DatabaseSchema.loadSchema(schemaName, null, arg);
    }

    /**
     * Connects the database backend. This is not done in the c'tor, because some apps don't need
     * this heavyweight instructions.
     *
     * @param schemaName is the name of the schema class to load. This better be the fully-qualified
     *     name in-sync with properties.
     * @param dbDriverName is the name of the database driver
     * @return the schema class on success, null on non-exceptional failure. The result is to be
     *     cast to appropriate catalog classes.
     * @see org.griphyn.vdl.util.ChimeraProperties#getVDCSchemaName()
     * @see org.griphyn.vdl.util.ChimeraProperties#getPTCSchemaName()
     */
    public DatabaseSchema connectDatabase(String schemaName, String dbDriverName)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {
        DatabaseSchema result = null;
        m_logger.log("connect", 0, "Connecting the database backend");

        Object[] arg = new Object[1];
        arg[0] = (dbDriverName == null) ? new String() : dbDriverName;

        return DatabaseSchema.loadSchema(schemaName, null, arg);
    }
}
