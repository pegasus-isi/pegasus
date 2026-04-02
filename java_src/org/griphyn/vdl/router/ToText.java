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

import java.io.IOException;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.toolkit.Toolkit;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

public class ToText extends Toolkit {
    public ToText(String arg0) {
        super(arg0);
    }

    public void showUsage() {}

    public static void main(String[] args) throws IllegalArgumentException, IOException, Exception {
        ToText me = new ToText("ToText");
        if (args.length != 1) throw new IllegalArgumentException("java prg xml");

        // create debug output
        Logging.instance().register("state", System.err);
        Logging.instance().register("route", System.err);

        //    // force file
        //    me.m_props.setProperty( "vds.db.vds", "file" );
        //    me.m_props.setProperty( "vds.db.file.store", args[0] );

        // user supplied database, set up me.m_dbschema
        String vdcSchemaName = ChimeraProperties.instance().getVDCSchemaName();
        Connect connect = new Connect();
        DatabaseSchema dbschema = connect.connectDatabase(vdcSchemaName);

        Definitions defs = null;
        if (dbschema instanceof InMemorySchema) {
            // already everything in main memory, use backdoor (Yuck! Dirty!)
            // avoid duplicating DVs in main memory.
            defs = ((InMemorySchema) dbschema).backdoor();
        } else {
            // Load all Definitions into an in-memory database (uiuiui)
            me.m_logger.log("app", 1, "loading *all* definitions into memory");
            defs = new Definitions();
            defs.setDefinition(((VDC) dbschema).searchDefinition(null, null, null, -1));
        }

        if (defs == null) {
            System.err.println("No input data, nothing to route");
            return;
        }

        // create new route object, in memory classes
        Route r = new Route(new InMemorySchema(defs));

        // dump contents onto stdout
        // r.dump(System.out); no longer a valid operation
    }
}
