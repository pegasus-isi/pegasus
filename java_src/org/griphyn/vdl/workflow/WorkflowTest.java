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
package org.griphyn.vdl.workflow;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.util.*;

/**
 * This class is used to show-case some elementary WF table stuff.
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public class WorkflowTest // extends Toolkit
 {
    public static void asdf(DatabaseSchema dbschema) throws SQLException {
        WF workflow = (WF) dbschema;
        Map x = workflow.getWorkflows(null);
        for (Iterator i = x.values().iterator(); i.hasNext(); ) {
            WorkEntry w = (WorkEntry) i.next();
            System.out.println(w.toString());
        }
    }

    public static void main(String args[]) throws Exception {
        // Connect the database.
        String schemaName = ChimeraProperties.instance().getWFSchemaName();
        Connect connect = new Connect();
        DatabaseSchema dbschema = connect.connectDatabase(schemaName);
        asdf(dbschema);
        dbschema.close();
    }
}
