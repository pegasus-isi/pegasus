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

import java.io.*;
import java.lang.reflect.*;
import java.sql.SQLException;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.util.Logging;

public class DiamondTest {
    public static void main(String[] args)
            throws IllegalArgumentException, IOException, ClassNotFoundException,
                    NoSuchMethodException, InstantiationException, SQLException,
                    IllegalAccessException, InvocationTargetException {
        // create debug output
        Logging.instance().register("dag", System.err);
        Logging.instance().register("state", System.err);
        Logging.instance().register("route", System.err);

        // create new route object, in memory classes
        Definitions original = org.griphyn.vdl.router.CreateDiamond.create();

        // serialize onto disk
        Logging.instance().log("default", 0, "serializing to disk");
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("data.out"));
        oos.writeObject(original);
        oos.flush();
        oos.close();
        Logging.instance().log("default", 0, "serializing done");

        // de-serialize from file
        Logging.instance().log("default", 0, "de-serialize from disk");
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream("data.out"));
        Definitions diamond = (Definitions) ois.readObject();
        ois.close();
        Logging.instance().log("default", 0, "de-serializing done");

        // create a router
        Route r = new Route(new InMemorySchema(diamond));

        // request known production
        BookKeeper bk = r.requestLfn("f.d");

        // show us the result
        System.out.println(bk.toString());

        // show the DAX
        System.out.println("----------");
        System.out.println(bk.getDAX("testing").toXML("", null));
        System.exit(0);
    }
}
