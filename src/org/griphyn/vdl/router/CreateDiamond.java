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

import org.griphyn.vdl.classes.*;

/**
 * create a Diamond DAG exmample structure in memory using VDL classes.
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public class CreateDiamond {

    /**
     * create a 4 node diamond DAG as in-memory data structures employing the VDL classes. This is a
     * test module for the router, until the input (SAX) becomes available.
     *
     * @return a list of Transformations and Derivations, encapsulated as Definitions.
     */
    public static Definitions create() {
        // create result vector
        Definitions result = new Definitions("test", "1.0");

        try {
            // create "generate" transformation
            Transformation t1 = new Transformation("generate");
            t1.addProfile(new Profile("hints", "pfnHint", new Text("generator.exe")));
            t1.addDeclare(new Declare("a", Value.SCALAR, LFN.OUTPUT));
            t1.addArgument(new Argument("stdout", new Use("a", LFN.OUTPUT)));
            result.addDefinition(t1);

            // create "findrange" transformation
            Transformation t2 = new Transformation("findrange");
            t2.addProfile(new Profile("hints", "pfnHint", new Text("findrange.exe")));
            Argument t2_arg1 = new Argument("arg", new Text("-i "));
            t2_arg1.addLeaf(new Use("p", LFN.NONE));
            t2.addDeclare(new Declare("a", Value.SCALAR, LFN.INPUT));
            t2.addDeclare(new Declare("b", Value.SCALAR, LFN.OUTPUT));
            t2.addDeclare(new Declare("p", new Scalar(new Text("0.0"))));
            t2.addArgument(t2_arg1);
            t2.addArgument(new Argument("stdin", new Use("a", LFN.INPUT)));
            t2.addArgument(new Argument("stdout", new Use("b", LFN.OUTPUT)));
            result.addDefinition(t2);

            // create "analyze" transformation
            Transformation t3 = new Transformation("analyze");
            t3.addProfile(new Profile("hints", "pfnHint", new Text("analyze.exe")));
            t3.addArgument(new Argument("files", new Use("a", "", " ", "")));
            t3.addArgument(new Argument("stdout", new Use("c", LFN.OUTPUT)));
            t3.addDeclare(new Declare("a", Value.LIST, LFN.INPUT));
            t3.addDeclare(new Declare("c", Value.SCALAR, LFN.OUTPUT));
            result.addDefinition(t3);

            // create "top" node derivation of "generate"
            Derivation d1 =
                    new Derivation(
                            "top",
                            "generate",
                            new Pass("a", new Scalar(new LFN("f.a", LFN.OUTPUT))));
            result.addDefinition(d1);

            // create "left" node derivation of "findrange"
            Derivation d2 = new Derivation("left", "findrange");
            d2.addPass(new Pass("b", new Scalar(new LFN("f.b", LFN.OUTPUT))));
            d2.addPass(new Pass("a", new Scalar(new LFN("f.a", LFN.INPUT))));
            d2.addPass(new Pass("p", new Scalar(new Text("0.5"))));
            result.addDefinition(d2);

            // create "right" node derivation of "findrange"
            Derivation d3 = new Derivation("right", "findrange");
            d3.addPass(new Pass("a", new Scalar(new LFN("f.a", LFN.INPUT))));
            d3.addPass(new Pass("b", new Scalar(new LFN("f.c", LFN.OUTPUT))));
            d3.addPass(new Pass("p", new Scalar(new Text("1.0"))));
            result.addDefinition(d3);

            // create "bottom" node derivation of "analyze"
            Derivation d4 = new Derivation("bottom", "analyze");
            List d4_list1 = new List();
            d4_list1.addScalar(new Scalar(new LFN("f.b", LFN.INPUT)));
            d4_list1.addScalar(new Scalar(new LFN("f.c", LFN.INPUT)));
            d4.addPass(new Pass("a", d4_list1));
            d4.addPass(new Pass("c", new Scalar(new LFN("f.d", LFN.OUTPUT))));
            result.addDefinition(d4);
        } catch (IllegalArgumentException iae) {
            System.err.println(iae.getMessage());
            System.exit(1);
        }

        // finally
        return result;
    }
}
