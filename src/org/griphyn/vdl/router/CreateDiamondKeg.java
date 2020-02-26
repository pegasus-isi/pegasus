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
 * Creates a Diamond DAG exmample structure in memory using VDL classes and the kanonical executable
 * for GriPhyN aka keg.
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public class CreateDiamondKeg {

    /**
     * create a 4 node diamond DAG as in-memory data structures employing the VDL classes. This is a
     * test module for the router, until the input (SAX) becomes available.
     *
     * @return a list of Transformations and Derivations, encapsulated as Definitions.
     */
    public static Definitions create(boolean condor) {
        // create result vector
        Definitions result = new Definitions("diamond", "1.0");

        Profile executable =
                new Profile("hints", "pfnHint", new Text(condor ? "keg.condor" : "keg"));
        Profile universe =
                new Profile("hints", "pfnUniverse", new Text(condor ? "standard" : "vanilla"));
        try {
            // create "generate" transformation
            Transformation t1 = new Transformation("generate");
            t1.addProfile(executable);
            t1.addProfile(universe);
            t1.addDeclare(new Declare("a", Value.SCALAR, LFN.OUTPUT));
            Argument t1a1 = new Argument();
            t1a1.addLeaf(new Text("-a generate -o "));
            t1a1.addLeaf(new Use("a", LFN.OUTPUT));
            t1.addArgument(t1a1);
            result.addDefinition(t1);

            // create "findrange" transformation
            Transformation t2 = new Transformation("findrange");
            t2.addProfile(executable);
            t2.addProfile(universe);
            t2.addDeclare(new Declare("a", Value.SCALAR, LFN.INPUT));
            t2.addDeclare(new Declare("b", Value.SCALAR, LFN.OUTPUT));
            t2.addDeclare(new Declare("p", new Scalar(new Text("0.0"))));
            t2.addArgument(new Argument(null, new Text("-a findrange")));
            Argument t2a1 = new Argument(null, new Text(" -p "));
            t2a1.addLeaf(new Use("p"));
            t2.addArgument(t2a1);
            Argument t2a2 = new Argument(null, new Text(" -i "));
            t2a2.addLeaf(new Use("a", LFN.INPUT));
            t2.addArgument(t2a2);
            Argument t2a3 = new Argument(null, new Text(" -o "));
            t2a3.addLeaf(new Use("b", LFN.OUTPUT));
            t2.addArgument(t2a3);
            result.addDefinition(t2);

            // create "analyze" transformation
            Transformation t3 = new Transformation("analyze");
            t3.addProfile(executable);
            t3.addProfile(universe);
            t3.addArgument(new Argument(null, new Text("-a analyze")));
            Argument t3a1 = new Argument("files", new Text(" -i "));
            t3a1.addLeaf(new Use("a", "", " ", ""));
            t3.addArgument(t3a1);
            Argument t3a2 = new Argument(null, new Text(" -o "));
            t3a2.addLeaf(new Use("c", LFN.OUTPUT));
            t3.addArgument(t3a2);
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
