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
 * Create a fully connected fake Diamond DAG exmample structure in memory using VDL classes.
 *
 * <p>
 *
 * <pre>
 *   C --<-- A
 *   |\     /|
 *   | \ _ / |
 *   V  | L  V
 *   |   /\  |
 *   |  /  \ |
 *   | /    \|
 *   D --<-- B
 * </pre>
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public class CreateFullDiamond {

    /**
     * create a 4 node diamond DAG as in-memory data structures employing the VDL classes. This is a
     * test module for some sample concrete planning modules.
     *
     * @return a list of Transformations and Derivations, encapsulated as Definitions.
     */
    public static Definitions create() {
        // create result vector
        Definitions result = new Definitions("test", "1.0");

        try {
            // create "generate" transformation
            Transformation t1 = new Transformation("generate");
            t1.addProfile(new Profile("hints", "pfnHint", new Text("keg.exe")));
            t1.addDeclare(new Declare("a", Value.SCALAR, LFN.OUTPUT));
            t1.addArgument(new Argument(null, new Text("-a generate -o ")));
            t1.addArgument(new Argument(null, new Use("a", LFN.OUTPUT)));
            result.addDefinition(t1);

            // create "gobble" transformation
            Transformation t2 = new Transformation("take1");
            t2.addProfile(new Profile("hints", "pfnHint", new Text("keg.exe")));
            t2.addDeclare(new Declare("a", Value.SCALAR, LFN.INPUT));
            t2.addDeclare(new Declare("b", Value.SCALAR, LFN.OUTPUT));
            Argument t2a1 = new Argument();
            t2a1.addLeaf(new Text("-a "));
            t2a1.addLeaf(new Text("take1"));
            t2.addArgument(t2a1);
            Argument t2a2 = new Argument();
            t2a2.addLeaf(new Text("-i "));
            t2a2.addLeaf(new Use("a", LFN.INPUT));
            t2.addArgument(t2a2);
            t2.addArgument(new Argument(null, new Text("-o")));
            t2.addArgument(new Argument(null, new Use("b", LFN.OUTPUT)));
            result.addDefinition(t2);

            Transformation t3 = new Transformation("take2");
            t3.addProfile(new Profile("hints", "pfnHint", new Text("keg.exe")));
            t3.addDeclare(new Declare("a", Value.SCALAR, LFN.INPUT));
            t3.addDeclare(new Declare("b", Value.SCALAR, LFN.INPUT));
            t3.addDeclare(new Declare("c", Value.SCALAR, LFN.OUTPUT));
            // you can do it thus...
            Argument t3a1 = new Argument();
            t3a1.addLeaf(new Text("-a take2 -i "));
            t3a1.addLeaf(new Use("a", LFN.INPUT));
            t3.addArgument(t3a1);
            // ...or thus...
            // NOTE: the space is now unnecessary, since argumentSeparator
            // defaults to one space. The argumentSeparator will be applied
            // when constructing the commandline from two <argument> elements.
            //// t3.addArgument( new Argument( null, new Text(" ") ) );
            t3.addArgument(new Argument(null, new Use("b", LFN.INPUT)));
            // ...or thus again
            Argument t3a2 = new Argument();
            t3a2.addLeaf(new Text("-o "));
            t3a2.addLeaf(new Use("c", LFN.OUTPUT));
            t3.addArgument(t3a2);
            result.addDefinition(t3);

            Transformation t4 = new Transformation("analyze");
            t4.addProfile(new Profile("hints", "pfnHint", new Text("keg.exe")));
            t4.addDeclare(new Declare("abc", Value.LIST, LFN.INPUT));
            t4.addDeclare(new Declare("d", Value.SCALAR, LFN.OUTPUT));
            t4.addArgument(new Argument(null, new Text("-a analyze -i")));
            t4.addArgument(new Argument("files", new Use("abc", "\"", " ", "\"")));
            t4.addArgument(new Argument(null, new Text("-o")));
            t4.addArgument(new Argument(null, new Use("d", LFN.OUTPUT)));
            result.addDefinition(t4);

            // create "top" node derivation of "generate"
            Derivation d1 = new Derivation("A", "generate");
            d1.addPass(new Pass("a", new Scalar(new LFN("f.a", LFN.OUTPUT))));
            result.addDefinition(d1);

            // create "left" node derivation of "findrange"
            Derivation d2 = new Derivation("B", "take1");
            d2.addPass(new Pass("b", new Scalar(new LFN("f.b", LFN.OUTPUT))));
            d2.addPass(new Pass("a", new Scalar(new LFN("f.a", LFN.INPUT))));
            result.addDefinition(d2);

            // create "right" node derivation of "findrange"
            Derivation d3 = new Derivation("C", "take2");
            d3.addPass(new Pass("a", new Scalar(new LFN("f.a", LFN.INPUT))));
            d3.addPass(new Pass("b", new Scalar(new LFN("f.b", LFN.INPUT))));
            d3.addPass(new Pass("c", new Scalar(new LFN("f.c", LFN.OUTPUT))));
            result.addDefinition(d3);

            // create "bottom" node derivation of "analyze"
            Derivation d4 = new Derivation("D", "analyze");
            List d4_list1 = new List();
            d4_list1.addScalar(new Scalar(new LFN("f.a", LFN.INPUT)));
            d4_list1.addScalar(new Scalar(new LFN("f.b", LFN.INPUT)));
            d4_list1.addScalar(new Scalar(new LFN("f.c", LFN.INPUT)));
            d4.addPass(new Pass("abc", d4_list1));
            d4.addPass(new Pass("d", new Scalar(new LFN("f.d", LFN.OUTPUT))));
            result.addDefinition(d4);
        } catch (IllegalArgumentException iae) {
            System.err.println(iae.getMessage());
            System.exit(1);
        }

        // finally
        return result;
    }
}
