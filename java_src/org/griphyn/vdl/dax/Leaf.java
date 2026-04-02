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
package org.griphyn.vdl.dax;

import java.io.IOException;
import java.io.Writer;

/**
 * <code>Leaf</code> is an abstract base class for leaf nodes in the instance tree.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see PseudoText
 * @see Filename
 */
public abstract class Leaf extends DAX implements Cloneable {
    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance.
     */
    public abstract Object clone();

    /**
     * Dumps the state of the filename as PlainFilenameType without the transiency information.
     * Other leaves will use their regular toXML method.
     *
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @param flag if bit#0 is set, also dump the linkage information, otherwise do not dump linkage
     *     information. If bit#1 is set, also dump optionality for true optional files. Unused in
     *     base class.
     * @return a String which contains the state of the current class and its siblings using XML.
     *     Note that these strings might become large.
     * @see #toXML( String, String )
     */
    public String shortXML(String indent, String namespace, int flag) {
        return toXML(indent, namespace);
    }

    /**
     * Dumps the state of the filename as PlainFilenameType without the transiency information.
     * Other leaves will use their regular toXML method.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @param flag if bit#0 is set, also dump the linkage information, otherwise do not dump linkage
     *     information. If bit#1 is set, also dump optionality for true optional files. Unused in
     *     base class.
     * @exception IOException if something fishy happens to the stream.
     * @see #toXML( Writer, String, String )
     */
    public void shortXML(Writer stream, String indent, String namespace, int flag)
            throws IOException {
        toXML(stream, indent, namespace);
    }
}
