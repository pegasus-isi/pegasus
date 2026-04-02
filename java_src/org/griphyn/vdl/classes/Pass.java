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

package org.griphyn.vdl.classes;

import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;

/**
 * This class encapsulates a single formal argument that is passed from a {@link Derivation} to a
 * {@link Transformation}.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Value
 */
public class Pass extends VDL implements Cloneable, Serializable {
    /**
     * Each actual argument must bind to a formal argument. Binding is done via the name of the
     * formal argument, as stored in the binding variable.
     */
    private String m_bind;

    /**
     * Each actual argument does have a value. This attributes store the current state of a value.
     */
    private Value m_value;

    /**
     * Creates and returns a copy of this object.
     *
     * @return a new instance, semi-deep copy
     */
    public Object clone() {
        return new Pass(this.m_bind, (Value) this.m_value.clone());
    }

    /** Ctor. */
    public Pass() {
        super();
    }

    /**
     * Convenience ctor: Establishes a binding with an empty value. Note that the value is still
     * null, and must be set explicitely.
     *
     * @param bind is the name of the formal argument to bind to.
     * @see #setValue(Value)
     */
    public Pass(String bind) {
        super();
        this.m_bind = bind;
    }

    /**
     * Convencience ctor: Establishes a binding with a value.
     *
     * @param bind is the name of the formal argument to bind to.
     * @param value is the value to pass to a {@link Transformation}.
     */
    public Pass(String bind, Value value) {
        super();
        this.m_bind = bind;
        this.m_value = value;
    }

    /**
     * Accessor: Gets the current bound variable name.
     *
     * @return the name of the variable bound to. May return null on an default constructed object.
     * @see #setBind( java.lang.String )
     */
    public String getBind() {
        return this.m_bind;
    }

    /**
     * Accessor: Gets the current value to be passed. Note that each {@link Value} is either a
     * {@link Scalar} or {@link List}.
     *
     * @return the value that is to be passed to a {@link Transformation}.
     * @see #setValue( Value )
     */
    public Value getValue() {
        return this.m_value;
    }

    /**
     * Accessor: Sets a new binding with a formal argument.
     *
     * @param bind is the new binding name.
     * @see #getBind()
     */
    public void setBind(String bind) {
        this.m_bind = bind;
    }

    /**
     * Accessor: Sets a new value for a bound variable.
     *
     * @param value is the new value, which can be a {@link Scalar} or a {@link List}.
     * @see #getValue()
     */
    public void setValue(Value value) {
        this.m_value = value;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toString(Writer stream) throws IOException {
        // stream.write( escape(this.m_bind) );
        stream.write(this.m_bind);
        stream.write('=');
        this.m_value.toString(stream);
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently, if you use a buffered writer.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty printing. The initial amount
     *     of spaces should be an empty string. The parameter is used internally for the recursive
     *     traversal.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":pass" : "pass";
        String newline = System.getProperty("line.separator", "\r\n");

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " bind=\"", this.m_bind);
        stream.write('>');
        if (indent != null) stream.write(newline);

        // write content
        String newindent = indent == null ? null : indent + "  ";
        this.m_value.toXML(stream, newindent, namespace);

        // close tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        stream.write(tag);
        stream.write('>');
        if (indent != null) stream.write(newline);
    }
}
