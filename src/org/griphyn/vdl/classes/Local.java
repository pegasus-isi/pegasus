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
import java.util.*;

/**
 * This class defines the temporary variables in compound <code>Transformation</code>.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Transformation
 */
public class Local extends VDL implements Serializable {
    /** Each temporary variable has a name to which it is bound. */
    private String m_name;

    /**
     * Each temporary variable has a specific type. There are currently only the type of {@link
     * Scalar} and {@link List}.
     */
    private int m_containerType = Value.SCALAR;

    /**
     * For linking the DAG we need to know if the argument is passed into the transformation,
     * produced by the transformation, or has some other behavior. However, temporary variables are
     * usually transient, and thus either of type none or inout.
     */
    private int m_link = LFN.NONE;

    /** The initialization value of a temporary variable is mandatory. */
    private Value m_value;

    /** Default ctor: needed for JDO */
    public Local() {
        super();
    }

    /**
     * ctor: Construct a temporary variable with a binding and default container type.
     *
     * @param name is the binding.
     * @param ct is the container type, the type of the argument.
     * @throws IllegalArgumentException if the container type is outside the legal range [{@link
     *     Value#SCALAR}, {@link Value#LIST}].
     */
    public Local(String name, int ct) throws IllegalArgumentException {
        super();
        this.m_name = name;
        if (Value.isInRange(ct)) this.m_containerType = ct;
        else throw new IllegalArgumentException("container type outside legal range");
    }

    /**
     * ctor: Construct a new temporary variable with a binding and default container type, as well
     * as a linkage for the variable.
     *
     * @param name is the binding.
     * @param ct is the container type, the type of the variable.
     * @param link is the linkage type for the variable.
     * @throws IllegalArgumentException if the container type is outside the legal range [{@link
     *     Value#SCALAR}, {@link Value#LIST}], or the linkage is outside [{@link LFN#NONE}, {@link
     *     LFN#INOUT}].
     */
    public Local(String name, int ct, int link) throws IllegalArgumentException {
        super();
        this.m_name = name;
        if (Value.isInRange(ct)) this.m_containerType = ct;
        else throw new IllegalArgumentException("container type outside legal range");

        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException("linkage type outside legal range");
    }

    /**
     * ctor: Construct a new temporary variable with a binding and default value. The container type
     * will be determined from the default value.
     *
     * @param name is the binding.
     * @param value is either a {@link Scalar} or {@link List} value.
     */
    public Local(String name, Value value) {
        super();
        this.m_name = name;
        this.m_value = value;
        this.m_containerType = value.getContainerType();
    }

    /**
     * ctor: Construct a temporary variable with a binding and default value. The container type
     * will be determined from the default value. The linkage is set separately.
     *
     * @param name is the binding.
     * @param value is either a {@link Scalar} or {@link List} value.
     * @param link is the linkage of the value for DAG creation.
     * @throws IllegalArgumentException if the linkage is outside [{@link LFN#NONE}, {@link
     *     LFN#INOUT}].
     */
    public Local(String name, Value value, int link) throws IllegalArgumentException {
        super();
        this.m_name = name;
        this.m_value = value;
        this.m_containerType = value.getContainerType();
        if (LFN.isInRange(link)) this.m_link = link;
        else throw new IllegalArgumentException("linkage type outside legal range");
    }

    /**
     * Accessor: Obtains the value of the variable.
     *
     * @return the default as {@link Scalar} or {@link List}, or <code>null</code> in case of error.
     * @see #setValue(Value)
     */
    public Value getValue() {
        return this.m_value;
    }

    /**
     * Accessor: Obtains the name of this temporary variable.
     *
     * @return the name to refer to this variable.
     * @see #setName( java.lang.String )
     */
    public String getName() {
        return this.m_name;
    }

    /**
     * Accessor: Obtains the linkage type of the variable.
     *
     * @return the linkage as an integer.
     * @see #setLink(int)
     * @see LFN#NONE
     * @see LFN#INPUT
     * @see LFN#OUTPUT
     * @see LFN#INOUT
     */
    public int getLink() {
        return this.m_link;
    }

    /**
     * Accessor: Obtains the container type. Note that the container type of the value will be
     * considered first.
     *
     * @return the container type of the variable.
     * @see #setContainerType(int)
     * @see Value#SCALAR
     * @see Value#LIST
     */
    public int getContainerType() {
        return (m_value != null) ? m_value.getContainerType() : m_containerType;
    }

    /**
     * Accessor: Establishes a new name for this variable. FIXME: Note that no checks will be done
     * concerning the uniqueness of the new name.
     *
     * @param name is the new binding.
     * @see #getName()
     */
    public void setName(String name) {
        this.m_name = name;
    }

    /**
     * Accessor: Sets a new linkage type for the variable.
     *
     * @param link is the new linkage type from {@link LFN}.
     * @throws IllegalArgumentException, if the argument is outside the valid range.
     * @see #getLink()
     */
    public void setLink(int link) throws IllegalArgumentException {
        if (!LFN.isInRange(link)) throw new IllegalArgumentException();
        this.m_link = link;
    }

    /**
     * Accessor: Sets a new container type for the variable. The new container type must match the
     * value's container type.
     *
     * @param containerType is the new integer describing a container type.
     * @throws IllegalArgumentException if the container type is neither {@link Value#SCALAR} nor
     *     {@link Value#LIST}.
     * @see #getContainerType()
     */
    public void setContainerType(int containerType) throws IllegalArgumentException {
        if (m_value == null) {
            // no default value known, need to set container type
            if (Value.isInRange(containerType)) this.m_containerType = containerType;
            else throw new IllegalArgumentException("container type outside legal range");
        } else {
            // there is a default value, new type must match default
            if (m_value.getContainerType() != containerType)
                throw new IllegalArgumentException(
                        "new container type does not match container type of default value");
        }
    }

    /**
     * Accessor: Sets or overwrites the default value of variable. The new default must match the
     * container type.
     *
     * @param value is the new default value.
     * @throws IllegalArgumentException if the container type of the new value and of the registered
     *     container type for the parameter don't match.
     * @see #getValue()
     */
    public void setValue(Value value) throws IllegalArgumentException {
        if (value.getContainerType() == this.m_containerType) this.m_value = value;
        else
            // container types do not match
            throw new IllegalArgumentException(
                    "container type of new value does not match Locald container type");
    }

    /**
     * Dumps the content of the this element into a string for human consumption.
     *
     * @return a textual description of the element and its attributes. Be advised that these
     *     strings might become large.
     */
    public String toString() {
        String value = this.m_value == null ? "" : this.m_value.toString();
        StringBuffer result = new StringBuffer(12 + this.m_name.length() + value.length());

        result.append(LFN.toString(this.m_link));
        result.append(' ');
        result.append(this.m_name);
        if (this.m_containerType == Value.LIST) result.append("[]");

        if (this.m_value != null) {
            result.append('=');
            result.append(value);
        }
        return result.toString();
    }

    /**
     * Dumps the content of the variable definition into a string for human consumption.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toString(Writer stream) throws IOException {
        stream.write(LFN.toString(this.m_link));
        stream.write(' ');
        stream.write(escape(this.m_name));
        if (this.m_containerType == Value.LIST) stream.write("[]");

        if (this.m_value != null) {
            stream.write('=');
            this.m_value.toString(stream);
        }
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
     *     traversal. If a <code>null</code> value is specified, no indentation nor linefeeds will
     *     be generated.
     * @param namespace is the XML schema namespace prefix. If neither empty nor null, each element
     *     will be prefixed with this prefix, and the root element will map the XML namespace.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":local" : "local";

        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);

        writeAttribute(stream, " name=\"", this.m_name); // null-safe
        if (LFN.isInRange(this.m_link))
            writeAttribute(stream, " link=\"", LFN.toString(this.m_link));

        if (this.m_containerType == Value.LIST) {
            stream.write(" container=\"list\"");
        } else if (this.m_containerType == Value.SCALAR) {
            stream.write(" container=\"scalar\"");
        }

        if (this.m_value == null) {
            // no default value
            stream.write("/>");
        } else {
            // there is a default value
            String newindent = indent == null ? null : indent + "  ";
            stream.write('>');
            if (indent != null) stream.write(newline);

            // dump content
            this.m_value.toXML(stream, newindent, namespace);

            // write close tag
            if (indent != null && indent.length() > 0) stream.write(indent);
            stream.write("</");
            stream.write(tag);
            stream.write('>');
        }
        if (indent != null) stream.write(newline);
    }
}
