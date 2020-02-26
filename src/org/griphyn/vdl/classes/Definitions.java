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

import edu.isi.pegasus.common.util.Currently;
import java.io.*;
import java.util.*;
import org.griphyn.vdl.util.*;

/**
 * This class implements the container to carry any number of <code>Transformation</code> and <code>
 * Derivation</code> instances. In addition, it captures some attributes from the root element of
 * the XML document.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Definition
 * @see Transformation
 * @see Derivation
 */
public class Definitions extends VDL implements Serializable {
    /** The "official" namespace URI of the VDLx schema. */
    public static final String SCHEMA_NAMESPACE = "http://www.griphyn.org/chimera/VDL";

    /** The "not-so-official" location URL of the VDLx schema definition. */
    public static final String SCHEMA_LOCATION = "http://www.griphyn.org/chimera/vdl-1.24.xsd";

    // attributes of "definitions" element
    /**
     * Capture the global namespace given to all child elements that do not set their own namespace
     * definition.
     */
    private String m_vdlns;

    /** Capture the version of the XML document. */
    private String m_version;

    /**
     * children are either {@link Transformation}s or {@link Derivation}s, both of which are {@link
     * Derivation}s.
     */
    private ArrayList m_definitionList;

    /**
     * ctor. It is strongly suggested that you set the namespace and version of the object before
     * adding any other {@link Definition} objects.
     */
    public Definitions() {
        this.m_definitionList = new ArrayList();
        this.m_vdlns = null;
        this.m_version = null;
    }

    /**
     * ctor: Create a new container, using the given namespace. It is highly recommended that you
     * set the version number before you add any {@link Definition} instance.
     *
     * @param vdlns is the namespace to use for elements w/o namespace.
     */
    public Definitions(String vdlns) {
        this.m_definitionList = new ArrayList();
        this.m_vdlns = vdlns;
        this.m_version = null;
    }

    /**
     * ctor: Create a new container, using a namespace and version.
     *
     * @param vdlns is the namespace to propagate to children w/o namespace.
     * @param version is a version of the XML document used to transport the data.
     */
    public Definitions(String vdlns, String version) {
        this.m_definitionList = new ArrayList();
        this.m_vdlns = vdlns;
        this.m_version = version;
    }

    /**
     * updating old Use.linkage with new Use.linkage. This table uses the old/stored linkage in the
     * top row, and the new/found linkage in the first column. (-) denotes no action to be taken,
     * and # an illegal combination.
     *
     * <p>
     *
     * <pre>
     *       | -1  | NONE| IN  | OUT | IO
     *   ----+-----+-----+-----+-----+----
     *    -1 | (-) | (-) | (-) | (-) | (-)
     *   NONE| NONE| (-) |  #  |  #  |  #
     *     IN| IN  |  #  | (-) | IO  | (-)
     *    OUT| OUT |  #  | IO  | (-) | (-)
     *     IO| IO  |  #  | IO  | IO  | (-)
     *   ----+-----+-----+-----+-----+----
     * </pre>
     *
     * The table uses -1 for no action to do, and -2 for an illegal state.
     */
    private int m_state[][] = {
        {-1, -1, -1, -1, -1}, // newlink == -1
        {0, -1, -2, -2, -2}, // newlink == NONE
        {1, -2, -1, 3, -1}, // newlink == IN
        {2, -2, 3, -1, -1}, // newlink == OUT
        {3, -2, 3, 3, -1}
    };
    /**
     * Checks the linkage of a transformation between a declared, previously used and currently used
     * variable of the same name.
     *
     * @param use is a table of previously used variables and their linkage
     * @param u is the variable at the "cursor position".
     * @param tr is the transformation to be checked.
     */
    private void checkLinkage(Map use, Use u, Transformation tr) {
        if (use.containsKey(u.getName())) {
            // key exists, check/modify linkage
            int linkage = ((Integer) use.get(u.getName())).intValue();
            int newlink = u.getLink();
            int result = this.m_state[newlink + 1][linkage + 1];
            if (result == -2) {
                // illegal combination of linkages, usually NONE w/ I,O,IO
                throw new IncompatibleLinkageException(
                        "Transformation "
                                + tr.shortID()
                                + "uses variable "
                                + u.getName()
                                + " with incompatibles linkages");
            } else if (result > -1) {
                // store new result
                use.put(u.getName(), new Integer(result));
            }
        } else {
            // key does not exist, add
            use.put(u.getName(), new Integer(u.getLink()));
        }
    }

    /**
     * Clean-up definition and perform abstract type checks before submitting them into the
     * document.
     *
     * @exception IllegalArgumentException will be thrown if the <code>Definition</code> is neither
     *     a <code>Derivation</code> nor a <code>Transformation</code>. This should not happen.
     * @exception UndeclaredVariableException will be thrown, if a <code>Transformation</code> uses
     *     a bound variable via <code>Use</code>, but fails to declare the formal argument with
     *     <code>Declare</code>.
     * @exception IncompatibleLinkageException will be thrown, if the declared linkage of a formal
     *     argument is incompatible with the usage of such a bound variable within a <code>
     *     Transformation</code>.
     * @exception IllegalTransformationException will be thrown, if the <code>Transformation</code>
     *     has simultaneously <code>Call</code> and <code>Argument</code> items. This exception is
     *     bound to vanish with the next major re-design.
     * @see Transformation
     * @see Derivation
     * @see Use
     * @see Declare
     */
    protected void sanitizeDefinition(Definition d)
            throws IllegalArgumentException, IncompatibleLinkageException,
                    UndeclaredVariableException, IllegalTransformationException {
        String newline = System.getProperty("line.separator", "\r\n");

        // update definition with namespace and version, if necessary
        // Note: results may still be null
        if (d.getNamespace() == null && this.m_vdlns != null) d.setNamespace(this.m_vdlns);
        if (d.getVersion() == null && this.m_version != null) d.setVersion(this.m_version);

        switch (d.getType()) {
            case Definition.TRANSFORMATION:
                Transformation tr = (Transformation) d;
                HashMap use = new HashMap();

                // a TR must not be simultaneously simple and compound
                if (tr.getArgumentCount() > 0 && tr.getCallCount() > 0)
                    throw new IllegalTransformationException(
                            "TR "
                                    + tr.identify()
                                    + " is simultaneously simple and compound"
                                    + newline
                                    + tr.toXML("\t", null));

                //
                // collect all unique bindings of class Use
                //
                if (tr.isSimple()) {
                    // collect from Argument list
                    for (Iterator e = tr.iterateArgument(); e.hasNext(); ) {
                        for (Iterator f = ((Argument) e.next()).iterateLeaf(); f.hasNext(); ) {
                            Leaf l = (Leaf) f.next();
                            if (l instanceof Use) checkLinkage(use, (Use) l, tr);
                        }
                    }
                } else {
                    // collect from Call list, this is slightly more complex...
                    // only 3..4 nested for loops, why do you worry...
                    for (Iterator e = tr.iterateCall(); e.hasNext(); ) {
                        for (Iterator f = ((Call) e.next()).iteratePass(); f.hasNext(); ) {
                            Value v = (Value) ((Pass) f.next()).getValue();
                            switch (v.getContainerType()) {
                                case Value.SCALAR:
                                    for (Iterator g = ((Scalar) v).iterateLeaf(); g.hasNext(); ) {
                                        Leaf l = (Leaf) g.next();
                                        if (l instanceof Use) checkLinkage(use, (Use) l, tr);
                                    }
                                    break;
                                case Value.LIST:
                                    for (Iterator h = ((List) v).iterateScalar(); h.hasNext(); ) {
                                        for (Iterator g = ((Scalar) h.next()).iterateLeaf();
                                                g.hasNext(); ) {
                                            Leaf l = (Leaf) g.next();
                                            if (l instanceof Use) checkLinkage(use, (Use) l, tr);
                                        }
                                    }
                                    break;
                            }
                        }
                    }
                }
                // collect from Profile list
                for (Iterator e = tr.iterateProfile(); e.hasNext(); ) {
                    for (Iterator f = ((Profile) e.next()).iterateLeaf(); f.hasNext(); ) {
                        Leaf l = (Leaf) f.next();
                        if (l instanceof Use) checkLinkage(use, (Use) l, tr);
                    }
                }

                // check usages against all declared and temporary variables. Also
                // check linkage. Note that the declared variables must have a
                // linkage. It is permissable to declare variables, but not use
                // them.
                for (Iterator i = use.keySet().iterator(); i.hasNext(); ) {
                    String name = (String) i.next();

                    // check that the used variable is declared
                    Declare dec = (Declare) tr.getDeclare(name);
                    Local local = (Local) tr.getLocal(name);
                    if (dec == null && local == null)
                        throw new UndeclaredVariableException(
                                "variable "
                                        + name
                                        + " is used, but not declared"
                                        + newline
                                        + tr.toXML("\t", null));

                    // match up linkages. Note that a use linkage of -1 means
                    // that we don't have any information on the used linkage.
                    int dLinkage = (dec == null ? local.getLink() : dec.getLink());
                    int uLinkage = ((Integer) use.get(name)).intValue();
                    if (uLinkage > -1) {
                        if (dLinkage == LFN.NONE && uLinkage != LFN.NONE
                                || dLinkage == LFN.INPUT && uLinkage != LFN.INPUT
                                || dLinkage == LFN.OUTPUT && uLinkage != LFN.OUTPUT
                                || dLinkage == LFN.INOUT && uLinkage == LFN.NONE)
                            throw new IncompatibleLinkageException(
                                    "variable "
                                            + name
                                            + " uses incompatible linkages"
                                            + newline
                                            + tr.toXML("\t", null));
                    }
                }
                break;
            case Definition.DERIVATION:
                Derivation dv = (Derivation) d;

                if (dv.getUsesspace() == null) {
                    if (this.m_vdlns != null) {
                        // either default uses namespace to vdlns
                        dv.setUsesspace(this.m_vdlns);
                    } else if (d.getNamespace() != null) {
                        // or default uses namespace to derivation namespace
                        dv.setUsesspace(d.getNamespace());
                    }
                }

                // nothing really to check for derivations
                // note: Do *not* check here, if a DV has a matching TR, because
                // in the future, TR will be stored in distributed database(s).
                break;

            default:
                // this must not happen
                throw new IllegalArgumentException(
                        "Definition " + d.identify() + " is neither TR nor DV");
        }
    }

    /**
     * Accessor: Appends a {@link Definition} to the container. The namespace and version
     * information will be, in case they are missing, updated from the definitions namespace and
     * version respectively.
     *
     * @param d is the {@link Transformation} or {@link Derivation} to append to the internal
     *     container.
     * @throws IndexOutOfBoundsException if the definition does not fit into the container.
     */
    public void addDefinition(Definition d) throws IndexOutOfBoundsException {
        this.sanitizeDefinition(d);
        this.m_definitionList.add(d);
    }

    /**
     * Accessor: Inserts a {@link Definition} at a particular place. The namespace and version
     * information will be, in case they are missing, updated from the definitions namespace and
     * version respectively.
     *
     * <p>Each component in this vector with an index greater or equal to the specified index is
     * shifted upward to have an index one greater than the value it had previously.
     *
     * @param index is the position to insert a {@link Definition}
     * @param d is the {@link Transformation} or {@link Derivation} to append to the internal
     *     container.
     * @throws IndexOutOfBoundsException if the definition does not fit into the container.
     */
    public void addDefinition(int index, Definition d) throws IndexOutOfBoundsException {
        this.sanitizeDefinition(d);
        this.m_definitionList.add(index, d);
    }

    /**
     * Accessor: Search the database for the existence of a Definition with the same primary keys
     * and type as the parameter.
     *
     * @param d is the Definition to search for
     * @return the position of the selfsame Definition, or -1 if not found.
     */
    public int positionOfDefinition(Definition d) {
        int n = 0;
        for (Iterator i = this.m_definitionList.iterator(); i.hasNext(); ++n) {
            if (d.equals(i.next())) return n;
        }
        return -1;
    }

    /**
     * Accessor: Provides an iterator for the list of {@link Transformation} and {@link Derivation}.
     *
     * @return the iterator to traverse the container of {@link Definition}s.
     * @see java.util.Enumeration
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumerateDefinition() {
        return Collections.enumeration(this.m_definitionList);
    }

    /**
     * Obtains a vector of all definition instances that share the same instance type. Please note
     * that the definitions below may change after the vector is obtained.
     *
     * @return a vector with all {@link Transformation} or {@link Derivation} objects. The vector
     *     may have zero size, if no such instances exist.
     */
    public java.util.List getDefinitionOfAKind(int type) {
        ArrayList result = new ArrayList();
        for (Iterator i = this.m_definitionList.iterator(); i.hasNext(); ) {
            Definition d = (Definition) i.next();
            if (d.getType() == type) {
                result.add(d);
            }
        }

        return result;
    }

    /**
     * Accessor: Obtains a <code>Definition</code> at a particular place in this container.
     *
     * @param index is the place to look up the element.
     * @return the <code>Definition</code> at the specified place.
     * @throws IndexOutOfBoundsException if the referenced position does not exist.
     * @see Definition
     */
    public Definition getDefinition(int index) throws IndexOutOfBoundsException {
        // -- check bound for index
        if ((index < 0) || (index >= this.m_definitionList.size()))
            throw new IndexOutOfBoundsException();

        return (Definition) this.m_definitionList.get(index);
    }

    /**
     * Accessor: Obtains all {@link Definition}s available. This array is a copy to avoid
     * write-through modifications.
     *
     * @return an array containing either a {@link Transformation} or {@link Derivation} at each
     *     position.
     * @deprecated Use the new Collection based interfaces
     */
    public Definition[] getDefinition() {
        int size = this.m_definitionList.size();
        Definition[] mDefinition = new Definition[size];
        System.arraycopy(this.m_definitionList.toArray(new Definition[0]), 0, mDefinition, 0, size);
        return mDefinition;
    }

    /**
     * Accessor: Counts the number of {@link Transformation} and {@link Derivation} definitions.
     *
     * @return item count.
     */
    public int getDefinitionCount() {
        return this.m_definitionList.size();
    }

    /**
     * Accessor: Obtains all {@link Definition}s available. This list is read-only.
     *
     * @return an array containing either a {@link Transformation} or {@link Derivation} at each
     *     position.
     */
    public java.util.List getDefinitionList() {
        return Collections.unmodifiableList(this.m_definitionList);
    }

    /**
     * Accessor: Obtains the document namespace.
     *
     * @return the namespace of the document, or null, if not used.
     * @see #setVdlns(java.lang.String)
     */
    public String getVdlns() {
        return this.m_vdlns;
    }

    /**
     * Accessor: Obtains the document version number.
     *
     * @return the version number from the document header, or null, if unset. Since the version
     *     number is a required attribute, it should never return null, only an empty string.
     */
    public String getVersion() {
        return this.m_version;
    }

    /**
     * Accessor: Provides an iterator for the list of {@link Transformation} and {@link Derivation}
     * references.
     *
     * @return a list iterator to traverse the container of {@link Definition}s.
     * @see java.util.ListIterator
     */
    public Iterator iterateDefinition() {
        return this.m_definitionList.iterator();
    }

    /**
     * Accessor: Provides an iterator for the list of {@link Transformation} and {@link Derivation}
     * references.
     *
     * @return a list iterator to traverse the container of {@link Definition}s.
     * @see java.util.ListIterator
     */
    public ListIterator listIterateDefinition() {
        return this.m_definitionList.listIterator();
    }

    /**
     * Accessor: Provides an iterator for the list of {@link Transformation} and {@link Derivation}
     * references.
     *
     * @param start is the starting point of the iteration.
     * @return a list iterator to traverse the container of {@link Definition}s.
     * @see java.util.ListIterator
     */
    public ListIterator listIterateDefinition(int start) {
        return this.m_definitionList.listIterator(start);
    }

    /**
     * Accessor: Removes all definitions we know about.
     *
     * @see Definition
     */
    public void removeAllDefinition() {
        this.m_definitionList.clear();
    }

    /**
     * Accessor: Removes a definition. Each component in this vector with an index greater or equal
     * to the specified index is shifted downward to have an index one smaller than the value it had
     * previously. The size of this vector is decreased by 1.
     *
     * @param index is the position to remove the argument fragment from.
     * @return the removed Definition.
     * @exception ArrayIndexOutOfBoundsException if the index was invalid.
     * @see Definition
     */
    public Definition removeDefinition(int index) {
        return (Definition) this.m_definitionList.remove(index);
    }

    /**
     * Accessor: Removes a definition named by its reference. Removes the first occurrence of the
     * specified element in this Vector.
     *
     * @param d is a definition instance that originated from this list.
     * @return true, if the first occurance of the element was deleted, false, if there was nothing
     *     found to be removed.
     * @see Definition
     */
    public boolean removeDefinition(Definition d) {
        return this.m_definitionList.remove(d);
    }

    /**
     * Accessor: Sets the component at the specified index of this vector to be the specified
     * object. The previous component at that position is discarded. The index must be a value
     * greater than or equal to 0 and less than the current size of the vector.
     *
     * @param index is the postion at which to replace a {@link Definition}.
     * @param d is either a {@link Transformation} or {@link Derivation} to use for replacement.
     * @throws IndexOutOfBoundsException if the index was invalid.
     */
    public Definition setDefinition(int index, Definition d) throws IndexOutOfBoundsException {
        // -- check bounds for index
        if ((index < 0) || (index >= this.m_definitionList.size())) {
            throw new IndexOutOfBoundsException();
        }

        this.sanitizeDefinition(d);
        return (Definition) this.m_definitionList.set(index, d);
    }

    /**
     * Accessor: Replace all {@link Definition}s with a new list.
     *
     * @param definitionArray is an array of possibly mixed {@link Transformation} and {@link
     *     Derivation} elements.
     * @deprecated Use the new Collection based interfaces
     */
    public void setDefinition(Definition[] definitionArray) {
        this.m_definitionList.clear();
        this.m_definitionList.addAll(Arrays.asList(definitionArray));
    }

    /**
     * Accessor: Replace all {@link Definition}s with a new list.
     *
     * @param definitions is an collection of possibly mixed {@link Transformation} and {@link
     *     Derivation} elements.
     */
    public void setDefinition(Collection definitions) {
        this.m_definitionList.clear();
        this.m_definitionList.addAll(definitions);
    }

    /**
     * Accessor: Sets the document default namespace.
     *
     * @param vdlns is the new namespace to use. Note that the change will <b>not</b> be propagated
     *     to contained elememts.
     * @see #getVdlns()
     */
    public void setVdlns(String vdlns) {
        this.m_vdlns = vdlns;
    }

    /**
     * Accessor: Replaces the version number of the document.
     *
     * @param version is the new version number.
     * @see #getVersion()
     */
    public void setVersion(String version) {
        this.m_version = version;
    }

    /**
     * Dumps the content of the given element into a string. This function traverses all sibling
     * classes as necessary and converts the data into textual output.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toString(Writer stream) throws IOException {
        for (Iterator i = this.m_definitionList.iterator(); i.hasNext(); ) {
            ((Definition) i.next()).toString(stream);
        }
    }

    /**
     * Writes the header of the XML output. The output contains the special strings to start an XML
     * document, some comments, and the root element. The latter points to the XML schema via XML
     * Instances.
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
    public void writeXMLHeader(Writer stream, String indent, String namespace) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");

        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        stream.write(newline);

        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("<!-- generated: ");
        stream.write(Currently.iso8601(false));
        stream.write(" -->");
        stream.write(newline);

        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("<!-- generated by: ");
        stream.write(System.getProperties().getProperty("user.name", "unknown"));
        stream.write(" [");
        stream.write(System.getProperties().getProperty("user.region", "??"));
        stream.write("] -->");
        stream.write(newline);

        // start root element
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("definitions xmlns");
        if (namespace != null && namespace.length() > 0) {
            stream.write(':');
            stream.write(namespace);
        }
        stream.write("=\"");
        stream.write(SCHEMA_NAMESPACE);
        stream.write(
                "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"");
        stream.write(SCHEMA_NAMESPACE);
        stream.write(' ');
        stream.write(SCHEMA_LOCATION);
        stream.write('"');

        writeAttribute(stream, " vdlns=\"", this.m_vdlns);
        writeAttribute(stream, " version=\"", this.m_version);

        stream.write('>');
        if (indent != null) stream.write(newline);
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
        // write prefix
        writeXMLHeader(stream, indent, namespace);

        // optionally write content
        if (this.m_definitionList.size() > 0) {
            String newindent = indent == null ? null : indent + "  ";
            for (Iterator i = this.m_definitionList.iterator(); i.hasNext(); ) {
                ((Definition) i.next()).toXML(stream, newindent, namespace);
            }
        }

        // finish document
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write("</");
        if (namespace != null && namespace.length() > 0) {
            stream.write(namespace);
            stream.write(':');
        }
        stream.write("definitions>");
        stream.write(System.getProperty("line.separator", "\r\n"));
        stream.flush(); // this is the only time we flush
    }
}
