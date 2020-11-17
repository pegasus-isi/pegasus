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

import edu.isi.pegasus.common.util.Separator;
import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.util.*;
import org.griphyn.vdl.util.SequenceGenerator;

/**
 * <code>Call</code> is an implementation of an anonymous <code>Derivation</code>. A call describes
 * the mutable part concerning input, processing, and output (IPO). Calls can only be part of a
 * <code>CompoundTransformation</code>.
 *
 * <p>A call parametrizes the template provided by a <code>Transformation</code> with actual values.
 * Think of a call as something akin to a C function call. The call provides the actual parameter to
 * a job. The immutable parts are hidden in a <code>Transformation</code>.
 *
 * <p>FIXME: A <code>Call</code> is essentially an anonymous <code>Derivation</code> that occurs
 * within a <code>CompoundTransformation</code>. Thus, the first two should share code. Also, the
 * latter two already share code. Therefore, the class hierarchies need to be re-designed, and
 * attribute groups should be out-sourced.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision $
 * @see Definition
 * @see Definitions
 * @see Transformation
 * @see Derivation
 */
public class Call extends VDL implements HasPass, Serializable {
    /**
     * Though most <code>Call</code>s may have a name of their own, most of the times, though, calls
     * are anonymous. A call must provide the name of the <code>Transformation</code> that it calls,
     * though.
     *
     * @see Definition
     * @see Transformation
     */
    private String m_uses;

    /**
     * The namespace in which a call resides can differ from the namespace that the transformation
     * lives in. This argument provides the namespace of the <code>Transformation</code> to call.
     *
     * @see Definition
     * @see Transformation
     */
    private String m_usesspace;

    /**
     * Any <code>Transformation</code> may exist in multiple versions. This argument specifies the
     * minimum permissable version that can be used. FIXME: versioning is not really supported.
     */
    private String m_maxIncludeVersion;

    /**
     * Any <code>Transformation</code> may exist in multiple versions. This argument specifies the
     * maximum permissable version that can be used. FIXME: versioning is not really supported.
     */
    private String m_minIncludeVersion;

    /**
     * Actual arguments used when calling a {@link Transformation} are matched up with the formal
     * arguments of the transformation by their names.
     */
    private TreeMap m_passMap;

    /**
     * Since <code>Call</code> is an anonymous <code>Derivation</code>, we still need some unique
     * identifiers for the call. This is a sequence generator.
     */
    private static SequenceGenerator s_sequence = new SequenceGenerator();

    /** This is the sequence number assigned by the c'tor to this call. */
    private String m_id;

    /** ctor. */
    public Call() {
        this.m_id = "anon" + Call.s_sequence.generate();
        this.m_passMap = new TreeMap();
    }

    /**
     * Convenience ctor: Names the used <code>Transformation</code>
     *
     * @param uses is the name of the <code>Transformation</code>
     * @see Transformation
     */
    public Call(String uses) {
        this.m_id = "anon" + Call.s_sequence.generate();
        this.m_passMap = new TreeMap();
        this.m_uses = uses;
    }

    /**
     * Convenience ctor: Supplies the used <code>Transformation</code> as well as the permissable
     * version range.
     *
     * @param uses is the name of the <code>Transformation</code>.
     * @param min is the minimum inclusive permissable version.
     * @param max is the maximum inclusive permissable version.
     * @see Transformation
     */
    public Call(String uses, String min, String max) {
        this.m_id = "anon" + Call.s_sequence.generate();
        this.m_passMap = new TreeMap();
        this.m_uses = uses;
        this.m_minIncludeVersion = min;
        this.m_maxIncludeVersion = max;
    }

    /**
     * Accessor: Adds an actual argument to the bag of arguments.
     *
     * @param vPass is the new actual argument to add.
     * @see Pass
     */
    public void addPass(Pass vPass) {
        this.m_passMap.put(vPass.getBind(), vPass);
    }

    /*
     * won't work with maps
     *
    public void addPass( int index, Pass vPass )
      throws IndexOutOfBoundsException
    { this.m_passList.insertElementAt(vPass, index); }
     */

    /**
     * Accessor: Provides an iterator for the bag of actual arguments.
     *
     * @return the iterator for <code>Pass</code> elements.
     * @see Pass
     * @see java.util.Enumeration
     * @deprecated Use the new Collection based interfaces
     */
    public Enumeration enumeratePass() {
        // return this.m_passMap.elements();
        return Collections.enumeration(this.m_passMap.values());
    }

    /**
     * Determines all LFN instances of a given scalar that match the specified linkage. This is a
     * higher-level method employing the given API. Note that also linkage of NONE will not be found
     * in wildcard search mode.
     *
     * @param linkage is the linkage to check for, -1 for any linkage.
     * @return a set of all logical filenames that match the linkage and were part of the scalar.
     *     The result may be an empty set, if no such result were to be found. For a linkage of -1,
     *     complete LFNs will be returned, for any other linkage, just the filename will be
     *     returned.
     * @see Value#getLFNList( int )
     * @see LFN
     */
    public java.util.List getLFNList(int linkage) {
        java.util.List result = new ArrayList();

        for (Iterator i = this.iteratePass(); i.hasNext(); ) {
            Value actual = ((Pass) i.next()).getValue();
            result.addAll(actual.getLFNList(linkage));
        }

        return result;
    }

    /**
     * Determines if the list contains an LFN of the specified linkage. The logic uses short-circuit
     * evaluation, thus finding things is faster than not finding things. Searching a list is a
     * potentially expensive method.
     *
     * @param filename is the name of the LFN
     * @param linkage is the linkage to check for, -1 for any linkage type.
     * @return true if the LFN is contained in the scalar, false otherwise.
     * @see Value#containsLFN( String, int )
     * @see LFN
     */
    public boolean containsLFN(String filename, int linkage) {
        for (Iterator i = this.iteratePass(); i.hasNext(); ) {
            Value actual = ((Pass) i.next()).getValue();
            if (actual.containsLFN(filename, linkage)) return true;
        }

        return false;
    }

    /**
     * Accessor: Obtains the maximum inclusive version permissable for binding to a {@link
     * Transformation}.
     *
     * @return the maximum inclusive version number.
     * @see #setMaxIncludeVersion( java.lang.String )
     */
    public String getMaxIncludeVersion() {
        return this.m_maxIncludeVersion;
    }

    /**
     * Accessor: Obtains the minimum inclusive version permissable for binding to a {@link
     * Transformation}.
     *
     * @return the minimum inclusive version number.
     * @see #setMinIncludeVersion( java.lang.String )
     */
    public String getMinIncludeVersion() {
        return this.m_minIncludeVersion;
    }

    /**
     * Accessor: Obtains an actual argument identified by the bound variable.
     *
     * @param name is the binding name.
     * @return the bound value to the given name.
     * @see Pass
     */
    public Pass getPass(String name) {
        return (Pass) this.m_passMap.get(name);
    }

    /**
     * Accessor: Obtains the bag of actual arguments as array. Note that the order is arbitrary.
     *
     * @return an array containing all bound variables.
     * @see Pass
     * @deprecated Use the new Collection based interfaces
     */
    public Pass[] getPass() {
        int size = this.m_passMap.size();
        Pass[] mPass = new Pass[size];
        this.m_passMap.values().toArray(mPass);
        return mPass;
    }

    /**
     * Accessor: Counts the number of actual arguments.
     *
     * @return the number of actual arguments in the internal bag.
     */
    public int getPassCount() {
        return this.m_passMap.size();
    }

    /**
     * Accessor: Gets an array of all values that constitute the current content. This list is
     * read-only.
     *
     * @return an array with <code>Pass</code> elements.
     * @see Pass
     */
    public java.util.List getPassList() {
        return Collections.unmodifiableList(new ArrayList(this.m_passMap.values()));
    }

    /**
     * Accessor: Obtains all actual arguments. The map is a read-only map to avoid modifications
     * outside the API.
     *
     * @return a map will all actual arguments.
     * @see Pass
     */
    public java.util.Map getPassMap() {
        return Collections.unmodifiableMap(this.m_passMap);
    }

    /**
     * Accessor: Provides an iterator for the bag of actual arguments.
     *
     * @return an iterator to walk the <code>Pass</code> list with.
     * @see Pass
     */
    public Iterator iteratePass() {
        return this.m_passMap.values().iterator();
    }

    /* NOT APPLICABLE
     *
     * Accessor: Provides an iterator for the bag of actual arguments.
     * @return an iterator to walk the <code>Pass</code> list with.
     * @see Pass
     *
    public ListIterator listIteratePass()
    {
      return (new ArrayList( this.m_passMap.values() ).listIterator());
    }
     */

    /**
     * Accessor: Obtains the name of the logical {@link Transformation} that this call refers to.
     *
     * @see #setUses( java.lang.String )
     */
    public java.lang.String getUses() {
        return this.m_uses;
    }

    /**
     * Accessor: Obtains the namespace of the logical {@link Transformation} that this call refers
     * to.
     *
     * @see #setUsesspace( java.lang.String )
     */
    public java.lang.String getUsesspace() {
        return this.m_usesspace;
    }

    /**
     * Instance method for matching an external version against the inclusive version range.
     *
     * @param version is an externally supplied version to be checked, if it is within the inclusive
     *     interval of min and max.
     * @return true, if the version is in range, false otherwise.
     * @see Derivation#match( String, String, String )
     */
    public boolean match(String version) {
        return Derivation.match(version, this.m_minIncludeVersion, this.m_maxIncludeVersion);
    }

    /** Accessor: Removes all actual arguments. Effectively empties the bag. */
    public void removeAllPass() {
        this.m_passMap.clear();
    }

    /**
     * Accessor: Removes a specific actual argument.
     *
     * @param name is the bound variable name of the argument to remove.
     * @return the object that was removed, or null, if not found.
     * @see Pass
     */
    public Pass removePass(String name) {
        return (Pass) this.m_passMap.remove(name);
    }

    /**
     * Accessor: Sets the maximum inclusive permissable version of a logical transformation to run
     * with.
     *
     * @param miv is the (new) maximum inclusive version.
     * @see #getMaxIncludeVersion()
     */
    public void setMaxIncludeVersion(String miv) {
        this.m_maxIncludeVersion = miv == null ? null : miv.trim();
    }

    /**
     * Accessor: Sets the minimum inclusive permissable version of a logical transformation to run
     * with.
     *
     * @param miv is the (new) minimum inclusive version.
     * @see #getMinIncludeVersion()
     */
    public void setMinIncludeVersion(String miv) {
        this.m_minIncludeVersion = miv == null ? null : miv.trim();
    }

    /**
     * Accessor: Adds a new or overwrites an existing actual argument.
     *
     * @param vPass is a new actual argument with bound name and value.
     * @see Pass
     */
    public void setPass(Pass vPass) {
        this.m_passMap.put(vPass.getBind(), vPass);
    }

    /**
     * Accessor: Replaces the bag of actual argument with new arguments.
     *
     * @param passArray is the new actual argument list.
     * @see Pass
     * @deprecated Use the new Collection based interfaces
     */
    public void setPass(Pass[] passArray) {
        // -- copy array
        this.m_passMap.clear();
        for (int i = 0; i < passArray.length; i++) {
            this.m_passMap.put(passArray[i].getBind(), passArray[i]);
        }
    }

    /**
     * Accessor: Replaces the bag of actual argument with a bag of new arguments.
     *
     * @param passes is the new actual argument collection.
     * @see Pass
     */
    public void setPass(Collection passes) {
        this.m_passMap.clear();
        for (Iterator i = passes.iterator(); i.hasNext(); ) {
            Pass p = (Pass) i.next();
            this.m_passMap.put(p.getBind(), p);
        }
    }

    /**
     * Accessor: Replaces the bag of actual argument with a map of new arguments.
     *
     * @param passes is the new actual argument map.
     * @see Pass
     */
    public void setPass(Map passes) {
        this.m_passMap.clear();
        this.m_passMap.putAll(passes);
    }

    /**
     * Accessor: Sets a new name for a logical <code>Transformation</code> to call.
     *
     * @param uses is the new name of the <code>Transformation</code> to use.
     * @see #getUses()
     * @see Transformation
     */
    public void setUses(String uses) {
        this.m_uses = uses;
    }

    /**
     * Accessor: Sets a new namespace identifier for a logical <code>Transformation</code> to call.
     *
     * @param usesspace is the new namespace of the <code>Transformation</code>.
     * @see #getUsesspace()
     * @see Transformation
     */
    public void setUsesspace(String usesspace) {
        this.m_usesspace = usesspace;
    }

    /**
     * Generates a pseudo id for this Call. FIXME: With the advent of a database, we'll need to fix
     * this to something like the primary key.
     */
    public String shortID() {
        return this.m_id;
    }

    /**
     * Since calls are anonymous derivations, this function can only construct the mapped
     * transformation
     *
     * @return a string describing the call
     */
    public String identify() {
        StringBuffer result = new StringBuffer();
        result.append(this.m_id);
        result.append("->");

        // and now for the called part
        result.append(
                Separator.combine(
                        this.m_usesspace,
                        this.m_uses,
                        getMinIncludeVersion(),
                        getMaxIncludeVersion()));

        // result
        return result.toString();
    }

    /**
     * Dumps the content of the given element into a string. This function traverses all sibling
     * classes as necessary and converts the data into textual output. Note that order of the actual
     * arguments is not preserved.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toString(Writer stream) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");

        String me = this.identify();
        stream.write("call ");
        stream.write(me.substring(me.indexOf("->") + 2));
        stream.write('(');

        if (this.m_passMap.size() > 0) {
            stream.write(newline);
            for (Iterator i = this.m_passMap.values().iterator(); i.hasNext(); ) {
                stream.write('\t');
                ((Pass) i.next()).toString(stream);
                if (i.hasNext()) stream.write("," + newline);
            }
        }

        stream.write(" )");
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
     * @see org.griphyn.vdl.Chimera#writeAttribute( Writer, String, String )
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream, String indent, String namespace) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":call" : "call";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " usesspace=\"", this.m_usesspace);
        writeAttribute(stream, " uses=\"", this.m_uses);
        writeAttribute(stream, " minIncludeVersion=\"", this.m_minIncludeVersion);
        writeAttribute(stream, " maxIncludeVersion=\"", this.m_maxIncludeVersion);

        if (this.m_passMap.size() == 0) {
            // empty argument list
            stream.write("/>");
        } else {
            stream.write('>');
            if (indent != null) stream.write(newline);

            // dump content
            String newindent = indent == null ? null : indent + "  ";
            for (Iterator i = this.m_passMap.values().iterator(); i.hasNext(); ) {
                ((Pass) i.next()).toXML(stream, newindent, namespace);
            }

            if (indent != null && indent.length() > 0) stream.write(indent);
            stream.write("</");
            stream.write(tag);
            stream.write('>');
        }
        if (indent != null) stream.write(newline);
    }
}
