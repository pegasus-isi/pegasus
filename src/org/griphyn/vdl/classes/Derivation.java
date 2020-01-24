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

/**
 * <code>Derivation</code> is an implementation of an abstract VDL <code>Definition</code>. A
 * derivation describes the mutable part concerning input, processing, and output (IPO) when calling
 * an application. The environment is part of the capture.
 *
 * <p>A derivation parametrizes the template provided by a <code>Transformation</code> with actual
 * values. Think of a derivation as something akin to a C function call. The derivation provides the
 * actual parameter to a job. The immutable parts are hidden in a <code>Transformation</code>.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Definition
 * @see Definitions
 * @see Transformation
 */
public class Derivation extends Definition // thus implements VDL
        implements HasPass, Serializable {
    /**
     * Though most <code>Derivation</code>s may have a name of their own, most of the times, though,
     * derivations are anonymous. A derivation must provide the name of the <code>Transformation
     * </code> that it calls, though.
     *
     * @see Definition
     * @see Transformation
     */
    private String m_uses;

    /**
     * The namespace in which a derivation resides can differ from the namespace that the
     * transformation lives in. This argument provides the namespace of the <code>Transformation
     * </code> to call.
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
     * Type accessor for quick distinction between definitions.
     *
     * @return the value of {@link Definition#DERIVATION}
     */
    public int getType() {
        return Definition.DERIVATION;
    }

    /** ctor. */
    public Derivation() {
        super();
        this.m_passMap = new TreeMap();
    }

    /**
     * Convenience ctor: Names a derivation and the used <code>Transformation</code>
     *
     * @param name is the name of the <code>Derivation</code>
     * @param uses is the name of the <code>Transformation</code>
     * @see Transformation
     */
    public Derivation(String name, String uses) {
        super(name);
        this.m_passMap = new TreeMap();
        this.m_uses = uses;
    }

    /**
     * Convenience ctor: Names a derivation and supplies the used <code>Transformation</code> as
     * well as the permissable version range.
     *
     * @param name is the name of the <code>Derivation</code>.
     * @param uses is the name of the <code>Transformation</code>.
     * @param min is the minimum inclusive permissable version.
     * @param max is the maximum inclusive permissable version.
     * @see Transformation
     */
    public Derivation(String name, String uses, String min, String max) {
        super(name);
        this.m_passMap = new TreeMap();
        this.m_uses = uses;
        this.m_minIncludeVersion = min;
        this.m_maxIncludeVersion = max;
    }

    /**
     * Complete ctor: Constructs the full three part <code>Derivation</code> identifier, and four
     * part <code>Transformation</code> mapper.
     *
     * @param ns is then namespace of the <code>Derivation</code>.
     * @param name is the name of the <code>Derivation</code>.
     * @param version is the version of the <code>Derivation</code>.
     * @param us is the namespace to search for a <code>Transformation</code>.
     * @param uses is the name of the <code>Transformation</code>.
     * @param min is the minimum inclusive permissable version.
     * @param max is the maximum inclusive permissable version.
     * @see Transformation
     */
    public Derivation(
            String ns,
            String name,
            String version,
            String us,
            String uses,
            String min,
            String max) {
        super(ns, name, version);
        this.m_usesspace = us;
        this.m_uses = uses;
        this.m_minIncludeVersion = min;
        this.m_maxIncludeVersion = max;
        this.m_passMap = new TreeMap();
    }

    /**
     * Convenience ctor: Names the derivation and supplies the used <code>Transformation</code>, and
     * the first actual argument.
     *
     * @param name is the name of the <code>Derivation</code>.
     * @param uses is the name of the <code>Transformation</code>.
     * @param firstChild is a first (possibly only) actual argument.
     * @see Transformation
     * @see Pass
     */
    public Derivation(String name, String uses, Pass firstChild) {
        super(name);
        this.m_passMap = new TreeMap();
        this.m_passMap.put(firstChild.getBind(), firstChild);
        this.m_uses = uses;
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
     * Determines all LFN instances from the actual arguments of a given derivation that match the
     * specified linkage. This is a higher-level method employing the given interface. Note that
     * also linkage of NONE will not be found in wildcard search mode.
     *
     * @param linkage is the linkage type to match against, -1 for all files.
     * @return a list of logical filenames from the given derivation which match the given linkage.
     *     For a linkage of -1, complete LFNs will be returned, for any other linkage, just the
     *     filename will be returned.
     * @see Value#getLFNList( int )
     * @see LFN
     */
    public java.util.List getLFNList(int linkage) {
        java.util.List result = new ArrayList();

        for (Iterator i = this.iteratePass(); i.hasNext(); ) {
            Value value = ((Pass) i.next()).getValue();
            result.addAll(value.getLFNList(linkage));
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
     * Accessor: Obtains the name of the logical {@link Transformation} that this derivation refers
     * to.
     *
     * @see #setUses( java.lang.String )
     */
    public java.lang.String getUses() {
        return this.m_uses;
    }

    /**
     * Accessor: Obtains the namespace of the logical {@link Transformation} that this derivation
     * refers to.
     *
     * @see #setUsesspace( java.lang.String )
     */
    public java.lang.String getUsesspace() {
        return this.m_usesspace;
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
     * Matches an external version string against the internal range. This predicate function uses
     * inclusive matches. Special interpretation will be applied to <code>null</code> values,
     * internally as well as an external null value.
     *
     * <p>
     *
     * <pre>
     *   vers.   min    max     result
     *   -----   ----   -----   ------
     *   null    null   null    true
     *   null    *      null    true
     *   null    null   *       true
     *   null    *      *       true
     *
     *   *       null   null    true
     *
     *   "A"     "B"    null    false
     *   "B"     "B"    null    true
     *   "C"     "B"    null    true
     *   "A"     null   "B"     true
     *   "B"     null   "B"     true
     *   "C"     null   "B"     false
     *   "A"     "B"    "B"     false
     *   "B"     "B"    "B"     true
     *   "C"     "B"    "B"     false
     * </pre>
     *
     * @param version is an externally supplied version to be checked, if it is within the inclusive
     *     interval of min and max.
     * @param minInc is the minimum inclusive version of the range.
     * @param maxInc is the maximum inclusive version of the range.
     * @return true, if the version is in range, false otherwise.
     */
    public static boolean match(String version, String minInc, String maxInc) {
        // special null combinations first.
        if (minInc == null && maxInc == null || version == null) return true;

        String ver = version.trim();
        String min = minInc == null ? "" : minInc;
        String max = maxInc == null ? "" : maxInc;
        return (ver.compareTo(min) >= 0 && ver.compareTo(max) <= 0);
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
     * Constructs dynamically a short descriptive, hopefully unique identifier for this derivation.
     * Recent modification add the complete identification in terms of derivation name, namespace,
     * and version as well as the called transformation name, namespace and version range. FIXME:
     * Anonymous derivations get their hash code, which is well for the first versions working
     * without database. Later versions with database must use some unique sequence mechanism
     * instead.
     *
     * @return a string describing the derivation
     * @see Object#hashCode()
     */
    public String identify() {
        StringBuffer result = new StringBuffer();

        result.append(shortID());
        result.append("->");
        result.append(
                Separator.combine(
                        this.m_usesspace,
                        this.m_uses,
                        this.getMinIncludeVersion(),
                        this.getMaxIncludeVersion()));

        //     // and now for the called part
        //     result.append( shortID(null, this.m_usesspace, this.m_uses, null) );
        //
        //     String vmin = this.getMinIncludeVersion();
        //     String vmax = this.getMaxIncludeVersion();
        //     if ( vmin != null && vmin.length() > 0 &&
        // 	 vmax != null && vmax.length() > 0 ) {
        //       result.append(Separator.NAME);
        //       if ( vmin != null ) result.append(vmin);
        //       result.append(Separator.VERSION);
        //       if ( vmax != null ) result.append(vmax);
        //     }

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

        stream.write("DV ");
        stream.write(this.identify());
        stream.write('(');

        // write arguments
        if (this.m_passMap.size() > 0) {
            stream.write(newline);
            for (Iterator i = this.m_passMap.values().iterator(); i.hasNext(); ) {
                stream.write("  ");
                ((Pass) i.next()).toString(stream);
                if (i.hasNext()) stream.write("," + newline);
            }
        }

        stream.write(" );");
        stream.write(newline);
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
        String tag =
                (namespace != null && namespace.length() > 0)
                        ? namespace + ":derivation"
                        : "derivation";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        super.toXML(stream);
        writeAttribute(stream, " usesspace=\"", this.m_usesspace);
        writeAttribute(stream, " uses=\"", this.m_uses);
        writeAttribute(stream, " minIncludeVersion=\"", this.m_minIncludeVersion);
        writeAttribute(stream, " maxIncludeVersion=\"", this.m_maxIncludeVersion);

        if (this.m_passMap.size() == 0) {
            // no actual arguments
            stream.write("/>");
        } else {
            // there are actual arguments
            stream.write('>');
            if (indent != null) stream.write(newline);

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
