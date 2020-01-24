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
 * This is a base class which collects attributes that belong to <code>Transformation</code> and
 * <code>Derivation</code>.
 *
 * <p>Note: this class has a natural ordering that may be inconsistent with equals due to differing
 * implementations. The equal method will take object type and primary key triple into
 * consideration, making extensive null checks. The compareTo method compares the type and short ids
 * of the Definitions.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Transformation
 * @see Derivation
 */
public abstract class Definition extends VDL implements Comparable, Serializable {
    // common constants for quick type access
    /** This is the return value for {@link #getType()} from a {@link Transformation}. */
    public static final int TRANSFORMATION = 0x71077345; // shell oil

    /** This is the return value for {@link #getType()} from a {@link Derivation}. */
    public static final int DERIVATION = 0xCAFEBABE;

    /**
     * This is an abstract function that must be implemented by instantiable children, of which
     * currently only exist {@link Transformation} and {@link Derivation} siblings and derivatives.
     *
     * @return the integer representing the concrete definition type of a instance. The value of -1
     *     *might* be used to indicate an unknown type.
     */
    public abstract int getType();

    // common attributes from child elements
    /**
     * Each transformation and derivation resides in a namespace. Mind that namespaces are currently
     * flat. If you need to impose any kind of hierarchy, please do so yourself, e.g. use periods
     * between hierarchy intifiers. The namespace is part of the key identifying a logical
     * transformation. The default is <code>null</code>.
     *
     * @see Transformation
     * @see Derivation
     */
    private String m_namespace;

    /**
     * Each transformation and derivation can be identified by a name. For a transformation, this is
     * part of the logical transformation name. Derivations can no longer be anonymous.
     *
     * @see Transformation
     * @see Derivation
     */
    private String m_name;

    /**
     * Each transformation and derivation has a version associated with their definition. While a
     * version number is highly recommended for transformation, being part of the primary key triple
     * into the (future) transformation catalog, a derivation can remain without version. The
     * default is <code>null</code>.
     *
     * @see Transformation
     * @see Derivation
     */
    private String m_version;

    // AttributeGroup "DefinitionsAG"
    /** Yong's knowledgebase approach needs this. */
    private String m_description;
    /** Yong's knowledgebase approach needs this. */
    private String m_title;
    /** Yong's knowledgebase approach needs this. */
    private String m_keyword;
    /** Yong's knowledgebase approach needs this. */
    private String m_url;

    /** ctor: empty */
    public Definition() {
        this.m_namespace = null;
        this.m_version = null;
    }

    /**
     * Convenience ctor: name the definition. The name is part of a logical {@link Transformation}.
     * Note that a {@link Derivation} may remain anonymous. The namespace will be the default
     * namespace, or taken from the {@link Definitions}. The version remains unset.
     *
     * @param name is the name to be used for the defintion.
     */
    public Definition(String name) {
        this.m_namespace = null;
        this.m_version = null;
        this.m_name = name;
    }

    /**
     * Convenience ctor: name the definition. The name is part of a logical {@link Transformation}.
     * Note that a {@link Derivation} may remain anonymous. The version remains unset.
     *
     * @param namespace is the namespace the name resides in.
     * @param name is the name to be used for the defintion.
     */
    public Definition(String namespace, String name) {
        this.m_name = name;
        this.m_namespace = namespace;
        this.m_version = null;
    }

    /**
     * Convenience ctor: name the definition. The name is part of a logical {@link Transformation}.
     * Note that a {@link Derivation} may remain anonymous.
     *
     * @param namespace is the namespace the name resides in.
     * @param name is the name to be used for the defintion.
     * @param version is the version of this definition.
     */
    public Definition(String namespace, String name, String version) {
        this.m_name = name;
        this.m_namespace = namespace;
        this.m_version = version;
    }

    /**
     * Implementation of the {@link java.lang.Comparable} interface. Compares this object with the
     * specified object for order. Returns a negative integer, zero, or a positive integer as this
     * object is less than, equal to, or greater than the specified object. The definitions are
     * compared by their type, and by their short ids.
     *
     * @param o is the object to be compared
     * @return a negative number, zero, or a positive number, if the object compared against is less
     *     than, equals or greater than this object.
     * @exception ClassCastException if the specified object's type prevents it from being compared
     *     to this Object.
     */
    public int compareTo(Object o) {
        if (o instanceof Definition) {
            Definition d = (Definition) o;
            int diff = d.getType() - getType(); // order is important
            return (diff != 0 ? diff : d.shortID().compareTo(this.shortID()));
        } else {
            throw new ClassCastException("object is not a Definition");
        }
    }

    /**
     * Calculate a hash code value for the object to support hash tables.
     *
     * @return a hash code value for the object.
     */
    public int hashCode() {
        int result = m_namespace == null ? 0 : m_namespace.hashCode();
        result = (result << 8) ^ (m_name == null ? 0 : m_name.hashCode());
        result = (result << 8) ^ (m_version == null ? 0 : m_version.hashCode());
        return (result ^ getType());
    }

    /**
     * Accessor: match the primary key of a definition. Note, this match is not wildcard capable.
     * The type of the definitions will also be checked. The primary key of a definition is the
     * triple namespace, name and version. This function is null-capable.
     *
     * @param type is the type identifier TRANSFORMATION or DERIVATION
     * @param namespace is the namespace
     * @param name is the name
     * @param version is the version
     * @return true, if the primary keys match, false otherwise.
     */
    public boolean match(int type, String namespace, String name, String version) {
        return ( // check type
        type == this.getType()
                &&
                // check namespace
                (m_namespace == null && namespace == null
                        || m_namespace != null
                                && namespace != null
                                && namespace.equals(m_namespace))
                &&
                // check name
                (m_name != null && name != null && name.equals(m_name))
                &&
                // check version string
                (m_version == null && version == null
                        || m_version != null && version != null && version.equals(m_version)));
    }

    /**
     * Accessor: match primary keys of two Definitions. Note, this match is not wildcard capable.
     * The type of the definitions will also be checked. The primary key of a definition is the
     * triple namespace, name and version. The equals function is null-capable.
     *
     * @param obj the reference object with which to compare.
     * @return true, if the primary keys match, false otherwise.
     */
    public boolean equals(Object obj) {
        // ward against null
        if (obj == null) return false;

        // shortcut
        if (obj == this) return true;

        // don't compare apples with oranges
        if (!(obj instanceof Definition)) return false;

        // now we can safely cast
        Definition d = (Definition) obj;
        return match(d.getType(), d.getNamespace(), d.getName(), d.getVersion());
    }

    /**
     * Accessor: Obtains the current description state.
     *
     * @return a string containing a descriptive remark on the definition, or null for no
     *     description.
     * @see #setDescription(java.lang.String)
     */
    public String getDescription() {
        return this.m_description;
    }

    /**
     * Accessor: Obtains the current keyword state.
     *
     * @return a string containing a collection of keywords describing the definition, or null for
     *     no keywords.
     * @see #setKeyword(java.lang.String)
     */
    public String getKeyword() {
        return this.m_keyword;
    }

    /**
     * Accessor: Obtains the current name of the definition. Note that a name is mandatory for any
     * {@link Transformation}, but a {@link Derivation} may remain anonymous.
     *
     * @return the current name used for the definition. Note that derivations may be anonymous.
     *     Returns null, if no name exists.
     * @see #setName(java.lang.String)
     */
    public String getName() {
        return this.m_name;
    }

    /**
     * Accessor: Obtains the current namespace that is used for the definition. Note that a
     * namespace is part of the key for any {@link Transformation}.
     *
     * @return the namespace the definition resides in, or null, if no namespace was defined.
     * @see #setNamespace(java.lang.String)
     */
    public String getNamespace() {
        return this.m_namespace;
    }

    /**
     * Accessor: Obtains the current title state.
     *
     * @return the title given to this definition, or null, if there was no title defined.
     * @see #setTitle(java.lang.String)
     */
    public String getTitle() {
        return this.m_title;
    }

    /**
     * Accessor: Obtains the current URI definition.
     *
     * @return the URL pointing to related information or a project, or null, if no URL was
     *     registered.
     * @see #setUrl(java.lang.String)
     */
    public String getUrl() {
        return this.m_url;
    }

    /**
     * Accessor: Obtains the current version of the definition. A version is an integral part of a
     * logical {@link Transformation}.
     *
     * @return the version number of this definition, or null, if no version number was defined.
     * @see #setVersion(java.lang.String)
     */
    public String getVersion() {
        return this.m_version;
    }

    /**
     * Accessor: Sets the description.
     *
     * @param description
     * @see #getDescription()
     */
    public void setDescription(String description) {
        this.m_description = description;
    }

    /**
     * Accessor: Sets or overwrites the keyword list.
     *
     * @param keyword
     * @see #getKeyword()
     */
    public void setKeyword(String keyword) {
        this.m_keyword = keyword;
    }

    /**
     * Accessor: Sets or overwrite the currently given name.
     *
     * @param name
     * @see #getName()
     */
    public void setName(String name) {
        this.m_name = name;
    }

    /**
     * Accessor: Sets or overwrites the namespace identifier.
     *
     * @param namespace
     * @see #getNamespace()
     */
    public void setNamespace(String namespace) {
        this.m_namespace = namespace;
    }

    /**
     * Accessor: Sets the current title for the definition.
     *
     * @param title
     * @see #getTitle()
     */
    public void setTitle(String title) {
        this.m_title = title;
    }

    /**
     * Accessor: Sets the project reference.
     *
     * @param url
     * @see #getUrl()
     */
    public void setUrl(String url) {
        this.m_url = url;
    }

    /**
     * Accessor: Sets the version of the definition.
     *
     * @param version
     * @see #getVersion()
     */
    public void setVersion(String version) {
        this.m_version = version;
    }

    /** Identify the transformation or derivation by its name. */
    public abstract String identify();

    /**
     * Create the short id from ns:id:version.
     *
     * @param d is a Definition, or null for non-siblings
     * @param namespace is the namespace to use, may be null.
     * @param name is the name to produce the id for, should not be null.
     * @param version is a version string, may be null.
     * @return A string which textually identifies a Definition.
     * @exception RuntimeException, if the name and definition are both null.
     */
    public static String shortID(Definition d, String namespace, String name, String version) {
        if (name != null) return Separator.combine(namespace, name, version);
        else if (d != null)
            return Separator.combine(namespace, Integer.toHexString(d.hashCode()), version);
        else throw new RuntimeException("Definitions require valid identifiers");
    }

    /**
     * Constructs dynamically a short descriptive, hopefully unique identifier for this derivation
     * w/o referring to any transformation. FIXME: Anonymous derivations get their hash code, which
     * is well for the first versions working without database. Later versions with database must
     * use some unique sequence mechanism instead.
     *
     * @return a string describing the derivation
     * @see Object#hashCode()
     */
    public String shortID() {
        return shortID(this, this.m_namespace, this.m_name, this.m_version);
    }

    /**
     * The toXML method is a partial method, to be incorporated/called by its sibling class method
     * of the same name. For this reason, it does not fit the {@link VDL} interface.
     *
     * @return a string containing the attributes collected in the base class.
     */
    public String toXML() {
        StringBuffer result = new StringBuffer();

        if (this.m_namespace != null)
            result.append(" namespace=\"").append(quote(this.m_namespace, true)).append("\"");

        if (this.m_name != null)
            result.append(" name=\"").append(quote(this.m_name, true)).append("\"");

        if (this.m_version != null)
            result.append(" version=\"").append(quote(this.m_version, true)).append("\"");

        if (this.m_description != null)
            result.append(" description=\"").append(quote(this.m_description, true)).append("\"");

        if (this.m_title != null)
            result.append(" title=\"").append(quote(this.m_title, true)).append("\"");

        if (this.m_keyword != null)
            result.append(" keyword=\"").append(quote(this.m_keyword, true)).append("\"");

        if (this.m_url != null)
            result.append(" url=\"").append(quote(this.m_url, true)).append("\"");

        return result.toString();
    }

    /**
     * The toXML method is a partial method, to be incorporated/called by its sibling class method
     * of the same name. For this reason, it does not fit the {@link VDL} interface.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer stream) throws IOException {
        writeAttribute(stream, " namespace=\"", this.m_namespace);
        writeAttribute(stream, " name=\"", this.m_name);
        writeAttribute(stream, " version=\"", this.m_version);
        writeAttribute(stream, " description=\"", this.m_description);
        writeAttribute(stream, " title=\"", this.m_title);
        writeAttribute(stream, " keyword=\"", this.m_keyword);
        writeAttribute(stream, " url=\"", this.m_url);
    }
}
