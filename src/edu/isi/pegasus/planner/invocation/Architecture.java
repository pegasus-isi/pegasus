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
package edu.isi.pegasus.planner.invocation;

import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * This class is transient for XML parsing. The data value will be incorporated into the job
 * classes.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see StatCall
 */
public class Architecture extends Invocation implements HasText {
    /** This is the data contained between the tags. A <code>null</code> value is not valid. */
    private StringBuffer m_value;

    /**
     * Describes the architecture runtime mode. For instance, on a SPARC can run in ILP32 or LP64
     * mode, an IA64 may have a backward-compatible 32bit mode (IA32), etc.
     */
    private String m_archmode;

    /** Describes the operating system name. For instance: linux, sunos, ... */
    private String m_sysname;

    /**
     * Describes the machine's network node hostname. Note that incorrect host setup may include the
     * domainname into this.
     */
    private String m_nodename;

    /** Contains the operating system's version string. */
    private String m_release;

    /** Contains the machine's hardware description. For instance: i686, sun4u, ... */
    private String m_machine;

    /**
     * Contains the optional domain name on the network. Note that incorrect setup of the host name
     * may contain the domain portion there.
     */
    private String m_domainname;

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public Architecture() {
        m_value = null;
        m_sysname = m_archmode = m_nodename = m_release = m_machine = m_domainname = null;
    }

    /**
     * Appends a piece of text to the existing text.
     *
     * @param fragment is a piece of text to append to existing text. Appending <code>null</code> is
     *     a noop.
     */
    public void appendValue(String fragment) {
        if (fragment != null) {
            if (this.m_value == null) this.m_value = new StringBuffer(fragment);
            else this.m_value.append(fragment);
        }
    }

    /**
     * Accessor
     *
     * @see #setArchMode(String)
     */
    public String getArchMode() {
        return this.m_archmode;
    }

    /**
     * Accessor.
     *
     * @param archmode
     * @see #getArchMode()
     */
    public void setArchMode(String archmode) {
        this.m_archmode = archmode;
    }

    /**
     * Accessor
     *
     * @see #setValue(String)
     */
    public String getValue() {
        return (m_value == null ? null : m_value.toString());
    }

    /**
     * Accessor.
     *
     * @param value is the new value to set.
     * @see #getValue()
     */
    public void setValue(String value) {
        this.m_value = (value == null ? null : new StringBuffer(value));
    }

    /**
     * Accessor
     *
     * @see #setSystemName(String)
     */
    public String getSystemName() {
        return this.m_sysname;
    }

    /**
     * Accessor.
     *
     * @param sysname
     * @see #getSystemName()
     */
    public void setSystemName(String sysname) {
        this.m_sysname = sysname;
    }

    /**
     * Accessor
     *
     * @see #setNodeName(String)
     */
    public String getNodeName() {
        return this.m_nodename;
    }

    /**
     * Accessor.
     *
     * @param nodename
     * @see #getNodeName()
     */
    public void setNodeName(String nodename) {
        this.m_nodename = nodename;
        this.normalize();
    }

    /**
     * Accessor
     *
     * @see #setRelease(String)
     */
    public String getRelease() {
        return this.m_release;
    }

    /**
     * Accessor.
     *
     * @param release
     * @see #getRelease()
     */
    public void setRelease(String release) {
        this.m_release = release;
    }

    /**
     * Accessor
     *
     * @see #setDomainName(String)
     */
    public String getDomainName() {
        return this.m_domainname;
    }

    /**
     * Accessor.
     *
     * @param domainname
     * @see #getDomainName()
     */
    public void setDomainName(String domainname) {
        this.m_domainname = domainname;
    }

    /**
     * Accessor
     *
     * @see #setMachine(String)
     */
    public String getMachine() {
        return this.m_machine;
    }

    /**
     * Accessor.
     *
     * @param machine
     * @see #getMachine()
     */
    public void setMachine(String machine) {
        this.m_machine = machine;
    }

    /** Normalizes a misconfigured nodename that contains a domainname. */
    public void normalize() {
        int pos = this.m_nodename.indexOf('.');
        if (pos != -1 && this.m_domainname == null) {
            // normalize domain portion
            this.m_domainname = this.m_nodename.substring(pos + 1);
            this.m_nodename = this.m_nodename.substring(0, pos);
        }
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        throw new IOException("method not implemented, please contact vds-support@griphyn.org");
    }

    /**
     * Quotes an input string for XML attributes while converting nulls.
     *
     * @param s is the attribute string, may be null
     * @return the XML-quoted string, or an empty-but-not-null string.
     */
    private String myquote(String s) {
        if (s == null) return new String();
        else return quote(s, true);
    }

    /**
     * Dump the state of the current element as XML output. This function traverses all sibling
     * classes as necessary, and converts the data into pretty-printed XML output. The stream
     * interface should be able to handle large output efficiently.
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
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":uname" : "uname";

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        stream.write(" sysname=\"");
        stream.write(myquote(m_sysname));
        stream.write('\"');
        if (m_archmode != null) writeAttribute(stream, " archmode=\"", m_archmode);
        stream.write(" nodename=\"");
        stream.write(myquote(m_nodename));
        stream.write("\" release=\"");
        stream.write(myquote(m_release));
        stream.write("\" machine=\"");
        stream.write(myquote(m_machine));
        stream.write('\"');
        if (this.m_domainname != null) writeAttribute(stream, " domainname=\"", m_domainname);

        if (this.m_value != null) {
            stream.write('>');
            stream.write(quote(getValue(), false));
            stream.write("</");
            stream.write(tag);
            stream.write('>');
        } else {
            // no content
            stream.write("/>");
        }

        if (indent != null) stream.write(System.getProperty("line.separator", "\r\n"));
    }
}
