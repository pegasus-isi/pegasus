/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.invocation;

import java.io.IOException;
import java.io.Writer;

/**
 * The Machine element groups a time stamp, the page size, the generic utsname information, and a
 * machine-specific content collecting element.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public class Machine extends Invocation {
    /** element name */
    public static final String ELEMENT_NAME = "machine";

    /** The only attribute to the machine element is required. */
    private long m_pagesize;

    /** The time when the snapshot was taken. */
    private Stamp m_stamp;

    /** The uname child element is mandatory. */
    private Uname m_uname;

    /** This is a grouping element for the remaining machine-specific items. */
    private MachineSpecific m_specific;

    /** Default constructor. */
    public Machine() {
        m_pagesize = 0;
        m_stamp = null;
        m_uname = null;
        m_specific = null;
    }

    /**
     * Sets the page size.
     *
     * @param size is the remote page size in byte.
     */
    public void setPageSize(long size) {
        m_pagesize = size;
    }

    /**
     * Obtains the page size information.
     *
     * @return pagesize in byte
     */
    public long getPageSize() {
        return m_pagesize;
    }

    /**
     * Sets the time stamp when the machine info was obtained.
     *
     * @param stamp is the time stamp
     */
    public void setStamp(Stamp stamp) {
        m_stamp = stamp;
    }

    /**
     * Obtains the time stamp information when the remote machine element was recorded.
     *
     * @return stamp is a time stamp
     */
    public Stamp getStamp() {
        return m_stamp;
    }

    /**
     * Sets the utsname generic system information record.
     *
     * @param uname is the utsname record
     */
    public void setUname(Uname uname) {
        m_uname = uname;
    }

    /**
     * Obtains the utsname generic system information record.
     *
     * @return uname is the utsname record
     */
    public Uname getUname() {
        return m_uname;
    }

    /**
     * Sets the machine-specific grouping element.
     *
     * @param m is the machine specific grouping element
     */
    public void setMachineSpecific(MachineSpecific m) {
        m_specific = m;
    }

    /**
     * Obtains the machine-specific grouping element.
     *
     * @return machine
     */
    public MachineSpecific getMachineSpecific() {
        return m_specific;
    }

    /**
     * Returns the name of the xml element corresponding to the object.
     *
     * @return name
     */
    public String getElementName() {
        return ELEMENT_NAME;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        throw new IOException("method not implemented, please contact pegasus-support@isi.edu");
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
        String newLine = System.getProperty("line.separator", "\r\n");
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":" : "";
        tag = tag + getElementName();

        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);
        writeAttribute(stream, " page-size=\"", Long.toString(m_pagesize));
        stream.write('>');
        if (indent != null) stream.write(newLine);

        // dump content
        String newIndent = (indent == null) ? null : indent + "  ";
        m_stamp.toXML(stream, newIndent, namespace);
        m_uname.toXML(stream, newIndent, namespace);
        m_specific.toXML(stream, newIndent, namespace);

        // close element
        stream.write("</");
        stream.write(tag);
        stream.write(">");
        if (indent != null) stream.write(newLine);
    }
}
