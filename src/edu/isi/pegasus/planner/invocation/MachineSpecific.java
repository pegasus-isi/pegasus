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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This class collects the various OS-specific elements that we are capturing machine information
 * for.
 *
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public class MachineSpecific extends Invocation {
    /**
     * This is the tag to group the machine-specific information. Usually, it is one of "darwin",
     * "sunos", "linux" or "basic".
     */
    private String m_tag;

    /** The List of <code>MachineInfo</code> elements associated with the machine. */
    private List<MachineInfo> m_info;

    /** Default constructor. */
    public MachineSpecific(String tag) {
        m_tag = tag;
        m_info = new LinkedList<MachineInfo>();
    }

    /**
     * Accessor
     *
     * @see #setTag(String)
     */
    public String getTag() {
        return this.m_tag;
    }

    /**
     * Accessor.
     *
     * @param tag
     * @see #getTag()
     */
    public void setTag(String tag) {
        this.m_tag = tag;
    }

    /**
     * Returns the name of the xml element corresponding to the object.
     *
     * @return name
     */
    public String getElementName() {
        return this.m_tag;
    }

    /**
     * Add a <code>MachineInfo</code> element.
     *
     * @param info the machine info element
     */
    public void addMachineInfo(MachineInfo info) {
        m_info.add(info);
    }

    /**
     * Returns an iterator for the machine info objects
     *
     * @return Iterator for <code>MachineInfo</code> objects.
     */
    public Iterator<MachineInfo> getMachineInfoIterator() {
        return m_info.iterator();
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
        String newline = System.getProperty("line.separator", "\r\n");
        String tag =
                (namespace != null && namespace.length() > 0) ? namespace + ":" + m_tag : m_tag;

        // open tag
        if (indent != null && indent.length() > 0) stream.write(indent);
        stream.write('<');
        stream.write(tag);

        if (m_info.isEmpty()) {
            stream.write("/>");
        } else {
            stream.write('>');
            if (indent != null) stream.write(newline);

            // dump content -- MachineInfo elements
            String newIndent = (indent == null) ? null : indent + "  ";
            for (Iterator<MachineInfo> it = m_info.iterator(); it.hasNext(); ) {
                MachineInfo mi = (MachineInfo) it.next();
                mi.toXML(stream, newIndent, namespace);
            }

            // close tag
            if (indent != null && indent.length() > 0) stream.write(indent);
            stream.write("</");
            stream.write(tag);
            stream.write('>');
        }

        if (indent != null) stream.write(newline);
    }
}
