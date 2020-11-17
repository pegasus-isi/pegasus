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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An abstract class that is used for all the child elements that appear in the machine element.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class MachineInfo extends Invocation {

    /** An internal maps that is indexed by attribute keys. */
    protected Map<String, String> mAttributeMap;

    /** Default constructor. */
    public MachineInfo() {
        mAttributeMap = new HashMap<String, String>();
    }

    /**
     * Returns the name of the xml element corresponding to the object.
     *
     * @return name
     */
    public abstract String getElementName();

    /**
     * Adds an attribute.
     *
     * @param key the attribute key
     * @param value the attribute value
     */
    public void addAttribute(String key, String value) {
        mAttributeMap.put(key, value);
    }

    /**
     * Add multiple attributes to the machine info element.
     *
     * @param keys <code>List</code> of keys
     * @param values Corresponding <code>List</code> of values
     */
    public void addAttributes(List keys, List values) {
        for (int i = 0; i < keys.size(); ++i) {
            String name = (String) keys.get(i);
            String value = (String) values.get(i);

            addAttribute(name, value);
        }
    }

    /**
     * Returns Iterator for attribute keys.
     *
     * @return iterator
     */
    public Iterator<String> getAttributeKeysIterator() {
        return mAttributeMap.keySet().iterator();
    }

    /**
     * Returns attribute value for a key
     *
     * @param key
     * @return value
     */
    public String get(String key) {
        return mAttributeMap.get(key);
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
        String tag = (namespace != null && namespace.length() > 0) ? namespace + ":" : "";
        tag = tag + getElementName();

        //       if (this.m_value != null) {
        // open tag
        if (indent != null && indent.length() > 0) {
            stream.write(indent);
        }
        stream.write('<');
        stream.write(tag);
        stream.write(" ");

        // write out all the attributes
        for (Iterator it = mAttributeMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, String> entry = (Map.Entry) it.next();

            writeAttribute(stream, " " + entry.getKey() + "=\"", quote(entry.getValue(), true));
            // writeAttribute( stream, entry.getKey(), entry.getValue() );
        }

        // dump content if required
        if (this instanceof HasText) {
            stream.write(">");
            HasText ht = (HasText) this;
            stream.write(quote(ht.getValue(), false));

            // close tag
            stream.write("</");
            stream.write(tag);
            stream.write('>');
        } else {
            stream.write("/>");
        }
        if (indent != null) {
            stream.write(System.getProperty("line.separator", "\r\n"));
        }
    }
    // }

}
