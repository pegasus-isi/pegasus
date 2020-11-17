/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package edu.isi.pegasus.planner.catalog.site.classes;

import java.io.IOException;
import java.io.Writer;

/**
 * This data class describes a connection property for replica catalog.
 *
 * @version $Revision$
 * @author Karan Vahi
 */
public class Connection extends AbstractSiteData {

    /** The connection key. */
    private String mKey;

    /** The value of the connection key. */
    private String mValue;

    /** The default constructor. */
    public Connection() {
        this("", "");
    }

    /**
     * The overloaded constructor.
     *
     * @param key the key
     * @param value the key value
     */
    public Connection(String key, String value) {
        initialize(key, value);
        mKey = key;
        mValue = value;
    }

    /**
     * Initializes the object.
     *
     * @param key the key
     * @param value the key value
     */
    public void initialize(String key, String value) {
        mKey = key;
        mValue = value;
    }

    /**
     * Returns the connection key.
     *
     * @return key
     */
    public String getKey() {
        return this.mKey;
    }

    /**
     * Returns the key value.
     *
     * @return value.
     */
    public String getValue() {
        return this.mValue;
    }

    /**
     * Returns the connection key.
     *
     * @param key the key
     */
    public void setKey(String key) {
        this.mKey = key;
    }

    /**
     * Returns the key value.
     *
     * @param value the value.
     */
    public void setValue(String value) {
        this.mValue = value;
    }

    /**
     * Writes out the xml description of the object.
     *
     * @param writer is a Writer opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent the indent to be used.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer writer, String indent) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");

        // write out the  xml element
        writer.write(indent);
        writer.write("<connection ");

        writeAttribute(writer, "key", getKey());

        writer.write(">");
        writer.write(getValue());
        writer.write("</connection>");
        writer.write(newLine);
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        Connection obj;
        try {
            obj = (Connection) super.clone();
            obj.initialize(this.getKey(), this.getValue());
        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return obj;
    }

    @Override
    public void accept(SiteDataVisitor visitor) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
