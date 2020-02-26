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
import java.io.StringWriter;
import java.io.Writer;

/**
 * The abstract data class for Site classes.
 *
 * @author Karan Vahi
 */
public abstract class AbstractSiteData extends SiteData {

    /**
     * Accept method for the SiteData classes that accepts a visitor
     *
     * @param visitor the visitor to be used
     * @exception IOException if something fishy happens to the stream.
     */
    public abstract void accept(SiteDataVisitor visitor) throws IOException;

    /**
     * Sets the Print Visitor to use for printing the contents of the class.
     *
     * @param visitor the visitor to be used for printing
     * @exception IOException if something fishy happens to the stream.
     */
    // public abstract void setPrintVisitor( SiteDataVisitor visitor ) throws IOException;

    /**
     * Returns the xml description of the object. This is used for generating the partition graph.
     * That is no longer done.
     *
     * @return String containing the object in XML.
     * @exception IOException if something fishy happens to the stream.
     */
    public String toXML() throws IOException {
        Writer writer = new StringWriter(256);
        toXML(writer, "");
        return writer.toString();
    }

    /**
     * Writes out the xml description of the object.
     *
     * @param writer is a Writer opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent the indent to be used.
     * @exception IOException if something fishy happens to the stream.
     */
    public abstract void toXML(Writer writer, String indent) throws IOException;

    /**
     * Writes out the object as a string. Internally calls out the toXML method.
     *
     * @return string description
     */
    public String toString() {
        StringWriter writer = new StringWriter();
        try {
            this.toXML(writer, "");
        } catch (IOException ioe) {

        }
        return writer.toString();
    }

    /**
     * Writes an attribute to the stream. Wraps the value in quotes as required by XML.
     *
     * @param writer
     * @param key
     * @param value
     * @exception IOException if something fishy happens to the stream.
     */
    public void writeAttribute(Writer writer, String key, String value) throws IOException {
        writer.write(" ");
        writer.write(key);
        writer.write("=\"");
        writer.write(value);
        writer.write("\"");
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() throws CloneNotSupportedException {
        AbstractSiteData d;
        d = (AbstractSiteData) super.clone();

        return d;
    }
}
