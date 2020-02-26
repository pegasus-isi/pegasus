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
 * The base class to be used by the various visitor implementors for displaying the Site Catalog in
 * different XML formats
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class AbstractXMLPrintVisitor implements SiteDataVisitor {

    /** The internal writer */
    protected Writer mWriter;

    /** The new line character to be used */
    protected String mNewLine;

    /** The number of tabs to use for current indent */
    protected int mCurrentIndentIndex;

    /**
     * Initialize the visitor implementation
     *
     * @param writer the writer
     */
    public void initialize(Writer writer) {
        mWriter = writer;
        mNewLine = System.getProperty("line.separator", "\r\n");
        mCurrentIndentIndex = 0;
    }

    /**
     * Writes an attribute to the stream. Wraps the value in quotes as required by XML.
     *
     * @param key the attribute key
     * @param value the attribute value
     */
    public void writeAttribute(String key, String value) throws IOException {
        this.writeAttribute(mWriter, key, value);
    }

    /**
     * Writes an attribute to the stream. Wraps the value in quotes as required by XML.
     *
     * @param writer the stream to write to
     * @param key the attribute key
     * @param value the attribute value
     */
    public void writeAttribute(Writer writer, String key, String value) throws IOException {
        writer.write(" ");
        writer.write(key);
        writer.write("=\"");
        writer.write(value);
        writer.write("\"");
    }

    /**
     * Returns the current indent to be used while writing out
     *
     * @return the current indent
     */
    public String getCurrentIndent() {
        StringBuffer indent = new StringBuffer();
        for (int i = 0; i < this.mCurrentIndentIndex; i++) {
            indent.append("\t");
        }
        return indent.toString();
    }

    /**
     * Returns the indent to be used for the nested element.
     *
     * @return the new indent
     */
    public String getNextIndent() {
        return this.getCurrentIndent() + "\t";
    }

    /** Increments the indent index */
    public void incrementIndentIndex() {
        mCurrentIndentIndex++;
    }

    /** Decrements the indent index */
    public void decrementIndentIndex() {
        mCurrentIndentIndex--;
    }

    /**
     * Generates a closing tag for an element
     *
     * @param element the element tag name
     * @throws IOException
     */
    public void closeElement(String element) throws IOException {
        // decrement the IndentIndex
        decrementIndentIndex();
        String indent = getCurrentIndent();
        mWriter.write(indent);
        mWriter.write("</");
        mWriter.write(element);
        mWriter.write(">");
        mWriter.write(mNewLine);
    }

    public void visit(SiteData data) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void depart(SiteData data) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
