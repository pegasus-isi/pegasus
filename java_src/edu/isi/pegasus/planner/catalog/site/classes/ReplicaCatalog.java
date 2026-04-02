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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * This data class describes the Replica Catalog associated with the site.
 *
 * @version $Revision$
 * @author Karan Vahi
 */
public class ReplicaCatalog extends AbstractSiteData {

    /** The url for the catalog. */
    protected String mURL;

    /** The type of the replica catalog implementation to use. */
    protected String mType;

    /** Collection of connection parameters to use for connecting to that replica catalog. */
    protected Collection<Connection> mConnectionParams;

    /** Set of alias names to be used for lookup. */
    protected Set<String> mAliases;

    /** The default constrcutor. */
    public ReplicaCatalog() {
        this("", "");
    }

    /**
     * The overloaded constructor.
     *
     * @param url the url for the replica catalog.
     * @param type the type of replica catalog.
     */
    public ReplicaCatalog(String url, String type) {
        initialize(url, type);
    }

    /**
     * Initialize the class.
     *
     * @param url the url for the replica catalog.
     * @param type the type of replica catalog.
     */
    public void initialize(String url, String type) {
        mURL = url;
        mType = type;
        mAliases = new HashSet<String>();
        mConnectionParams = new LinkedList<Connection>();
    }

    /**
     * Sets the url for the replica catalog.
     *
     * @param url the url
     */
    public void setURL(String url) {
        mURL = url;
    }

    /**
     * Returns the url for the replica catalog.
     *
     * @return url
     */
    public String getURL() {
        return mURL;
    }

    /**
     * Sets the type of replica catalog.
     *
     * @param type the type of replica catalog.
     */
    public void setType(String type) {
        mType = type;
    }

    /**
     * Returns the type of replica catalog.
     *
     * @return type.
     */
    public String getType() {
        return mType;
    }

    /**
     * Adds an alias site handle.
     *
     * @param name the site handle to alias to.
     */
    public void addAlias(String name) {
        mAliases.add(name);
    }

    /**
     * Adds a connection parameter
     *
     * @param connection the connection parameter.
     */
    public void addConnection(Connection connection) {
        mConnectionParams.add(connection);
    }

    /** Clears the aliases associates with the replica catalog. */
    public void clearAliases() {
        this.mAliases.clear();
    }

    /**
     * Returns an iterator to aliases associated with the site.
     *
     * @return Iterator<String>
     */
    public Iterator<String> getAliasIterator() {
        return this.mAliases.iterator();
    }

    /**
     * Returns an iterator to connection params associated with the replica catalog.
     *
     * @return Iterator<Connection>
     */
    public Iterator<Connection> getConnectionIterator() {
        return this.mConnectionParams.iterator();
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
        String newIndent = indent + "\t";

        // write out the  xml element
        writer.write(indent);
        writer.write("<replica-catalog ");
        writeAttribute(writer, "type", getType());
        writeAttribute(writer, "url", getURL());

        if (this.mAliases.isEmpty() && this.mConnectionParams.isEmpty()) {
            writer.write("/>");
        } else {
            writer.write(">");
            writer.write(newLine);

            // list all the aliases first
            for (Iterator<String> it = this.getAliasIterator(); it.hasNext(); ) {
                writeAlias(writer, newIndent, it.next());
            }

            // list all the connection params
            for (Iterator<Connection> it = this.getConnectionIterator(); it.hasNext(); ) {
                it.next().toXML(writer, newIndent);
            }

            writer.write(indent);
            writer.write("</replica-catalog>");
        }
        writer.write(newLine);
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        ReplicaCatalog obj;
        try {
            obj = (ReplicaCatalog) super.clone();
            obj.initialize(this.getType(), this.getURL());

            for (Iterator<String> it = this.getAliasIterator(); it.hasNext(); ) {
                obj.addAlias(it.next());
            }

            for (Iterator<Connection> it = this.getConnectionIterator(); it.hasNext(); ) {
                obj.addConnection((Connection) it.next().clone());
            }

        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return obj;
    }

    /**
     * Renders alias as xml
     *
     * @param writer is a Writer opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent the indent to be used.
     * @param value the value to use.
     * @exception IOException if something fishy happens to the stream.
     */
    protected void writeAlias(Writer writer, String indent, String value) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");

        // write out the  xml element
        writer.write(indent);
        writer.write("<alias ");
        writeAttribute(writer, "name", value);
        writer.write("/>");
        writer.write(newLine);
    }

    /**
     * Accept the visitor
     *
     * @param visitor
     */
    public void accept(SiteDataVisitor visitor) throws IOException {
        visitor.visit(this);

        // list all the connection params
        for (Iterator<Connection> it = this.getConnectionIterator(); it.hasNext(); ) {
            Connection c = it.next();
            c.accept(visitor);
        }

        visitor.depart(this);
    }
}
