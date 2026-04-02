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
 * This data class describes the storage area on a node.
 *
 * @version $Revision$
 * @author Karan Vahi
 */
public class HeadNodeStorage extends StorageType {

    /** The default constructor */
    public HeadNodeStorage() {
        super();
    }

    /**
     * The overloaded constructor
     *
     * @param type StorageType
     */
    public HeadNodeStorage(StorageType type) {
        this(type.getLocalDirectory(), type.getSharedDirectory());
    }

    /**
     * The overloaded constructor.
     *
     * @param local the local directory on the node.
     * @param shared the shared directory on the node.
     */
    public HeadNodeStorage(LocalDirectory local, SharedDirectory shared) {
        super(local, shared);
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
        writer.write("<storage>");
        writer.write(newLine);

        this.getLocalDirectory().toXML(writer, newIndent);
        this.getSharedDirectory().toXML(writer, newIndent);

        writer.write(indent);
        writer.write("</storage>");
        writer.write(newLine);
    }
}
