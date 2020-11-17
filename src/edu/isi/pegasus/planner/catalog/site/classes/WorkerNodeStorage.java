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
 * This data class describes the storage area on worker nodes. The difference from the headnode
 * storage is that it additionally has a worker shared directory that designates the shared
 * directory amongst the worker nodes.
 *
 * @version $Revision$
 * @author Karan Vahi
 */
public class WorkerNodeStorage extends StorageType {

    /** The directory shared only amongst the worker nodes. */
    protected WorkerSharedDirectory mWorkerShared;

    /** The default constructor */
    public WorkerNodeStorage() {
        super();
        mWorkerShared = null;
    }

    /**
     * The overloaded constructor
     *
     * @param type StorageType
     */
    public WorkerNodeStorage(StorageType type) {
        this(type.getLocalDirectory(), type.getSharedDirectory());
        mWorkerShared = null;
    }

    /**
     * The overloaded constructor.
     *
     * @param local the local directory on the node.
     * @param shared the shared directory on the node.
     */
    public WorkerNodeStorage(LocalDirectory local, SharedDirectory shared) {
        super(local, shared);
        mWorkerShared = null;
    }

    /**
     * Sets the directory shared amongst the worker nodes only.
     *
     * @param directory the worker node shared directory.
     */
    public void setWorkerSharedDirectory(WorkerSharedDirectory directory) {
        mWorkerShared = directory;
    }

    /**
     * Returns the directory shared amongst the worker nodes only.
     *
     * @return the worker shared directory.
     */
    public WorkerSharedDirectory getWorkerSharedDirectory() {
        return mWorkerShared;
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
        if (this.getWorkerSharedDirectory() != null) {
            this.getWorkerSharedDirectory().toXML(writer, newIndent);
        }

        writer.write(indent);
        writer.write("</storage>");
        writer.write(newLine);
    }
}
