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
 * A data class to signify the Internal Mount Point for a filesystem.
 *
 * @author Karan Vahi
 */
public class InternalMountPoint extends FileSystemType {

    /** The default constructor. */
    public InternalMountPoint() {
        super();
    }

    /**
     * The overloaded constructor.
     *
     * @param mountPoint the mount point of the system.
     */
    public InternalMountPoint(String mountPoint) {
        this(mountPoint, null, null);
    }

    /**
     * The overloaded constructor.
     *
     * @param mountPoint the mount point of the system.
     * @param totalSize the total size of the system.
     * @param freeSize the free size
     */
    public InternalMountPoint(String mountPoint, String totalSize, String freeSize) {
        super(mountPoint, totalSize, freeSize);
    }

    /**
     * * A convenience method that returns true if all the attributes values are uninitialized or
     * empty strings. Useful for serializing the object as XML.
     *
     * @return boolean
     */
    public boolean isEmpty() {
        return (this.getFreeSize() == null || this.getFreeSize().length() == 0)
                && (this.getMountPoint() == null || this.getMountPoint().length() == 0)
                && (this.getTotalSize() == null || this.getTotalSize().length() == 0);
    }

    /**
     * Writes out the xml description of the object.
     *
     * @param writer is a Writer opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent the indent to use.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer writer, String indent) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");

        // sanity check?
        if (this.isEmpty()) {
            return;
        }

        // write out the  xml element
        writer.write(indent);
        writer.write("<internal-mount-point");

        writeAttribute(writer, "mount-point", mMountPoint);
        if (mFreeSize != null) {
            writeAttribute(writer, "free-size", mFreeSize);
        }
        if (mTotalSize != null) {
            writeAttribute(writer, "total-size", mTotalSize);
        }

        writer.write("/>");
        writer.write(newLine);
    }

    @Override
    public void accept(SiteDataVisitor visitor) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
