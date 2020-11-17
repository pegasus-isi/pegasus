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
import java.util.Iterator;

/**
 * An Abstract Data class to describe the filesystem layout on a site, both shared and local on a
 * site/node
 *
 * @version $Revision$
 * @author Karan Vahi
 */
public class StorageType extends AbstractSiteData {

    /** The local directory on the node. */
    protected LocalDirectory mLocalDirectory;

    /** The shared directory on the node. */
    protected SharedDirectory mSharedDirectory;

    /** The default constructor */
    public StorageType() {
        mLocalDirectory = new LocalDirectory();
        mSharedDirectory = new SharedDirectory();
    }

    /**
     * The overloaded constructor.
     *
     * @param local the local directory on the node.
     * @param shared the shared directory on the node.
     */
    public StorageType(LocalDirectory local, SharedDirectory shared) {
        mLocalDirectory = local;
        mSharedDirectory = shared;
    }

    /**
     * Sets the local directory.
     *
     * @param local the local directory.
     */
    public void setLocalDirectory(LocalDirectory local) {
        mLocalDirectory = local;
    }

    /**
     * Sets the local directory.
     *
     * @param local the local directory.
     */
    public void setLocalDirectory(Directory local) {
        // sanity check on type
        Directory.TYPE type = local.getType();
        if (type == Directory.TYPE.shared_scratch || type == Directory.TYPE.shared_storage) {
            // complain
            complain(type);
        }

        mLocalDirectory = new LocalDirectory();
        mLocalDirectory.setInternalMountPoint(local.getInternalMountPoint());

        // iterate through all the file servers
        mLocalDirectory.resetFileServers();
        for (FileServer.OPERATION op : FileServer.OPERATION.values()) {
            for (Iterator<FileServer> it = local.getFileServersIterator(op); it.hasNext(); ) {
                mLocalDirectory.addFileServer(it.next());
            }
        }
    }

    /**
     * Returns the local directory.
     *
     * @return the local directory.
     */
    public LocalDirectory getLocalDirectory() {
        return mLocalDirectory;
    }

    /**
     * Sets the shared directory.
     *
     * @param shared the shared directory.
     */
    public void setSharedDirectory(SharedDirectory shared) {
        mSharedDirectory = shared;
    }

    /**
     * Sets the shared directory.
     *
     * @param shared the shared directory.
     */
    public void setSharedDirectory(Directory shared) {
        // sanity check on type
        Directory.TYPE type = shared.getType();
        if (type == Directory.TYPE.local_scratch || type == Directory.TYPE.local_storage) {
            // complain
            complain(type);
        }

        mSharedDirectory = new SharedDirectory();
        mSharedDirectory.setInternalMountPoint(shared.getInternalMountPoint());
        mSharedDirectory.resetFileServers();
        for (FileServer.OPERATION op : FileServer.OPERATION.values()) {
            for (Iterator<FileServer> it = shared.getFileServersIterator(op); it.hasNext(); ) {
                mSharedDirectory.addFileServer(it.next());
            }
        }
    }

    /**
     * Returns the shared directory.
     *
     * @return the shared directory.
     */
    public SharedDirectory getSharedDirectory() {
        return mSharedDirectory;
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        StorageType obj;
        try {
            obj = (StorageType) super.clone();
            obj.setLocalDirectory((LocalDirectory) this.getLocalDirectory().clone());
            obj.setSharedDirectory((SharedDirectory) this.getSharedDirectory().clone());

        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return obj;
    }

    /**
     * Throws a Runtime Exception
     *
     * @param Directory.TYPE the directory type
     */
    private void complain(Directory.TYPE type) {
        throw new RuntimeException("Invalid directory type associated with storage type" + type);
    }

    @Override
    public void toXML(Writer writer, String indent) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void accept(SiteDataVisitor visitor) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
