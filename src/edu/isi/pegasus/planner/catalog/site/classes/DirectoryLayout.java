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

import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import edu.isi.pegasus.planner.common.PegRandom;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * An abstract base class that creates a directory type. It associates multiple file servers and an
 * internal mount point.
 *
 * @author Karan Vahi
 */
public abstract class DirectoryLayout extends AbstractSiteData {

    /**
     * The list of file servers that can be used to write to access this directory indexed by
     * operation type.
     */
    // protected List<FileServer> mFileServers;
    protected Map<FileServer.OPERATION, List<FileServer>> mFileServers;

    /** The internal mount point for the directory. */
    protected InternalMountPoint mInternalMount;

    /** The default constructor. */
    public DirectoryLayout() {
        initialize();
    }

    /** The copy constructor */
    public DirectoryLayout(DirectoryLayout directory) {
        this.initialize(directory.mFileServers, directory.getInternalMountPoint());
    }

    /**
     * The overloaded constructor
     *
     * @param fs map of file servers indexed by FileServer operation
     * @param imt the internal mount point.
     */
    public DirectoryLayout(Map<FileServer.OPERATION, List<FileServer>> fs, InternalMountPoint imt) {
        initialize(fs, imt);
    }

    /** Initializes the object */
    private void initialize() {
        this.mFileServers = new HashMap<FileServer.OPERATION, List<FileServer>>();
        this.resetFileServers();
        this.mInternalMount = new InternalMountPoint();
    }

    /**
     * Initializes the object
     *
     * @param mFS list of file servers indexed by operation type
     * @param imt the internal mount point.
     */
    private void initialize(
            Map<FileServer.OPERATION, List<FileServer>> mFS, InternalMountPoint imt) {
        this.initialize();
        for (Map.Entry<FileServer.OPERATION, List<FileServer>> entry : mFS.entrySet()) {
            List<FileServer> servers = entry.getValue();
            for (FileServer server : servers) {
                this.addFileServer(server);
            }
        }
        this.mInternalMount = imt;
    }

    /**
     * Adds a FileServer that can access this directory.
     *
     * @param server the file server.
     */
    public void addFileServer(FileServer server) {
        List<FileServer> l = mFileServers.get(server.getSupportedOperation());
        l.add(server);
    }

    /**
     * Sets the list of FileServers that can access this directory.
     *
     * @param servers the list of servers
     */
    public void setFileServers(List<FileServer> servers) {
        this.resetFileServers();
        for (FileServer server : servers) {
            this.addFileServer(server);
        }
    }

    /**
     * Selects a random file server and returns it matching an operation type. If not found matching
     * the operation type it defaults back to the all operation server.
     *
     * @param operation the operation for which the file server is required
     * @return FileServer else null
     */
    public FileServer selectFileServer(FileServer.OPERATION operation) {
        List<FileServer> servers = getFileServers(operation);

        if (servers == null || servers.isEmpty()) {
            servers = getFileServers(FileServer.OPERATION.all);
        }

        return (servers == null || servers.isEmpty())
                ? null
                : servers.get(PegRandom.getInteger(servers.size() - 1));
        /*
        return ( this.mFileServers == null || this.mFileServers.size() == 0 )?
                 null :
                 this.mFileServers.get(  PegRandom.getInteger( this.mFileServers.size() - 1) );
         */
    }

    /**
     * Selects all file servers and returns it matching an operation type.
     *
     * @param operation the operation for which the file server is required
     * @return List of FileServer else null
     */
    public List<FileServer> getFileServers(FileServer.OPERATION operation) {
        List<FileServer> servers = this.mFileServers.get(operation);

        return servers;
    }

    /**
     * A convenience method that retrieves whether the directory has a file server for get
     * operations ( file server with type get and all )
     *
     * @return boolean
     */
    public boolean hasFileServerForGETOperations() {
        return this.hasFileServerForOperations(OPERATION.get);
    }

    /**
     * A convenience method that retrieves whether the directory has a file server for put
     * operations ( file server with type put and all )
     *
     * @return boolean
     */
    public boolean hasFileServerForPUTOperations() {
        return this.hasFileServerForOperations(OPERATION.put);
    }

    /**
     * A convenience method that retrieves whether the directory has a file server for a particular
     * operation. Servers with operation type as ALL are also considered.
     *
     * @param operation the operation for which we need the file servers
     * @return boolean
     */
    public boolean hasFileServerForOperations(FileServer.OPERATION operation) {
        return !(this.mFileServers.get(FileServer.OPERATION.all).isEmpty()
                && this.mFileServers.get(operation).isEmpty());
    }

    /**
     * Returns at iterator to the file servers.
     *
     * @return Iterator<FileServer>
     */
    public Iterator<FileServer> getFileServersIterator(FileServer.OPERATION operation) {
        return mFileServers.get(operation).iterator();
    }

    /**
     * Sets the internal mount point for the directory.
     *
     * @param mountPoint the internal mount point.
     */
    public void setInternalMountPoint(InternalMountPoint mountPoint) {
        mInternalMount = mountPoint;
    }

    /**
     * Returns the internal mount point for the directory.
     *
     * @return the internal mount point.
     */
    public InternalMountPoint getInternalMountPoint() {
        return this.mInternalMount;
    }

    /**
     * * A convenience method that returns true if all the attributes values are uninitialized or
     * empty strings. Useful for serializing the object as XML.
     *
     * @return boolean
     */
    public boolean isEmpty() {
        boolean result = true;

        // we need to check if each file servers for each supported
        // operation are empty or not.
        for (FileServer.OPERATION operation : FileServer.OPERATION.values()) {
            result = result && this.mFileServers.get(operation).isEmpty();
            if (!result) {
                // break out of the computation.
                break;
            }
        }

        return result && this.getInternalMountPoint().isEmpty();
    }

    /** Resets the internal collection of file servers */
    public void resetFileServers() {
        for (OPERATION op : FileServer.OPERATION.values()) {
            this.mFileServers.put(op, new LinkedList<FileServer>());
        }
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        DirectoryLayout obj;
        try {
            obj = (DirectoryLayout) super.clone();
        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        obj.initialize();
        obj.setInternalMountPoint((InternalMountPoint) mInternalMount.clone());

        for (OPERATION op : FileServer.OPERATION.values()) {
            List<FileServer> servers = this.mFileServers.get(op);
            for (FileServer server : servers) {
                obj.addFileServer((FileServer) server.clone());
            }
        }

        return obj;
    }
}
