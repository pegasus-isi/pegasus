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
package edu.isi.pegasus.planner.catalog.site.classes;

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * An abstract class that describes a file server that can be used to stage data to and from a site.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class FileServerType extends AbstractSiteData {

    /** The operations supported by the file server */
    public static enum OPERATION {
        all,
        get,
        put;

        private static List<OPERATION> mGetOperations;

        private static List<OPERATION> mPutOperations;

        /**
         * Returns a collection of operations corresponding to a get or put operation
         *
         * @param operation the operation for which all the operations are reqd
         * @return Collection consisting of valid operations for operation passed
         */
        public static Collection<OPERATION> operationsFor(OPERATION operation) {
            return (operation.equals(OPERATION.get)) ? operationsForGET() : operationsForPUT();
        }

        /**
         * Returns a collection of get operations.
         *
         * @return Collection consisting of get and all
         */
        public static Collection<OPERATION> operationsForGET() {
            if (mGetOperations == null) {
                mGetOperations = new LinkedList<OPERATION>();
                mGetOperations.add(get);
                mGetOperations.add(all);
            }
            return mGetOperations;
        }

        /**
         * Returns a collection of get operations.
         *
         * @return Collection consisting of put and all
         */
        public static Collection<OPERATION> operationsForPUT() {
            if (mPutOperations == null) {
                mPutOperations = new LinkedList<OPERATION>();
                mPutOperations.add(put);
                mPutOperations.add(all);
            }
            return mPutOperations;
        }
    }

    /** The protocol used by the file server. */
    protected String mProtocol;

    /** The URL prefix for the server. */
    protected String mURLPrefix;

    /** The mount point for the server. */
    protected String mMountPoint;

    /** The profiles associated with the FileSystem. */
    protected Profiles mProfiles;

    /** The operations supported by the file server */
    protected OPERATION mOperation;

    /** The default constructor. */
    public FileServerType() {
        mProtocol = "";
        mURLPrefix = "";
        mMountPoint = "";
        mProfiles = new Profiles();
        mOperation = OPERATION.all;
    }

    /**
     * Overloaded constructor.
     *
     * @param protocol protocol employed by the File Server.
     * @param urlPrefix the url prefix
     * @param mountPoint the mount point for the server.
     */
    public FileServerType(String protocol, String urlPrefix, String mountPoint) {
        mProtocol = protocol;
        mURLPrefix = urlPrefix;
        mMountPoint = mountPoint;
        mProfiles = new Profiles();
        mOperation = OPERATION.all;
    }

    /**
     * Set the protocol implemented by the file server.
     *
     * @param protocol the protocol
     */
    public void setProtocol(String protocol) {
        mProtocol = protocol;
    }

    /**
     * Returns protocol implemented by the file server.
     *
     * @return protocol
     */
    public String getProtocol() {
        return mProtocol;
    }

    /**
     * Sets the url prefix .
     *
     * @param prefix the url prefix
     */
    public void setURLPrefix(String prefix) {
        mURLPrefix = prefix;
    }

    /**
     * Returns the url prefix .
     *
     * @return the url prefix
     */
    public String getURLPrefix() {
        return mURLPrefix;
    }

    /**
     * Returns the mount point.
     *
     * @param point the mount point.
     */
    public void setMountPoint(String point) {
        mMountPoint = point;
    }

    /**
     * Returns the mount point
     *
     * @return the mount point.
     */
    public String getMountPoint() {
        return mMountPoint;
    }

    /**
     * The operation supported by the file server
     *
     * @param operation the supported operation
     */
    public void setSupportedOperation(OPERATION operation) {
        mOperation = operation;
    }

    /**
     * Returns the operation supported by the file server
     *
     * @return the supported operation
     */
    public OPERATION getSupportedOperation() {
        return this.mOperation;
    }

    /**
     * Adds a profile.
     *
     * @param p the profile to be added
     */
    public void addProfile(Profile p) {
        // retrieve the appropriate namespace and then add
        mProfiles.addProfile(p);
    }

    /**
     * Sets the profiles associated with the file server.
     *
     * @param profiles the profiles.
     */
    public void setProfiles(Profiles profiles) {
        mProfiles = profiles;
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        FileServerType obj;
        try {
            obj = (FileServerType) super.clone();
            obj.setMountPoint(this.getMountPoint());
            obj.setProtocol(this.getProtocol());
            obj.setURLPrefix(this.getURLPrefix());
            obj.setSupportedOperation(this.getSupportedOperation());
            obj.setProfiles((Profiles) this.mProfiles.clone());

        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return obj;
    }
}
