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
package edu.isi.pegasus.planner.classes;

/**
 * The object that describes the authenticate request. It specifies the mode of authentication, the
 * contact string of the resource.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class AuthenticateRequest extends Data {

    /** The type identifying that the resource to authenticate against is a job manager. */
    public static final char JOBMANAGER_RESOURCE = 'j';

    /** The type identifying that the resource to authenticate against is a grid ftp server. */
    public static final char GRIDFTP_RESOURCE = 'g';

    /** Specifies what type of resource you are authenticating to. */
    private char mType;

    /** The pool id at of the pool which the resource is . */
    private String mPool;

    /** The contact string to the resource. */
    private String mResourceContact;

    /** Default Constructor. */
    private AuthenticateRequest() {}

    /** Overloaded Constructor. */
    public AuthenticateRequest(char type, String pool, String url) {
        mType = type;
        mPool = pool;
        mResourceContact = url;
    }

    /** Returns the type of the request. */
    public char getResourceType() {
        return mType;
    }

    /** Returns the url of the resource to contact. */
    public String getResourceContact() {
        return mResourceContact;
    }

    /** Returns the pool id of the associated resource in this request. */
    public String getPool() {
        return mPool;
    }

    /** Returns a string version of this. */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("TYPE")
                .append("-->")
                .append(mType)
                .append(" ")
                .append(" Pool")
                .append("-->")
                .append(mPool)
                .append(" URL")
                .append("-->")
                .append(mResourceContact);

        return sb.toString();
    }

    /** Returns a clone of the object. */
    public Object clone() {
        AuthenticateRequest ar = new AuthenticateRequest();
        ar.mType = this.mType;
        ar.mResourceContact = new String(this.mResourceContact);
        ar.mPool = new String(this.mPool);
        return ar;
    }

    /**
     * Checks if the request is invalid or not. It is invalid if the resource contact is null or
     * empty or the type is an invalid type.
     *
     * @return boolean true if the request is invalid.
     */
    public boolean requestInvalid() {
        String c = this.getResourceContact();
        // sanity check first
        if (c == null || c.length() == 0) {
            return true;
        }

        boolean val = true;
        switch (this.getResourceType()) {
            case 'g':
            case 'j':
                val = false;
                break;

            default:
                val = true;
        }

        return val;
    }
}
