/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.isi.pegasus.planner.catalog.site.impl.old.classes;


import java.util.ArrayList;
import java.util.List;

/**
 * This is a data class that is used to store information about a grid ftp server.
 * <p>
 * The various attributes that can be associated with the the server are
 * displayed in the following table.
 *
 * <p>
 * <table border="1">
 * <tr align="left"><th>Attribute Name</th><th>Attribute Description</th></tr>
 * <tr align="left"><th>url</th>
 *  <td>the url string pointing to gridftp server, consisting of the host and
 *      the port.</td>
 * </tr>
 * <tr align="left"><th>globus version</th>
 *  <td>the version of the Globus Toolkit that was used to install the server.</td>
 * </tr>
 * <tr align="left"><th>storage mount point</th>
 *  <td>the storage mount point for the server.</td>
 * </tr>
 * <tr align="left"><th>total size</th>
 *  <td>the total storage space at the grid ftp server.</td>
 * </tr>
 * <tr align="left"><th>free size</th>
 *  <td>the free space at the grid ftp server.</td>
 * </tr>
 * </table>
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @author Karan Vahi vahi@isi.edu
 *
 * @version $Revision$
 */
public class GridFTPServer{

    /**
     * Array storing the names of the attributes that are stored with the
     * grid ftp server.
     */
    public static final String GRIDFTPINFO[] = {
        "url", "storage", "globus-version", "total-size", "free-size"};

    /**
     * The constant to be passed to the accessor functions to get or set the url.
     */
    public static final int GRIDFTP_URL = 0;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * storage directory of the grid ftp server.
     */
    public static final int STORAGE_DIR = 1;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * globus version of the grid ftp server.
     */
    public static final int GLOBUS_VERSION = 2;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * total size.
     */
    public static final int TOTAL_SIZE = 3;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * free size.
     */
    public static final int FREE_SIZE = 4;

    /**
     * The url string of the gridftp that contains the host and the port.
     */
    private String mURL;

    /**
     * The storage mount point for the grid ftp server. This is the absolute
     * path on the file system being accessed through the grid ftp server.
     */
    private String mStorageDir;

    /**
     * The version of Globus Toolkit that was used to install the grid ftp server.
     */
    private String mGlobusVersion;

    /**
     * The total storage space at the grid ftp server.
     * In what units??
     */
    private String mTotalSize;

    /**
     * The free space at the grid ftp server.
     * In what units??
     */
    private String mFreeSize;

    /**
     *
     */
    private List mBandWidths;

    /**
     * The default constructor.
     */
    public GridFTPServer() {
        mGlobusVersion = null;
        mFreeSize      = null;
        mStorageDir    = null;
        mTotalSize     = null;
        mURL           = null;
        // sk initialised gridftp_bandwidths HashMap
        mBandWidths = new ArrayList();
    }

    /**
     * Checks if an object is similar to the one referred to by this class.
     * We compare the primary key to determine if it is the same or not.
     *
     * @param o Object
     * @return true if the primary key (universe,jobmanager-type,pool) match.
     *         else false.
     */
    public boolean equals( Object o ) {
        GridFTPServer server = ( GridFTPServer ) o;

        if ( this.mURL.equals( server.mURL ) ) {
            return true;
        }
        return false;
    }

    /**
     * Sets an attribute associated with the grid ftp server.
     *
     * @param key  the attribute key, which is one of the predefined keys.
     * @param value value of the attribute.
     *
     * @throws Exception if illegal key defined.
     */
    public void setInfo( int key, String value ) throws
        Exception {
        switch ( key ) {
            case 0:
                mURL = value == null ? null :
                    new String( value );
                break;

            case 1:
                mStorageDir = value == null ? null :
                    new String( value );
                break;

            case 2:
                mGlobusVersion = value == null ? null :
                    new String( ( new GlobusVersion( value ) ).
                    getGlobusVersion() );
                break;

            case 3:
                mTotalSize = value == null ? null :
                    new String( value );
                break;

            case 4:
                mFreeSize = value == null ? null :
                    new String( value );
                break;

            default:
                throw new Exception( "Wrong key = " +
                     key  +" specified. key must be one of the predefined types" );

        }
    }

    /**
     * It fills information in the mBandWidths ArrayList.
     *
     * @param bandwidth  the object that is stored in the hash, containing the
     *                   information about the gridftp bandwidth between the host
     *                   and the destination.
     *
     * @throws Exception
     */
    public void setGridFTPBandwidthInfo( GridFTPBandwidth bandwidth) throws Exception {
        mBandWidths.add( bandwidth );
    }

    /**
     * Returns a list of <code>GridFTPBandwidth</code> objects that contain the
     * bandwidths by which a site is connected to other sites.
     *
     * @return list of <code>GridFTPBandwidth</code> objects.
     *
     * @throws Exception
     */
    public List getGridFTPBandwidthInfo() throws Exception {
        return mBandWidths;
    }

    /**
     * Returns the attribute value of a particular attribute of the server.
     *
     * @param key the key/attribute name.
     *
     * @return the attribute value
     * @throws RuntimeException if illegal key defined.
     */
    public String getInfo( int key ) {
        switch ( key ) {

            case 0:
                return mURL;

            case 1:
                return mStorageDir;

            case 2:
                return mGlobusVersion;

            case 3:
                return mTotalSize;

            case 4:
                return mFreeSize;

            default:
                throw new RuntimeException( "Wrong key = " +
                     key  +
                    " specified. key must be one of the predefined types" );

        }
    }

    /**
     * Returns the textual description of the  contents of <code>GridFTPServer</code>
     * object in the multiline format.
     *
     * @return the textual description in multiline format.
     */
    public String toMultiLine() {
        String output = "gridftp";
        if ( mURL != null ) {
            output += " \"" + mURL +mStorageDir+"\"";
        }
        if (mGlobusVersion != null) {
            output += " \"" + mGlobusVersion + "\"";
        }

        return output;
    }

    /**
     * Returns the textual description of the  contents of <code>LRC</code>
     * object.
     *
     * @return the textual description.
     */
    public String toString() {
        String output = "gridftp";
        if ( mURL != null ) {
            output += " \"" + mURL +mStorageDir+"\"";
        }
        if (mURL != null) {
            output += " " + GRIDFTPINFO[GRIDFTP_URL] + "=" + mURL;
        }
        if (mStorageDir != null) {
            output += " " + GRIDFTPINFO[STORAGE_DIR] + "=" + mStorageDir;
        }
        if (mGlobusVersion != null) {
            output += " " + GRIDFTPINFO[GLOBUS_VERSION] + "=" +
                mGlobusVersion;
        }
        if (mTotalSize != null) {
            output += " " + GRIDFTPINFO[TOTAL_SIZE] + "=" + mTotalSize;
        }
        if (mFreeSize != null) {
            output += " " + GRIDFTPINFO[FREE_SIZE] + "=" + mFreeSize;
        }
        output += " )";
        // System.out.println(output);
        return output;
    }

    /**
     * Returns the XML description of the  contents of <code>LRC</code>
     * object.
     *
     * @return the xml description.
     */
    public String toXML() {

        String output = "<gridftp ";
        if ( mURL != null ) {
            output += " " + GRIDFTPINFO[ GRIDFTP_URL ] + "=\"" + mURL +
                "\"";
        }
        if ( mStorageDir != null ) {
            output += " " + GRIDFTPINFO[ STORAGE_DIR ] + "=\"" + mStorageDir +
                "\"";
        }
        if ( mGlobusVersion != null ) {
            GlobusVersion gv = new GlobusVersion(
                mGlobusVersion );
            output += " major=\"" +
                gv.getGlobusVersion( GlobusVersion.MAJOR ) + "\"" +
                " minor=\"" + gv.getGlobusVersion( GlobusVersion.MINOR ) +
                "\"" +
                " patch=\"" + gv.getGlobusVersion( GlobusVersion.PATCH ) +
                "\"";
        }
        if ( mTotalSize != null ) {
            output += " " + GRIDFTPINFO[ TOTAL_SIZE ] + "=\"" + mTotalSize +
                "\"";
        }
        if ( mFreeSize != null ) {
            output += " " + GRIDFTPINFO[ FREE_SIZE ] + "=\"" + mFreeSize +
                "\"";
        }

        output += "> \n";

        /**
         * Saurabh added code which picks up elements from gridftp_bandwidth
         * and prints them out as XML.
         */
        for ( int len = 0; len < mBandWidths.size(); len++ ) {
            GridFTPBandwidth gf = ( GridFTPBandwidth )
                mBandWidths.get( len );

            output += gf.toXML();

        }
        output += "    </gridftp>";

        return output;

    }
}
