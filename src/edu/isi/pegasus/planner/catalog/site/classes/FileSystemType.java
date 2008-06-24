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
package edu.isi.pegasus.planner.catalog.site.classes;


/**
 * An abstract class describing a filesystem type.
 *
 * <p>
 * The various attributes that can be associated with the the server are
 * displayed in the following table.
 *
 * <p>
 * <table border="1">
 * <tr align="left"><th>Attribute Name</th><th>Attribute Description</th></tr>
 * <tr align="left"><th>mount-point</th>
 *  <td>the mount point for the filesystem</td>
 * </tr>
 * </table>
 *

 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class FileSystemType extends AbstractSiteData {

    /**
     * The mount point for the file system.
     */
    protected String mMountPoint;

    /**
     * The total size of file system.
     */
    protected String mTotalSize;

    /**
     * The free space on the file system.
     */
    protected String mFreeSize;

    
    /**
     * The default constructor.
     */
    public FileSystemType() {
        mMountPoint = new String();
        mTotalSize = new String();
        mFreeSize  = new String();
    }
    
    /**
     * The overloaded constructor.
     * 
     * @param mountPoint  the mount point of the system.
     * @param totalSize   the total size of the system.  
     * @param freeSize    the free size  
     */
    public  FileSystemType( String mountPoint, String totalSize, String freeSize ){
        mMountPoint = mountPoint;
        mTotalSize  = totalSize;
        mFreeSize   = freeSize;
    }
    
    
   
}
