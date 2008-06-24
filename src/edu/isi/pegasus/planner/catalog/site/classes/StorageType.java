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

/**
 * An Abstract Data class to describe the filesystem layout on a site, both
 * shared and local on a site/node
 * 
 * @version $Revision$
 * @author Karan Vahi
 */
public abstract class StorageType extends AbstractSiteData{

    /**
     * The local directory on the node.
     */
    protected LocalDirectory mLocalDirectory;
    
    /**
     * The shared directory on the node.
     */
    protected SharedDirectory mSharedDirectory;
    
    
    /**
     * The default constructor
     */
    public StorageType() {
        mLocalDirectory = new LocalDirectory();
        mSharedDirectory = new SharedDirectory();
    }
    
    /**
     * The overloaded constructor.
     * 
     * @param local   the local directory on the node.
     * @param shared  the shared directory on the node.
     */
    public StorageType( LocalDirectory local, SharedDirectory shared ){
        mLocalDirectory  = local;
        mSharedDirectory = shared;
    }
    
    /**
     * Sets the local directory.
     * 
     * @param local  the local directory.
     */
    public void setLocalDirectory( LocalDirectory local ){
        mLocalDirectory = local;
    }
    
    /**
     * Returns the local directory.
     * 
     * @return  the local directory.
     */
    public LocalDirectory getLocalDirectory(  ){
        return mLocalDirectory;
    }
    
    
    /**
     * Sets the shared directory.
     * 
     * @param local  the shared directory.
     */
    public void setSharedDirectory( SharedDirectory shared ){
        mSharedDirectory = shared;
    }
    
    
    /**
     * Returns the shared directory.
     * 
     * @return  the shared directory.
     */
    public SharedDirectory getSharedDirectory(  ){
        return mSharedDirectory;
    }
}
