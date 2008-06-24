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

import org.griphyn.cPlanner.classes.Profile;


import edu.isi.pegasus.planner.catalog.classes.Profiles;




/**
 * An abstract class that describes a file server that can be used to stage data 
 * to and from a site.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class FileServerType extends AbstractSiteData {


    /**
     * The protocol used by the file server.
     */
    protected String mProtocol;

    /**
     * The URL prefix for the server.
     */
    protected String mURLPrefix;

    /**
     * The mount point for the server.
     */
    protected String mMountPoint;
    
    
    /**
     * The profiles associated with the FileSystem.
     */
    protected Profiles mProfiles;

    
    /**
     * The default constructor.
     */
    public FileServerType() {
        mProtocol = new String();
        mURLPrefix = new String();
        mMountPoint = new String();
    }
    
    /**
     * Overloaded constructor.
     * 
     * @param protocol   protocol employed by the File Server.
     * @param urlPrefix  the url prefix 
     * @param mountPoint the mount point for the server.
     */
    public FileServerType( String protocol, String urlPrefix, String mountPoint ) {
        mProtocol = protocol;
        mURLPrefix = urlPrefix;
        mMountPoint = mountPoint;
    }
    
    
    /**
     * Returns protocol implemented by the file server.
     * 
     * @return protocol
     */
    public String getProtocol(){
        return mProtocol;
    }

    
    /**
     * Returns the url prefix .
     * 
     * @return  the url prefix
     */
    public String getURLPrefix(){
        return mURLPrefix;
    }
    
    /**
     * Returns the mount point/
     * 
     * @return  the url prefix
     */
    public String getMountPoint(){
        return mMountPoint;
    }
    
    /**
     * Adds a profile.
     * 
     * @param profile  the profile to be added
     */
    public void addProfile( Profile p ){
        //retrieve the appropriate namespace and then add
       mProfiles.addProfile(  p );
    }
    
    
    
    
}
