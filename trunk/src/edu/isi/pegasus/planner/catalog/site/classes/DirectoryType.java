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

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import edu.isi.pegasus.planner.common.PegRandom;
        
/**
 * An abstract base class that creates a directory type. It associates multiple
 * file servers and an internal mount point.
 * 
 * @author Karan Vahi
 */
public abstract class DirectoryType extends AbstractSiteData{
    
    /**
     * The list of file servers that can be used to write to access this directory.
     */
    protected List<FileServer> mFileServers;
    
    /**
     * The internal mount point for the directory.
     */
    protected InternalMountPoint mInternalMount;    
    
    /**
     * The default constructor.
     */
    public DirectoryType(){
        initialize( );
    }
    
    /**
     * The overloaded constructor
     * 
     * @param  fs  list of file servers
     * @param  imt the internal mount point.
     */
    public DirectoryType( List<FileServer> fs, InternalMountPoint imt ){
        initialize( fs, imt );
    }
    
    /**
     *  Initializes the object
     * 
     */
    public void initialize( ){
        this.mFileServers = new LinkedList <FileServer> ();
        this.mInternalMount =  new InternalMountPoint();
    }
    
    /**
     *  Initializes the object
     * 
     * @param  fs  list of file servers
     * @param  imt the internal mount point.
     */
    public void initialize( List<FileServer> fs, InternalMountPoint imt ){
        this.mFileServers = fs;
        this.mInternalMount = imt ;
    }
    
    /**
     * Adds a FileServer that can access this directory.
     * 
     * @param server   the file server.
     */
    public void addFileServer( FileServer server ){
        mFileServers.add( server );
    }

    /**
     * Sets the list of FileServers that can access this directory.
     * 
     * @param servers   the list of servers
     */
    public void setFileServers( List<FileServer> servers ){
        mFileServers = servers;
    }
    
    /**
     * Selects a random file server and returns it.
     * 
     * @return FileServer
     */
    public FileServer selectFileServer(){
        return ( this.mFileServers == null || this.mFileServers.size() == 0 )?
                 null :
                 this.mFileServers.get(  PegRandom.getInteger( this.mFileServers.size() - 1) );
    }
    
    /**
     * Returns at iterator to the file servers.
     * 
     * @return Iterator<FileServer>
     */
    public Iterator<FileServer> getFileServersIterator(){
        return mFileServers.iterator();
    }
    
    /**
     * Sets the internal mount point for the directory.
     * 
     * @param mountPoint  the internal mount point.
     */
    public void setInternalMountPoint( InternalMountPoint mountPoint ){
        mInternalMount = mountPoint;
    }
    
    /**
     * Returns the internal mount point for the directory.
     * 
     * @return the internal mount point.
     */
    public InternalMountPoint getInternalMountPoint(){
        return this.mInternalMount;
    }
    
    /***
     * A convenience method that returns true if all the attributes values are 
     * uninitialized or empty strings. Useful for serializing the object as
     * XML.
     * 
     * @return boolean
     */
    public boolean isEmpty(){
        return this.mFileServers.isEmpty() && this.getInternalMountPoint().isEmpty();
    }
    
    
    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone(){
        DirectoryType obj;
        try{
            obj = ( DirectoryType ) super.clone();
        }
        catch( CloneNotSupportedException e ){
            //somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException("Clone not implemented in the base class of " + this.getClass().getName(),
                                       e );
        }
        obj.initialize( );        
        obj.setInternalMountPoint( (InternalMountPoint)mInternalMount.clone() );
        
        for( FileServer server : mFileServers ){
            obj.addFileServer( (FileServer)server.clone() );
        }
        
        return obj;
    }
    
}
