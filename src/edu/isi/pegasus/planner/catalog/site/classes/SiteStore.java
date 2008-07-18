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
import java.util.Set;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.classes.SysInfo;
        
import org.griphyn.common.util.Currently;


import java.io.IOException;
import java.io.Writer;

import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 * The site store contains the collection of sites backed by a HashMap.
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class SiteStore extends AbstractSiteData{
    
    /**
     * The "official" namespace URI of the site catalog schema.
     */
    public static final String SCHEMA_NAMESPACE = "http://pegasus.isi.edu/schema/sitecatalog";

    /**
     * The "not-so-official" location URL of the DAX schema definition.
     */
    public static final String SCHEMA_LOCATION = "http://pegasus.isi.edu/schema/sc-3.0.xsd";

    /**
     * The version to report.
     */
    public static final String SCHEMA_VERSION = "3.0";
    
    
    
    /**
     * The internal map that maps a site catalog entry to the site handle.
     */
    private Map<String, SiteCatalogEntry> mStore;
    
    /**
     * The default constructor.
     */
    public SiteStore(){
        initialize();
    }

    
    
    /**
     * The intialize method.
     */
    public void initialize() {        
        mStore = new HashMap<String, SiteCatalogEntry>( );
    }
    
    
    /**
     * Adds a site catalog entry to the store.
     * 
     * @param site  the site catalog entry.
     * 
     * @return previous value associated with specified key, or null 
     *         if there was no mapping for key
     */
    public SiteCatalogEntry addEntry( SiteCatalogEntry entry ){
        return this.mStore.put( entry.getSiteHandle() , entry );
    }
    
    /**
     * Returns an iterator to SiteCatalogEntry objects in the store.
     * 
     * @return Iterator<SiteCatalogEntry>
     */
    public Iterator<SiteCatalogEntry> entryIterator(){
        return this.mStore.values().iterator();
    }

    /**
     * Returns the list of sites, in the store.
     * 
     * @return
     */
    public Set<String> list() {
        return mStore.keySet();
    }
    
    /**
     * Returns SiteCatalogEntry matching a site handle.
     * 
     * @return SiteCatalogEntry if exists else null.
     */
    public SiteCatalogEntry lookup ( String handle ){
        return this.mStore.get( handle );
    }
    
    /**
     * Returns boolean indicating whether the store has a SiteCatalogEntry 
     * matching a handle.
     * 
     * @param handle  the site handle / identifier.
     * 
     * @return boolean
     */
    public boolean contains( String handle ){
        return this.mStore.containsKey( handle );
    }
    
    /**
     * 
     * @param siteids
     * @return
     */
    public Map getSysInfos(List siteids) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    /**
     * Returns the <code>SysInfo</code> for the site
     * 
     * @param handle the site handle / identifier.
     * @return the SysInfo else null
     */
    public SysInfo getSysInfo( String handle ){
        //sanity check
        if( !this.contains( handle ) ) {
            return null;
        }
        else{
            return this.lookup( handle ).getSysInfo();
        }
        
    }
    
    /**
     * Returns the value of VDS_HOME for a site.
     *
     * @param handle   the site handle / identifier.
     * 
     * @return value if set else null.
     */
    public String getVDSHome( String handle ){
        //sanity check
        if( !this.contains( handle ) ) {
            return null;
        }
        else{
            return this.lookup( handle ).getVDSHome();
        }
    }


    /**
     * Returns the value of PEGASUS_HOME for a site.
     *
     * @param handle   the site handle / identifier.
     * 
     * @return value if set else null.
     */
    public String getPegasusHome( String handle ){        
        if( !this.contains( handle ) ) {
            return null;
        }
        else{
            return this.lookup( handle ).getPegasusHome();
        }
    }

    
    /**
     * Returns an environment variable associated with the site.
     *
     * @param handle   the site handle / identifier.
     *
     * @return value of the environment variable if found, else null
     */
    public String getEnvironmentVariable( String handle, String variable ){
        //sanity check
        if( !this.contains( handle ) ) {
            return null;
        }
        else{
            return this.lookup( handle ).getEnvironmentVariable( variable );
        }
    }
    
    /**
     * This is a soft state remove, that removes a GridGateway from a particular
     * site. The cause of this removal could be the inability to
     * authenticate against it at runtime. The successful removal lead Pegasus
     * not to schedule job on that particular grid gateway.
     *
     * @param handle   the site handle with which it is associated.
     * @param contact  the contact string for the grid gateway.
     *
     * @return true if was able to remove the jobmanager from the cache
     *         false if unable to remove, or the matching entry is not found
     *         or if the implementing class does not maintain a soft state.
     */
    public boolean removeGridGateway( String handle,  String contact ) {
        //sanity check
        if( !this.contains( handle ) ) {
            return false;
        }
        else{
            return this.lookup( handle ).removeGridGateway( contact );
        }
    }

    /**
     * This is a soft state remove, that removes a file server from a particular
     * pool entry. The cause of this removal could be the inability to
     * authenticate against it at runtime. The successful removal lead Pegasus
     * not to schedule any transfers on that particular gridftp server.
     *
     * @param handle the site handle with which it is associated.
     * @param url    the contact string for the file server.
     *
     * @return true if was able to remove the gridftp from the cache
     *         false if unable to remove, or the matching entry is not found
     *         or if the implementing class does not maintain a soft state.
     *         or the information about site is not in the site catalog.
     */
    public boolean removeFileServer( String handle, String url ){
        throw new UnsupportedOperationException( "Method remove( String , String ) not yet implmeneted" );
    }
    
    
    /**
     * This determines the working directory on remote execution pool on the
     * basis of whether an absolute path is specified in the pegasus.dir.exec directory
     * or a relative path.
     *
     * @param handle  the site handle of the site where a job has to be executed.
     *
     * @return the path to the pool work dir.
     * @throws RuntimeException in case of site not found in the site catalog.
     */
    public String getWorkDirectory( String handle ) {
        return this.getWorkDirectory( handle, null, -1 );
    }

    /**
     * This determines the working directory on remote execution pool for a
     * particular job. The job should have it's execution pool set.
     *
     * @param job <code>SubInfo</code> object for the job.
     *
     * @return the path to the pool work dir.
     * @throws RuntimeException in case of site not found in the site catalog.
     */
    public String getWorkDirectory( SubInfo job ) {
        return this.getWorkDirectory( job.executionPool,
            job.vdsNS.getStringValue(
            VDS.REMOTE_INITIALDIR_KEY ),
            job.jobClass );
    }

    /**
     * This determines the working directory on remote execution pool on the
     * basis of whether an absolute path is specified in the pegasus.dir.exec
     * directory or a relative path.
     *
     * @param handle  the site handle of the site where a job has to be executed.
     * @param path    the relative path that needs to be appended to the
     *                workdir from the execution pool.
     *
     * @return the path to the pool work dir.
     * @throws RuntimeException in case of site not found in the site catalog.
     */
    public String getWorkDirectory( String handle, String path ) {
        return this.getWorkDirectory( handle, path, -1 );
    }

    /**
     * This determines the working directory on remote execution pool on the
     * basis of whether an absolute path is specified in the pegasus.dir.exec directory
     * or a relative path. If the job class happens to be a create directory job
     * it does not append the name of the random directory since the job is
     * trying to create that random directory.
     *
     * @param handle  the site handle of the site where a job has to be executed.
     * @param path       the relative path that needs to be appended to the
     *                   workdir from the execution pool.
     * @param jobClass   the class of the job.
     *
     * @return the path to the pool work dir.
     * @throws RuntimeException in case of site not found in the site catalog.
     */
    public String getWorkDirectory( String handle, String path, int jobClass ) {
        SiteCatalogEntry execPool = this.lookup( handle );
        if(execPool == null){
            throw new RuntimeException("Entry for " + handle +
                                       " does not exist in the Site Catalog");
        }
        throw new UnsupportedOperationException( "getWorkDirectory not implemented as yet" );
        /*
        String execPoolDir = mWorkDir;

        if(jobClass == SubInfo.CREATE_DIR_JOB ){
            //the create dir jobs always run in the
            //workdir specified in the site catalog
            return execPool.getExecMountPoint();
        }

        if ( mWorkDir.length() == 0 || mWorkDir.charAt( 0 ) != '/' ) {
            //means you have to append the
            //value specfied by pegasus.dir.exec
            File f = new File( execPool.getExecMountPoint(), mWorkDir );
            execPoolDir = f.getAbsolutePath();
        }


        //get the random directory name
        String randDir = mUserOpts.getRandomDirName();

        if ( randDir != null) {
            //append the random dir name to the
            //work dir constructed till now
            File f = new File( execPoolDir, randDir );
            execPoolDir = f.getAbsolutePath();
        }

        //path takes precedence over random dir
        if ( path != null ) {
            //well i can do nesting conditional return but wont
            return ( path.length() == 0 || path.charAt( 0 ) != '/' ) ?
                //append the path
                new File( execPoolDir, path ).getAbsolutePath()
                : //else absolute path specified
                path;
        }

        return execPoolDir;
         */
    }

    
    /**
     * Writes out the contents of the replica store as XML document
     * 
     * @param writer
     * @param indent
     * @throws java.io.IOException
     */
    public void toXML( Writer writer, String indent ) throws IOException {
        String newLine = System.getProperty( "line.separator", "\r\n" );
        indent = (indent != null && indent.length() > 0 ) ?
                 indent:
                 "";
        String newIndent = indent + "\t";
        
        //write out the xml header first.
        this.writeXMLHeader( writer, indent );
        
        //iterate through all the entries and spit them out.
        for( Iterator<SiteCatalogEntry> it = this.entryIterator(); it.hasNext(); ){
            it.next().toXML( writer, newIndent );
        }
        
        //write out the footer
        writer.write( indent );
        writer.write( "</sitecatalog>" );
        writer.write( newLine );
    }
    
    /**
     * Writes the header of the XML output. The output contains the special
     * strings to start an XML document, some comments, and the root element.
     * The latter points to the XML schema via XML Instances.
     *
     * @param stream is a stream opened and ready for writing. This can also
     *               be a string stream for efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty
     *               printing. The initial amount of spaces should be an empty
     *               string. The parameter is used internally for the recursive 
     *               traversal.
     * 
     * @exception IOException if something fishy happens to the stream.
     */
    public void writeXMLHeader( Writer stream, String indent )
                                                    throws IOException  {
        
        String newline = System.getProperty( "line.separator", "\r\n" );
        indent = (indent != null && indent.length() > 0 ) ?
                 indent:
                 "";
        
        stream.write( indent );
        stream.write( "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" );
        stream.write( newline );

        stream.write( indent );        
        stream.write( "<!-- generated: " );
        stream.write( Currently.iso8601(false) );
        stream.write( " -->" );
        stream.write( newline );

        // who generated this document
        stream.write( indent );
        stream.write( "<!-- generated by: " );
        stream.write( System.getProperties().getProperty("user.name", "unknown") );
        stream.write( " [" );
        stream.write( System.getProperties().getProperty("user.region","??") );
        stream.write( "] -->" );
        stream.write( newline );

        // root element with elementary attributes
        stream.write( indent );
        stream.write( '<' );
        
        stream.write( "sitecatalog xmlns" );       
        stream.write( "=\"");
        stream.write( SCHEMA_NAMESPACE );
        stream.write( "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"" );
        stream.write( SCHEMA_NAMESPACE );
        stream.write( ' ' );
        stream.write( SCHEMA_LOCATION );
        stream.write( '"' );
        writeAttribute( stream, "version", SCHEMA_VERSION );
        stream.write( '>' );
        stream.write( newline );
   }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone(){
        SiteStore obj;
        try{
            obj = ( SiteStore ) super.clone();
            obj.initialize();
           
             //iterate through all the entries and spit them out.
            for( Iterator<SiteCatalogEntry> it = this.entryIterator(); it.hasNext(); ){
                obj.addEntry( (SiteCatalogEntry)it.next().clone( ));
            }
        }
        catch( CloneNotSupportedException e ){
            //somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException("Clone not implemented in the base class of " + this.getClass().getName(),
                                       e );
        }
        return obj;
    }

     
    
}
