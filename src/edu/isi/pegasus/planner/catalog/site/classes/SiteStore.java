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


import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.transfer.mapper.OutputMapperFactory;

/**
 * The site store contains the collection of sites backed by a HashMap.
 * 
 * @author Karan Vahi
 * @author Mats Rynge
 * @version $Revision$
 */
public class SiteStore extends AbstractSiteData{
    
    
    
    /**
     * The internal map that maps a site catalog entry to the site handle.
     */
    private Map<String, SiteCatalogEntry> mStore;
    
    /**
     * The work dir path from the properties.
     */
    private String mWorkDir;
    private PlannerOptions mPlannerOptions;
    
    
    /**
     * The file backend for the site catalog.
     */
    private File mFileSource;

    
    /**
     * A boolean indicating whether to have a deep directory structure for
     * the storage directory or not.
     */
    protected boolean mDeepStorageStructure;
    
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
     * A setter method that is to be set to use getWorkDirectory functions, correctly.
     * 
     * @param properties  the <code>PegasusProperties</code>
     * @param options     the <code>PlannerOptions</code>
     */
    public void setForPlannerUse( PegasusProperties properties, PlannerOptions options ){
        mPlannerOptions = options;
        mWorkDir              = properties.getExecDirectory();  
        mDeepStorageStructure = properties.useDeepStorageDirectoryStructure() ;
                                //||hashedOutputMapperUsed( properties );
                                  
    }
    
    /**
     * Adds a site catalog entry to the store.
     * 
     * @param entry  the site catalog entry.
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
     * @return list of sites
     */
    public Set<String> list() {
        return mStore.keySet();
    }
    
    /**
     * Returns SiteCatalogEntry matching a site handle.
     * 
     * @param handle  the handle of the site to be looked up.
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
     * @param sites the list of site identifiers for which sysinfo is required.
     *
     *
     * @return  the sysinfo map
     */
    public Map<String,SysInfo> getSysInfos( List<String> sites ) {
        HashMap result = new HashMap();
        for ( Iterator i = sites.iterator(); i.hasNext(); ) {
            SiteCatalogEntry site = this.lookup (( String ) i.next());
            if( site != null ){
                result.put( site.getSiteHandle(), site.getSysInfo() );
            }
        }
        return result;
    }
    
    /**
     * 
     * @param sites the list of site identifiers for which sysinfo is required.
     *
     * 
     * @return  the sysinfo map
     */
    /*private Map<String,VDSSysInfo> getVDSSysInfos( List<String> sites ) {
        HashMap result = new HashMap();
        for ( Iterator i = sites.iterator(); i.hasNext(); ) {
            SiteCatalogEntry site = this.lookup (( String ) i.next());
            if( site != null ){
                result.put( site.getSiteHandle(), site.getVDSSysInfo() );
            }
        }
        return result;
    }
     */
    
    /**
     * Returns the <code>VDSSysInfo</code> for the site
     * 
     * @param handle the site handle / identifier.
     * @return the VDSSysInfo else null
     */
    public VDSSysInfo getVDSSysInfo( String handle ){
        //sanity check
        if( !this.contains( handle ) ) {
            return null;
        }
        else{
            return this.lookup( handle ).getVDSSysInfo();
        }
        
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
    @Deprecated public String getVDSHome( String handle ){
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
    @Deprecated public String getPegasusHome( String handle ){        
        if( !this.contains( handle ) ) {
            return null;
        }
        else{
            return this.lookup( handle ).getPegasusHome();
        }
    }

    
    /**
     * Returns an environment variable associated with the site. If a env value
     * is not specified in the site catalog, then only for local site
     * it falls back on the value retrieved from the environment.
     *
     * @param handle   the site handle / identifier.
     * @param variable the name of the environment variable.
     *
     * @return value of the environment variable if found, else null
     */
    public String getEnvironmentVariable( String handle, String variable ){
        
        String value = null;
        //sanity check
        if( !this.contains( handle ) ) {
            value = null;
        }
        else{
            value = this.lookup( handle ).getEnvironmentVariable( variable );
        }

        /* Moved to SiteCatalogEntry Karan Dec 15, 2011
        //change the preference order because of JIRA PM-471
        if( value == null ){
            //fall back only for local site the value in the env
            if( handle != null && handle.equals( "local" ) ){
                //try to retrieve value from environment
                //for local site.
                value = System.getenv( variable );
            }
        }
         */
        return value;
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
     * Returns a  URL to the work directory as seen externally ( including external
     * mount point ).
     *
     * @param siteHandle    the site handle.
     * @param operation     the operation for which we need the server.
     *
     * @return the url
     */
    public String getExternalWorkDirectoryURL( String siteHandle,  FileServer.OPERATION operation ){
        String url = null;

        SiteCatalogEntry site = this.lookup( siteHandle );
        if( site == null ){
            return url;
        }

        //select a file server
        FileServer fs = site.selectHeadNodeScratchSharedFileServer( operation );

        return this.getExternalWorkDirectoryURL( fs, siteHandle );
    }

    /**
     * Returns a  URL to the work directory as seen externally ( including external
     * mount point ).
     *
     * @param server        the FileServer to use
     * @param siteHandle    the site handle.
     *
     * @return the url else null
     */
    public String getExternalWorkDirectoryURL( FileServer server, String siteHandle ){
        String url = null;

        if( server == null ){
            return null;
        }

        url = server.getURLPrefix() + this.getExternalWorkDirectory( server, siteHandle );

        return url;
    }
    
    /**
     * Return the work directory as seen externally (including external mount point)
     *
     * @param fs          the FileServer with the file system
     * @param siteHandle  the site for which you want the directory
     *
     * @return    String corresponding to the mount point
     */    
    public String getExternalWorkDirectory( FileServer fs, String siteHandle) {
        
        StringBuffer path = new StringBuffer();

        if ( mWorkDir.length() == 0 ) {
            // special case - no pegasus.dir.exec
            path.append( fs.getMountPoint() );
        }
        else if ( mWorkDir.charAt( 0 ) != '/' ) {
            String mountPoint = fs.getMountPoint();

            
//            path = fs.getMountPoint() + File.separator + mWorkDir;

            // not a absolute path given - append
            path.append( mountPoint );
            if( mountPoint.charAt( mountPoint.length() - 1 ) == File.separatorChar ){
                //no need to add path separator
            }
            else{
                path.append( File.separator );
            }
        }

        //always add the mWorkDir, whatever it is
        StringBuffer addon = new StringBuffer();
        addon.append( mWorkDir );

        
        String randDir = mPlannerOptions.getRandomDirName();

        if ( randDir != null) {
            //append the random dir name to the
            //work dir constructed till now
            addon.append( File.separator );
             //append withtout any modifications
            addon.append( randDir );
        }

        path.append( addon.toString() );

        return path.toString();
    }

    /**
     * Return the relative directory that needs to be appended to the storage
     * directory for the workflow.
     *
     *
     * @return    String corresponding to the mount point if the pool is found.
     *            null if pool entry is not found.
     * 
     */
    public String getRelativeStorageDirectoryAddon(  ) {
        
        String mount_point = "";
        //check if we need to replicate the submit directory
        //structure on the storage directory
        if( mDeepStorageStructure ){
            String leaf = ( this.mPlannerOptions.partOfDeferredRun() )?
                             //if a deferred run then pick up the relative random directory
                             //this.mUserOpts.getOptions().getRandomDir():
                             this.mPlannerOptions.getRelativeDirectory():
                             //for a normal run add the relative submit directory
                             this.mPlannerOptions.getRelativeDirectory();
            
            //PM-1124 if leaf is null, means fall back to relative submit directory
            if( leaf == null ){
                leaf = this.mPlannerOptions.getRelativeSubmitDirectory();
            }
            
            File f = new File( mount_point, leaf );
            mount_point = f.getAbsolutePath();
        }


        return mount_point;

    }
 
     
    /**
     * Return the storage mount point for a particular pool.
     *
     * @param site  the site for which you want the  storage-mount-point.
     *
     * @return    String corresponding to the mount point if the pool is found.
     *            null if pool entry is not found.
     * 
     * @deprecated
     */
/*
    public String getExternalStorageDirectory( String site ) {
        
        String mount_point = mStorageDir;
        SiteCatalogEntry entry = this.lookup( site );
        
        //sanity check
        if( entry == null ){
            return null;
        }
        
        FileServer server = null;
        if ( mStorageDir.length() == 0 || mStorageDir.charAt( 0 ) != '/' ) {
            server = entry.selectStorageFileServerForStageout( FileServer.OPERATION.put );
            mount_point = server.getMountPoint();

            //removing the trailing slash if there
            int length = mount_point.length();
            if ( length > 1 && mount_point.charAt( length - 1 ) == '/' ) {
                mount_point = mount_point.substring( 0, length - 1 );
            }

            //append the Storage Dir
            File f = new File( mount_point, mStorageDir );
            mount_point = f.getAbsolutePath();

        }

        //check if we need to replicate the submit directory
        //structure on the storage directory
        if( mDeepStorageStructure ){
            String leaf = ( this.mPlannerOptions.partOfDeferredRun() )?
                             //if a deferred run then pick up the relative random directory
                             //this.mUserOpts.getOptions().getRandomDir():
                             this.mPlannerOptions.getRelativeDirectory():
                             //for a normal run add the relative submit directory
                             this.mPlannerOptions.getRelativeDirectory();
            File f = new File( mount_point, leaf );
            mount_point = f.getAbsolutePath();
        }


        return mount_point;

    }
 */ 
    
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
    public String getInternalWorkDirectory( String handle ) {
        return this.getInternalWorkDirectory( handle, null, -1 );
    }

    /**
     * This determines the working directory on remote execution pool for a
     * particular job. The job should have it's execution pool set.
     *
     * @param job <code>Job</code> object for the job.
     *
     * @return the path to the pool work dir.
     * @throws RuntimeException in case of site not found in the site catalog.
     */
    public String getInternalWorkDirectory( Job job ) {
        return this.getInternalWorkDirectory( job, false );
    }

    /**
     * This determines the working directory on remote execution pool or a staging
     * site for a particular job. The job should have it's execution pool set.
     *
     * @param job            <code>Job</code> object for the job.
     * @param onStagingSite  boolean indicating whether the work directory required
     *                       is the one on staging site.
     *
     * @return the path to the pool work dir.
     * @throws RuntimeException in case of site not found in the site catalog.
     */
    public String getInternalWorkDirectory( Job job , boolean onStagingSite ) {
        return this.getInternalWorkDirectory(
                            onStagingSite ? job.getStagingSiteHandle() : job.getSiteHandle(),
                            job.vdsNS.getStringValue( Pegasus.REMOTE_INITIALDIR_KEY ),
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
    public String getInternalWorkDirectory( String handle, String path ) {
        return this.getInternalWorkDirectory( handle, path, -1 );
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
    public String getInternalWorkDirectory( String handle, String path, int jobClass ) {
        //get the random directory name
        //sanitary check
        if( mPlannerOptions == null ){
            throw new RuntimeException(
                    "The initializeUseForPlanner() was not called before calling getWorkDirectory");
        }
    
        if(jobClass == Job.CREATE_DIR_JOB ){
            //the create dir jobs always run in the
            //workdir specified in the site catalog
            //return execPool.getHeadNodeFS().getScratch().getSharedDirectory().getInternalMountPoint().getMountPoint();
    
            //Related to JIRA PM-67 http://pegasus.isi.edu/jira/browse/PM-67
            //pegasus-get-sites generates site catalog with VO specific
            // storage mount points and work directories. These dont exist
            //by default. Hence the job needs to be launched in /tmp
            return File.separator + "tmp";
        }

        SiteCatalogEntry execPool = this.lookup( handle );
        if(execPool == null){
            throw new RuntimeException("Entry for " + handle +
                                       " does not exist in the Site Catalog");
        }



        String execPoolDir = mWorkDir;




        if ( mWorkDir.length() == 0 || mWorkDir.charAt( 0 ) != '/' ) {
            //means you have to append the
            //value specfied by pegasus.dir.exec
            File f = new File( execPool.getInternalMountPointOfWorkDirectory(), mWorkDir );
            execPoolDir = f.getAbsolutePath();
        }
        
        String randDir = mPlannerOptions.getRandomDirName();

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

    /**
     * Accept method for the SiteStore object
     *
     * @param visitor that goes through it
     */
    public void accept(SiteDataVisitor visitor) throws IOException {
        visitor.visit( this );

        for( Iterator<SiteCatalogEntry> it = this.entryIterator(); it.hasNext(); ){
            it.next().accept(visitor);
        }

        visitor.depart( this );
    }

    /**
     * Returns a boolean indicating whether the Hashed Output Mapper was used or
     * not
     * 
     * @param properties
     * 
     * @return boolean 
     */
    private boolean hashedOutputMapperUsed(PegasusProperties properties) {
        
        String mapper = properties.getProperty( OutputMapperFactory.PROPERTY_KEY );
        if( mapper == null ){
            mapper = OutputMapperFactory.DEFAULT_OUTPUT_MAPPER_IMPLEMENTATION;
        }
        
        return mapper.equals( OutputMapperFactory.HASHED_OUTPUT_MAPPER_IMPLEMENTATION );
    }

    /**
     * Set the file source.
     * 
     * @param source  the source.
     */
    public void setFileSource(File source) {
        this.mFileSource = source;
    }


    /**
     * Returns the file source.
     * 
     * @return 
     */
    public File getFileSource(){
        return this.mFileSource;
    }


    
    
}
