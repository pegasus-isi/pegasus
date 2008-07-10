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

package edu.isi.pegasus.planner.catalog;

import edu.isi.pegasus.planner.catalog.site.*;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;

import org.griphyn.common.catalog.Catalog;

import java.io.File;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.griphyn.cPlanner.classes.GridFTPServer;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.WorkDir;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.namespace.VDS;
import org.griphyn.common.classes.SysInfo;

/**
 *
 * @author Karan Vahi
 * @version $Revision$
 */
 
        //interface  should be interface
public        abstract class  //abstract classs only temporary to make the code base compile
        SiteCatalog implements Catalog{

    /**
     * The version of the API
     */
    public static final String VERSION = "1.0";
    
    /**
     * The name of the environment variable PEGASUS_HOME.
     */
    public static final String PEGASUS_HOME = "PEGASUS_HOME";


    /**
     * The name of the environment variable VDS_HOME.
     */
    public static final String VDS_HOME = "VDS_HOME";

    /**
     * Prefix for the property subset to use with this catalog.
     */
    public static final String c_prefix = "pegasus.catalog.site";

    /**
     * Handle to the Singleton instance containing the options passed to the
     * planner at run time.
     */
//    protected UserOptions mUserOpts;

    /**
     * A boolean indicating whether to have a deep directory structure for
     * the storage directory or not.
     */
//    protected boolean mDeepStorageStructure;


    
    /**
     * Inserts a new mapping into the Site catalog.
     *
     * @param entry  the <code>SiteCatalogEntry</code> object that describes
     *               a site.
     *
     * @return number of insertions, should always be 1. On failure,
     * throw an exception, don't use zero.
     * 
     * @throws SiteCatalogException in case of error.
     */
    public abstract int insert( SiteCatalogEntry entry ) throws SiteCatalogException;
    
    /**
     * Lists  the site handles for all the sites in the Site Catalog.
     *
     * @return A set of site handles.
     * 
     * @throws SiteCatalogException in case of error.
     */
    public abstract  Set<String> list() throws SiteCatalogException;

    /**
     * Retrieves the <code>SiteCatalogEntry</code> for a site.
     *
     * @param handle   the site handle / identifier.
     * 
     * @return SiteCatalogEntry in case an entry is found , or <code>null</code>
     *         if no match is found.
     * 
     * 
     * @throws SiteCatalogException in case of error.
     */
    public abstract  SiteCatalogEntry lookup( String handle ) throws SiteCatalogException;

    /**
     * Removes a site catalog entry matching the the handle.
     *
     * @param handle  the site handle / identifier.
     * 
     * @return the number of removed entries.
     * 
     * @throws SiteCatalogException in case of error.
     */
    public abstract int remove( String handle ) throws SiteCatalogException;

    /**
     * Returns the System information for a bunch of sites.
     *
     * @param siteids List The siteid whose system information is required
     *
     * @return Map  The key is the siteid and the value is a SysInfo object
     *
     * @see org.griphyn.common.classes.SysInfo
     */
    public abstract  Map getSysinfos( List siteids );

    /**
     * Returns the System information for a single site.
     *
     * @param siteID String The site whose system information is requested
     *
     * @return SysInfo The system information as a SysInfo object
     *
     * @see org.griphyn.common.classes.SysInfo
     */
    public abstract SysInfo getSysinfo( String siteID );
    
    /**
     * It returns all the jobmanagers corresponding to a specified site.
     *
     * @param siteID  the name of the site at which the jobmanager runs.
     *
     * @return  list of <code>JobManager</code>, each referring to
     *          one jobmanager contact string. An empty list if no jobmanagers
     *          found.
     */
//    public List getJobmanagers( String siteID );

    /**
     * It returns all the jobmanagers corresponding to a specified pool and
     * universe.
     *
     * @param siteID     the name of the site at which the jobmanager runs.
     * @param universe the gvds universe with which it is associated.
     *
     * @return  list of <code>JobManager</code>, each referring to
     *          one jobmanager contact string. An empty list if no jobmanagers
     *          found.
     */
//    public abstract List getJobmanagers( String siteID, String universe );

    /**
     * It returns all the gridftp servers corresponding to a specified pool.
     *
     * @param siteID  the name of the site at which the jobmanager runs.
     *
     * @return  List of <code>GridFTPServer</code>, each referring to one
     *          GridFtp Server.
     */
//    public abstract List getGridFTPServers( String siteID );

  
    

  
    /**
     * Returns the path to the execution mount point (The Workdir).
     *
     * @param workdir the <code>WorkDir</code> object containing the workdir
     *                information.
     *
     * @return String The exec-mount point (aka workdir)
     *
     * @throws Exception
     */
    public abstract String selectWorkdir( WorkDir workdir ) throws Exception;

    /**
     * Return a random gridftp url from the list of gridftp url's.
     *
     * @param ftp Takes an ArrayList of <code>GridFTPServer</code> Objects.
     *
     * @return String Returns a single gridftp url from among many
     *
     * @see org.griphyn.cPlanner.classes.GridFTPServer
     */
    public abstract GridFTPServer selectGridFtp( ArrayList ftp ) ;

    /**
     * Returns the value of VDS_HOME for a site.
     *
     * @param siteID   the name of the site.
     * @return value if set else null.
     */
    public abstract String getVDS_HOME( String siteID );


    /**
     * Returns the value of PEGASUS_HOME for a site.
     *
     * @param siteID   the name of the site.
     * @return value if set else null.
     */
    public abstract String getPegasusHome( String siteID );


    


    /**
     * This determines the working directory on remote execution pool on the
     * basis of whether an absolute path is specified in the pegasus.dir.exec directory
     * or a relative path.
     *
     * @param executionPool the pool where a job has to be executed.
     *
     * @return the path to the pool work dir.
     * @throws RuntimeException in case of site not found in the site catalog.
     */
  /*  public String getExecPoolWorkDir( String executionPool ) {
        return this.getExecPoolWorkDir( executionPool, null, -1 );
    }
*/
    /**
     * This determines the working directory on remote execution pool for a
     * particular job. The job should have it's execution pool set.
     *
     * @param job <code>SubInfo</code> object for the job.
     *
     * @return the path to the pool work dir.
     * @throws RuntimeException in case of site not found in the site catalog.
     */
  /*  public String getExecPoolWorkDir( SubInfo job ) {
        return this.getExecPoolWorkDir( job.executionPool,
            job.vdsNS.getStringValue(
            VDS.REMOTE_INITIALDIR_KEY ),
            job.jobClass );
    }
*/
    /**
     * This determines the working directory on remote execution pool on the
     * basis of whether an absolute path is specified in the pegasus.dir.exec
     * directory or a relative path.
     *
     * @param siteID  the name of the site where a job has to be executed.
     * @param path    the relative path that needs to be appended to the
     *                workdir from the execution pool.
     *
     * @return the path to the pool work dir.
     * @throws RuntimeException in case of site not found in the site catalog.
     */
  /*  public String getExecPoolWorkDir( String siteID, String path ) {
        logMessage("String getExecPoolWorkDir(String siteID, String path)");
        logMessage("\t String getExecPoolWorkDir(" + siteID + "," + path + ")");
        return this.getExecPoolWorkDir( siteID, path, -1 );
    }
*/
    /**
     * This determines the working directory on remote execution pool on the
     * basis of whether an absolute path is specified in the pegasus.dir.exec directory
     * or a relative path. If the job class happens to be a create directory job
     * it does not append the name of the random directory since the job is
     * trying to create that random directory.
     *
     * @param siteID     the name of the site where the job has to be executed.
     * @param path       the relative path that needs to be appended to the
     *                   workdir from the execution pool.
     * @param jobClass   the class of the job.
     *
     * @return the path to the pool work dir.
     * @throws RuntimeException in case of site not found in the site catalog.
     */
   /* public String getExecPoolWorkDir( String siteID, String path,int jobClass ) {
        SiteInfo execPool = this.getPoolEntry( siteID, "vanilla" );
        if(execPool == null){
            throw new RuntimeException("Entry for " + siteID +
                                       " does not exist in the Site Catalog");
        }
        String execPoolDir = mWorkDir;

        if(jobClass == SubInfo.CREATE_DIR_JOB){
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
    }*/

    /**
     * Returns the url prefix of a gridftp server on the pool.
     * gsiftp://dataserver.phys.uwm.edu/~/griphyn_test/ligodemo_output/
     * gives a URL prefix of gsiftp://dataserver.phys.uwm.edu
     *
     * @param poolName   the name of the pool.
     *
     * @return String corresponding to the url prefix if the pool is found.
     *            null if pool entry is not found.
     */
    public String getURLPrefix( String poolName ) {
        SiteInfo pool = getPoolEntry( poolName, "vanilla" );
        String urlPrefix = pool.getURLPrefix( true );

        if ( urlPrefix == null || urlPrefix.trim().length() == 0 ) {
            throw new RuntimeException( " URL prefix not specified for site "  + poolName );
        }

        return urlPrefix;

    }

    /**
     * Return the storage mount point for a particular pool.
     *
     * @param site  SiteInfo object of the site for which you want the
     *              storage-mount-point.
     *
     * @return    String corresponding to the mount point if the pool is found.
     *            null if pool entry is not found.
     */
    /*
    public String getSeMountPoint( SiteInfo site ) {
        logMessage("String getSeMountPoint(SiteInfo site)");
        String mount_point = mStorageDir;
        GridFTPServer server = null;
        if ( mStorageDir.length() == 0 || mStorageDir.charAt( 0 ) != '/' ) {
            server = site.selectGridFTP( false );
            mount_point = server.getInfo( GridFTPServer.STORAGE_DIR );

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
            String leaf = ( this.mUserOpts.getOptions().partOfDeferredRun() )?
                             //if a deferred run then pick up the relative random directory
                             //this.mUserOpts.getOptions().getRandomDir():
                             this.mUserOpts.getOptions().getRelativeSubmitDirectory():
                             //for a normal run add the relative submit directory
                             this.mUserOpts.getOptions().getRelativeSubmitDirectory();
            File f = new File( mount_point, leaf );
            mount_point = f.getAbsolutePath();
        }


        return mount_point;

    }
*/

    /**
     * Gets the pool object to be used for the transfer universe. If we
     * do not get that then defaults back to globus universe for the same pool.
     *
     * @param  poolName   the name of the pool
     * @return Pool
     *//*
    public SiteInfo getTXPoolEntry( String poolName ) {
        SiteInfo p = this.getPoolEntry( poolName, Engine.TRANSFER_UNIVERSE );
        return p;

    }*/

    private SiteInfo getPoolEntry(String siteID, String string) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private void logMessage(String string) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

}
