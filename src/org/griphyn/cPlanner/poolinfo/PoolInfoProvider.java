/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */

package org.griphyn.cPlanner.poolinfo;

import org.griphyn.cPlanner.classes.GridFTPServer;
import org.griphyn.cPlanner.classes.LRC;
import org.griphyn.cPlanner.classes.PoolConfig;
import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.WorkDir;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegRandom;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.UserOptions;
import org.griphyn.cPlanner.common.Utility;

import org.griphyn.cPlanner.engine.Engine;

import org.griphyn.cPlanner.namespace.Namespace;
import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.classes.SysInfo;

import java.io.File;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is an abstract class which defines the interface for the information
 * providers like sites.xml, sites.catalog.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */

public abstract class PoolInfoProvider {

    /**
     * The name of the environment variable PEGASUS_HOME.
     */
    public static final String PEGASUS_HOME = "PEGASUS_HOME";


    /**
     * The name of the environment variable VDS_HOME.
     */
    public static final String VDS_HOME = "VDS_HOME";


    /**
     * The LogManager object which is used to log all the messages. It's values
     * are set in the CPlanner class.
     */
    protected LogManager mLogger;

    /**
     * The String holding the log message.
     */
    protected String mLogMsg;

    /**
     * The path to the pool information provider.
     */
    protected String mPoolProvider;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The working directory relative to the mount point of the execution pool.
     * It is populated from the pegasus.dir.exec property from the properties file.
     * If not specified then it work_dir is supposed to be the exec mount point
     * of the execution pool.
     */
    protected String mWorkDir;

    /**
     * This contains the storage directory relative to the se mount point of the
     * pool. It is populated from the pegasus.dir.storage property from the properties
     * file. If not specified then the storage directory is the se mount point
     * from the pool.config file.
     */
    protected String mStorageDir;

    /**
     * Handle to the Singleton instance containing the options passed to the
     * planner at run time.
     */
    protected UserOptions mUserOpts;

    /**
     * A boolean indicating whether to have a deep directory structure for
     * the storage directory or not.
     */
    protected boolean mDeepStorageStructure;

    /**
     *
     * The method which returns a Singleton instance of the derived InfoProvider
     * class. It must be overriden, by implementing classes.
     *
     * @param poolProvider the url to site catalog source. Can be a URL.
     *
     * @return PoolInfoProvider
     */
    public static PoolInfoProvider singletonInstance( String poolProvider ) {
        String msg = "The method singletonInstance(String) not implemented " +
            "\nby the pool provider implementing class";

        //throw an exception
        //it seems the derived class did not define the method
        throw new java.lang.UnsupportedOperationException( msg );

    }

    /**
     * The method that returns a Non Singleton instance of the dervived
     * InfoProvider class. This method if invoked should also ensure that all
     * other internal Pegasus objectslike PegasusProperties are invoked
     * in a non singleton manner. It must be overriden, by implementing
     * classes.
     *
     * @param poolProvider  the path to the file containing the pool information.
     * @param propFileName  the name of the properties file that needs to be
     *                      picked up from PEGASUS_HOME/etc directory.If it is null,
     *                      then the default file should be picked up.
     * @return PoolInfoProvider
     */
    public static PoolInfoProvider nonSingletonInstance( String poolProvider,
        String propFileName ) {

        String msg =
            "The method nonSingletonInstance(String,String) not implemented " +
            "\nby the pool provider implementing class";

        //throw an exception
        //it seems the derived class did not define the method
        throw new java.lang.UnsupportedOperationException( msg );

    }

    /**
     * It loads the objects that the pool providers need in a singleton manner,
     * wherever possible. If the class in not implemented in Singleton manner,
     * the objects would be loaded normally.
     */
    protected void loadSingletonObjects() {
        mLogger = LogManager.getInstance();
        mLogMsg = new String();
        mPoolProvider = new String();
        mProps = PegasusProperties.getInstance();
        mUserOpts = UserOptions.getInstance();
        mWorkDir = mProps.getExecDirectory();
        mStorageDir = mProps.getStorageDirectory();
        mDeepStorageStructure = mProps.useDeepStorageDirectoryStructure();
    }


    /**
     * It loads the objects using their non singleton implementations.
     *
     *
     * @param propFileName  the name of the properties file that needs to be
     *                      picked up from PEGASUS_HOME/etc directory.If it is null,
     *                      then the default properties file should be picked up.
     *
     */
    public void loadNonSingletonObjects( String propFileName ) {
        //these should be invoked in non singleton
        //manner but is not.
        mLogger = LogManager.getInstance();

        mLogMsg = new String();
        mPoolProvider = new String();
        mProps = PegasusProperties.getInstance( ( propFileName == null ) ?
            //load the default properties file
            org.griphyn.common.util.VDSProperties.PROPERTY_FILENAME :
            //load the file with this name from $PEGASUS_HOME/etc directory
            propFileName );

        //these should be invoked in non singleton
        //manner but is not.
        mUserOpts = UserOptions.getInstance();

        mWorkDir = mProps.getExecDirectory();
        mStorageDir = mProps.getStorageDirectory();
        mDeepStorageStructure = mProps.useDeepStorageDirectoryStructure();
    }


    /**
     * Returns the System information for a bunch of sites.
     *
     * @param siteids List The siteid whose system information is required
     *
     * @return Map  The key is the siteid and the value is a SysInfo object
     *
     * @see org.griphyn.common.classes.SysInfo
     */
    public abstract Map getSysinfos( List siteids );

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
     * Gets the pool information from the pool.config file on the basis
     * of the name of the pool, and the universe.
     *
     * @param siteID   the name of the site
     * @param universe the execution universe for the job
     *
     * @return    the corresponding pool object for the entry if found
     *            else null
     */
    public abstract SiteInfo getPoolEntry( String siteID, String universe );

    /**
     * It returns the profile information associated with a particular pool. If
     * the pool provider has no such information it should return null.
     * The name of the object may purport that it is specific to GVDS format, but
     * in fact it a tuple consisting of namespace, key and value that can be used
     * by other Pool providers too.
     *
     * @param siteID  the name of the site, whose profile information you want.
     *
     * @return List of <code>Profile</code> objects
     *         null if the information about the site is not with the pool provider.
     *
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public abstract List getPoolProfile( String siteID );

    /**
     * It returns all the jobmanagers corresponding to a specified site.
     *
     * @param siteID  the name of the site at which the jobmanager runs.
     *
     * @return  list of <code>JobManager</code>, each referring to
     *          one jobmanager contact string. An empty list if no jobmanagers
     *          found.
     */
    public abstract List getJobmanagers( String siteID );

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
    public abstract List getJobmanagers( String siteID, String universe );

    /**
     * It returns all the gridftp servers corresponding to a specified pool.
     *
     * @param siteID  the name of the site at which the jobmanager runs.
     *
     * @return  List of <code>GridFTPServer</code>, each referring to one
     *          GridFtp Server.
     */
    public abstract List getGridFTPServers( String siteID );

    /**
     * It returns all the pools available in the site catalog
     *
     * @return  List of names of the pools available as String
     */
    public abstract List getPools();

    /**
     * This is a soft state remove, that removes a jobmanager from a particular
     * pool entry. The cause of this removal could be the inability to
     * authenticate against it at runtime. The successful removal lead Pegasus
     * not to schedule job on that particular jobmanager.
     *
     * @param siteID            the name of the site at which the jobmanager runs.
     * @param universe          the gvds universe with which it is associated.
     * @param jobManagerContact the contact string to the jobmanager.
     *
     * @return true if was able to remove the jobmanager from the cache
     *         false if unable to remove, or the matching entry is not found
     *         or if the implementing class does not maintain a soft state.
     */
    public abstract boolean removeJobManager( String siteID, String universe,
        String jobManagerContact ) ;

    /**
     * This is a soft state remove, that removes a gridftp server from a particular
     * pool entry. The cause of this removal could be the inability to
     * authenticate against it at runtime. The successful removal lead Pegasus
     * not to schedule any transfers on that particular gridftp server.
     *
     * @param siteID       the name of the site at which the gridftp runs.
     * @param urlPrefix  the url prefix containing the protocol,hostname and port.
     *
     * @return true if was able to remove the gridftp from the cache
     *         false if unable to remove, or the matching entry is not found
     *         or if the implementing class does not maintain a soft state.
     *         or the information about site is not in the site catalog.
     */
    public abstract boolean removeGridFtp( String siteID, String urlPrefix );

    /**
     * Returns a textual description of the pool mode being used.
     * @return String
     */
    public abstract String getPoolMode();



    /**
     * Return a random lrc url from the list of lrc url's.
     * @param lrcs Arraylist of <code>LRC</code> objects.
     *
     * @return String Returns one of lrc url's
     * @see org.griphyn.cPlanner.classes.LRC
     */
    public String selectLRC( ArrayList lrcs ) {
        String lrcurl = null;
        if ( lrcs.size() == 1 ) {
            lrcurl = ( ( LRC ) ( lrcs.get( 0 ) ) ).getURL();
        } else {
            lrcurl = ( ( LRC ) ( lrcs.get( ( int ) Math.random() * lrcs.size() ) ) ).
                getURL();
        }
        return lrcurl;
    }

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
    public String selectWorkdir( WorkDir workdir ) throws Exception {
        return workdir.getInfo( WorkDir.WORKDIR );
    }

    /**
     * Return a random gridftp url from the list of gridftp url's.
     *
     * @param ftp Takes an ArrayList of <code>GridFTPServer</code> Objects.
     *
     * @return String Returns a single gridftp url from among many
     *
     * @see org.griphyn.cPlanner.classes.GridFTPServer
     */
    public GridFTPServer selectGridFtp( ArrayList ftp ) {
        int sel = PegRandom.getInteger( ftp.size() - 1 );
        return ( GridFTPServer ) ( ftp.get( sel ) );
    }

    /**
     * Returns the value of VDS_HOME for a site.
     *
     * @param siteID   the name of the site.
     * @return value if set else null.
     */
    public String getVDS_HOME( String siteID ){
        return this.getEnvironmentVariable( siteID, VDS_HOME );
    }


    /**
     * Returns the value of PEGASUS_HOME for a site.
     *
     * @param siteID   the name of the site.
     * @return value if set else null.
     */
    public String getPegasusHome( String siteID ){
        return this.getEnvironmentVariable( siteID, PEGASUS_HOME );
    }


    /**
     * Returns an environment variable for a particular site set in the
     * Site Catalog.
     *
     * @param siteID       the name of the site.
     * @param envVariable  the environment variable whose value is required.
     *
     * @return value of the environment variable if found, else null
     */
    public String getEnvironmentVariable( String siteID, String envVariable ){
        String result = null;

        //get all environment variables
        List envs = this.getPoolProfile( siteID, Profile.ENV );
        if ( envs == null ) { return result; }

        //traverse through all the environment variables
        for( Iterator it = envs.iterator(); it.hasNext(); ){
            Profile p = ( Profile ) it.next();
            if( p.getProfileKey().equals( envVariable ) ){
                result = p.getProfileValue();
                break;
            }
        }

        return result;
    }


    /**
     * It returns profile information associated with a particular namespace and
     * pool.
     *
     * @param siteID    the name of the site, whose profile information you want.
     * @param namespace the namespace correspoinding to which the profile
     *                  information of a particular site is desired.
     *
     * @return List of <code>Profile</code> objects
     *         NULL when the information about the site is not there or no
     *         profile information associated with the site.
     *
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public List getPoolProfile( String siteID, String namespace ) {
        logMessage("List getPoolProfile(String siteID, String namespace");
        logMessage("\tList getPoolProfile(" + siteID + "," + namespace +")");
        List profileList = null;
        ArrayList namespList = null;
        //sanity checks
        if ( siteID == null || namespace == null ||
            namespace.length() < 2 ) {
            return null;
        }

        //check if the namespace asked for
        //is a valid namespace or not
        if ( !Namespace.isNamespaceValid( namespace ) ) {
            mLogger.log( "Namespace " + namespace +
                " not suppored. Ignoring", LogManager.WARNING_MESSAGE_LEVEL);
            return null;
        }

        //get information about all the profiles
        profileList = this.getPoolProfile( siteID );

        if ( profileList == null ) {
            return profileList;
        }

        //iterate through the list and add to the namespace list
        Iterator it = profileList.iterator();
        namespList = new ArrayList( 3 );
        Profile poolPf = null;
        while ( it.hasNext() ) {
            poolPf = ( Profile ) it.next();
            if ( poolPf.getProfileNamespace().equalsIgnoreCase( namespace ) ) {
                namespList.add( poolPf );
            }
        }

        if ( namespList.isEmpty() ) {
            namespList = null;

        }
        return namespList;
    }



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
    public String getExecPoolWorkDir( String executionPool ) {
        return this.getExecPoolWorkDir( executionPool, null, -1 );
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
    public String getExecPoolWorkDir( SubInfo job ) {
        return this.getExecPoolWorkDir( job.executionPool,
            job.vdsNS.getStringValue(
            VDS.REMOTE_INITIALDIR_KEY ),
            job.jobClass );
    }

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
    public String getExecPoolWorkDir( String siteID, String path ) {
        logMessage("String getExecPoolWorkDir(String siteID, String path)");
        logMessage("\t String getExecPoolWorkDir(" + siteID + "," + path + ")");
        return this.getExecPoolWorkDir( siteID, path, -1 );
    }

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
    public String getExecPoolWorkDir( String siteID, String path,int jobClass ) {
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
    }

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
                             this.mUserOpts.getOptions().getRandomDir():
                             //for a normal run add the relative submit directory
                             this.mUserOpts.getOptions().getRelativeSubmitDirectory();
            File f = new File( mount_point, leaf );
            mount_point = f.getAbsolutePath();
        }


        return mount_point;

    }


    /**
     * Gets the pool object to be used for the transfer universe. If we
     * do not get that then defaults back to globus universe for the same pool.
     *
     * @param  poolName   the name of the pool
     * @return Pool
     */
    public SiteInfo getTXPoolEntry( String poolName ) {
        SiteInfo p = this.getPoolEntry( poolName, Engine.TRANSFER_UNIVERSE );
        return p;

    }

    /**
     * Logs the message to a logging stream. Currently does not log to any stream.
     *
     * @param msg  the message to be logged.
     */
    protected void logMessage(String msg){
        //mLogger.logMessage("[Shishir] Site Catalog : " + msg);
    }
}
