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

package org.griphyn.cPlanner.poolinfo;

import org.griphyn.cPlanner.classes.PoolConfig;
import org.griphyn.cPlanner.classes.SiteInfo;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.common.classes.SysInfo;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

/**
 * An abstract implementation of the PoolInfoProvider. Implementations should
 * extend it, only if they are statically loading information into a
 * <code>PoolConfig</code> object. The object once populated contains all
 * the contents of the catalog.
 *
 * @author Karan Vahi
 * @version $Revision$
 *
 * @see #mPoolConfig
 */
public abstract class Abstract extends PoolInfoProvider {

    /**
     * Handle to the PoolConfig object
     */
    protected PoolConfig mPoolConfig = null;


    /**
     * Returns the System information for a bunch of sites.
     *
     * @param siteids List The siteid whose system information is required
     *
     * @return Map  The key is the siteid and the value is a SysInfo object
     *
     * @see org.griphyn.common.classes.SysInfo
     */
    public Map getSysinfos( List siteids ) {
        logMessage("Map getSysinfos(List siteIDS)");
        logMessage("\t getSysinfos(" + siteids + ")");
        HashMap sysinfomap = null;
        for ( Iterator i = siteids.iterator(); i.hasNext(); ) {
            String site = ( String ) i.next();
            SiteInfo siteinfo = mPoolConfig.get( site );
            if ( siteinfo != null ) {
                if ( sysinfomap == null ) {
                    sysinfomap = new HashMap( 5 );
                }
                sysinfomap.put( site, siteinfo.getInfo( SiteInfo.SYSINFO ) );
            }
        }
        return sysinfomap;
    }

    /**
     * Returns the System information for a single site.
     *
     * @param siteID String The site whose system information is requested
     *
     * @return SysInfo The system information as a SysInfo object
     *
     * @see org.griphyn.common.classes.SysInfo
     */
    public SysInfo getSysinfo( String siteID ) {
        logMessage("SysInfo getSysinfo(String siteID)");
        logMessage("\t getSysinfo(" + siteID + ")");
        SiteInfo siteinfo = mPoolConfig.get( siteID );
        if ( siteinfo != null ) {
            return ( SysInfo ) siteinfo.getInfo( SiteInfo.SYSINFO );
        }
        return null;
    }

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
    public SiteInfo getPoolEntry( String siteID, String universe ) {
        logMessage("SiteInfo getPoolEntry(String siteID,String universe)");
        logMessage("\tSiteInfo getPoolEntry(" + siteID + "," + universe +")");
        SiteInfo site = mPoolConfig.get( siteID);
        return site;
    }

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
    public List getPoolProfile( String siteID ) {
        logMessage("List getPoolProfile(String siteID)");
        logMessage("\tList getPoolProfile(" + siteID + ")");
        SiteInfo poolInfo = mPoolConfig.get( siteID );
        ArrayList profileList = null;

        try {
            profileList = ( poolInfo == null ) ? null :
                ( ArrayList ) poolInfo.getInfo( SiteInfo.PROFILE );

            if ( profileList == null  ) {
                return null;
            }

        } catch ( Exception e ) {
            throw new RuntimeException( "While getting profiles for site " + siteID ,
                                        e );
        }
        return profileList;
    }

    /**
     * It returns all the jobmanagers corresponding to a specified site.
     *
     * @param siteID  the name of the site at which the jobmanager runs.
     *
     * @return  list of <code>JobManager</code>, each referring to
     *          one jobmanager contact string. An empty list if no jobmanagers
     *          found.
     */
    public List getJobmanagers( String siteID ) {
        logMessage("List getJobmanagers(String siteID)");
        logMessage("\tList getJobamager(" + siteID + ")");
        SiteInfo poolInfo = mPoolConfig.get( siteID );
        return ( poolInfo == null ) ?
            new java.util.ArrayList( 0 ) :
            poolInfo.getJobmanagers();
    }

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
    public List getJobmanagers( String siteID, String universe ) {
        logMessage("List getJobmanagers(String siteID,String universe");
        logMessage("\tList getJobmanagers( " + siteID + "," + universe + ")");
        SiteInfo poolInfo = mPoolConfig.get( siteID );
        return ( poolInfo == null ) ?
            new java.util.ArrayList( 0 ) :
            poolInfo.getJobmanagers( universe );
    }

    /**
     * It returns all the gridftp servers corresponding to a specified pool.
     *
     * @param siteID  the name of the site at which the jobmanager runs.
     *
     * @return  List of <code>GridFTPServer</code>, each referring to one
     *          GridFtp Server.
     */
    public List getGridFTPServers( String siteID ) {
        logMessage("List getGridFTPServers(String siteID)");
        logMessage("\tList getGridFTPServers(" + siteID + ")" );
        SiteInfo poolInfo = mPoolConfig.get( siteID );
        if ( poolInfo == null ) {
            return new java.util.ArrayList();
        }

        ArrayList gridftp = ( ArrayList ) poolInfo.getInfo( SiteInfo.
            GRIDFTP );

        return gridftp;
    }

    /**
     * It returns all the pools available in the site catalog
     *
     * @return  List of names of the pools available as String
     */
    public  List getPools() {
        logMessage("List getPools()");
        Set s = mPoolConfig.getSites().keySet();
        return new ArrayList( s );
    }

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
    public boolean removeJobManager( String siteID, String universe,
        String jobManagerContact ) {
        logMessage("boolean removeJobManager(String siteID, String universe," +
                   "String jobManagerContact)");
        logMessage("\tboolean removeJobManager(" + siteID + "," + universe + "," +
                   jobManagerContact + ")");
        SiteInfo poolinfo = mPoolConfig.get( siteID );

        return ( poolinfo == null ) ?
            false :
            poolinfo.removeJobmanager( universe, jobManagerContact );

    }

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
    public boolean removeGridFtp( String siteID, String urlPrefix ) {
        logMessage("boolean removeGrid(String siteID, String urlPrefix)");
        logMessage("\t boolean removeGrid(" + siteID + "," + urlPrefix + ")");
        SiteInfo poolinfo = mPoolConfig.get( siteID );

        return ( poolinfo == null ) ?
            false :
            poolinfo.removeGridFtp( urlPrefix );

    }


}
