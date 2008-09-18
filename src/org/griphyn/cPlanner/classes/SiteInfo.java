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


package org.griphyn.cPlanner.classes;

import edu.isi.pegasus.common.logging.LoggerFactory;
import org.griphyn.common.classes.SysInfo;

import org.griphyn.cPlanner.common.PegRandom;
import org.griphyn.cPlanner.common.Utility;
import org.griphyn.cPlanner.common.LogManager;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This is a data class that is used to store information about a single
 * remote site (pool).
 *
 * <p>
 * The various types of information that can be associated with the the remote
 * site are displayed in the following table.
 *
 * <p>
 * <table border="1">
 * <tr align="left"><th>Name</th><th>Description</th></tr>
 * <tr align="left"><th>grid launch</th>
 *  <td>the path to kickstart on the remote site.</td>
 * </tr>
 * <tr align="left"><th>work directory</th>
 *  <td>the <code>WorkDir</code> object containing the information about the
 *  scratch space on the remote site.</td>
 * </tr>
 * <tr align="left"><th>grid ftp servers</th>
 *  <td>the list of <code>GridFTPServer</code> objects each containing information
 *   about one grid ftp server.</td>
 * </tr>
 * <tr align="left"><th>job managers</th>
 *  <td>the list of <code>JobManager</code> objects each containing information
 *  about one jobmanager.</td>
 * </tr>
 * <tr align="left"><th>profiles</th>
 *  <td>the list of <code>Profile</code> objects each containing one profile.</td>
 * </tr>
 * <tr align="left"><th>system info</th>
 *  <td>the <code>SysInfo</code> object containing the remote sites system
 *   information.</td>
 * </tr>
 * </table>
 *
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @author Karan Vahi vahi@isi.edu
 *
 * @version $Revision$
 *
 * @see GlobusVersion
 * @see GridFTPServer
 * @see GridFTPBandwidth
 * @see JobManager
 * @see LRC
 * @see Profile
 * @see SiteInfo
 * @see org.griphyn.common.classes.SysInfo
 * @see WorkDir
 */
public class SiteInfo {

    /**
     * Array storing the names of the attributes that are stored with the
     * site.
     */
    public static final String SITEINFO[] = {
        "grid-ftp-server", "jobmanager", "profile", "lrc",
        "workdir", "gridlaunch", "sysinfo", "handle"};


    /**
     * The constant to be passed to the accessor functions to get or set the
     * list of <code>GridFTP</code> objects for the remote site.
     */
    public static final int GRIDFTP = 0;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * list of <code>JobManager</code> objects for the remote site.
     */
    public static final int JOBMANAGER = 1;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * list of <code>Profile</code> objects for the remote site.
     */
    public static final int PROFILE = 2;

    /**
     * The constant to be passed to the accessor functions to get or set the list
     * of <code>LRC</code> objects for the remote site.
     */
    public static final int LRC = 3;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * List of <code>WorkDir</code> objects.
     */
    public static final int WORKDIR = 4;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * path to kickstart.
     */
    public static final int GRIDLAUNCH = 5;

    /**
     * The constant to be passed to the accessor functions to get or set the
     * <code>SysInfo</code> site.
     */
    public static final int SYSINFO = 6;

    /**
     * The name of the remote site. This is acts as the key by which to query
     * a site catalog for information regarding a particular remote site.
     */
    public static final int HANDLE = 7;

    /**
     * The path to the kickstart on the remote site.
     */
    private String mGridLaunch ;

    /**
     * The list of <code>LRC</code> objects that contain the information about
     * the various LRCs associated with the remote site.
     */
    private List mLRCList;

    /**
     * The list of <code>Profile</code> objects that contain the profile
     * information associated with the remote site.
     */
    private List mProfileList ;

    /**
     * The list of <code>GridFTPServer</code> objects that contain the information
     * about the gridftp servers on the remote site.
     */
    private List mGridFTPList ;

    /**
     * The list of <code>JobManager</code> objects that contain the information
     * about the jobmanagers associated with the remote site.
     */
    private List mJobManagerList ;

    /**
     * Contains the information about the work directory on the remote site.
     */
    private WorkDir mWorkDir;

    /**
     *  The system information of the remote site.
     */
    private SysInfo mSysInfo;

    /**
     * The handle to the site, usually name of the site.
     */
    private String mHandle;

    /**
     * Default Constructor.
     */
    public SiteInfo() {
        mHandle           = null;
        mLRCList        = new ArrayList(3);
        mProfileList    = new ArrayList(3);
        mGridFTPList    = new ArrayList(3);
        mJobManagerList = new ArrayList(5);
        mSysInfo        = new SysInfo();
        mWorkDir        = new WorkDir();
    }

    /**
     * Returns an <code>Object</code> containing the attribute value
     * corresponding to the key specified.
     *
     * @param key the key.
     *
     * @return <code>Object</code> corresponding to the key value.
     * @throws RuntimeException if illegal key defined.
     *
     *
     * @see #HANDLE
     * @see #GRIDFTP
     * @see #GRIDLAUNCH
     * @see #JOBMANAGER
     * @see #LRC
     * @see #PROFILE
     * @see #SYSINFO
     * @see #WORKDIR
     */
    public Object getInfo(int key) {

        switch (key) {
            case 0:
                return mGridFTPList;

            case 1:
                return mJobManagerList;

            case 2:
                return mProfileList;

            case 3:
                return mLRCList;

            case 4:
                return mWorkDir;

            case 5:
                return mGridLaunch;

            case 6:
                return mSysInfo;

            case 7:
                return mHandle;

            default:
                throw new RuntimeException(
                    " Wrong site key. Please use one of the predefined key types");
        }

    }

    /**
     * A helper method that returns the execution mount point.
     *
     * @return the execution mount point, else
     *         null if no mount point associated with the pool.
     */
    public String getExecMountPoint(){
        Object workdir = getInfo(this.WORKDIR);

        return (workdir == null)?
                null:
                ((WorkDir)workdir).getInfo(WorkDir.WORKDIR);
    }

    /**
     * A helper method that returns the path to gridlaunch on the site.
     *
     * @return the path to the kickstart.
     */
    public String getKickstartPath(){
        Object path = getInfo(this.GRIDLAUNCH);

        return (path == null)?
                null:
                ((String)path);
    }


    /**
     * A helper method that returns the url prefix for one of the gridftp server
     * associated with the pool. If more than one gridftp servers is associated
     * with the pool, then the function returns url prefix for the first
     * gridftp server in the list, unless the parameter random is set to true.
     *
     * @param random boolean denoting whether to select a random gridftp server.
     *
     * @return the url prefix for the grid ftp server,
     *         else null if no gridftp server mentioned.
     */
    public String getURLPrefix(boolean random){
        String url = null;
        GridFTPServer server = selectGridFTP(random);
        url = server.getInfo(GridFTPServer.GRIDFTP_URL);
        //on the safe side should prune also..
        return Utility.pruneURLPrefix(url);
    }

    /**
     * It returns all the jobmanagers corresponding to a specified pool.
     *
     * @return  list of <code>JobManager</code>, each referring to
     *          one jobmanager contact string. An empty list if no jobmanagers
     *          found.
     */
    public List getJobmanagers() {
        Object obj;
        return ((obj = getInfo(this.JOBMANAGER)) == null)?
               new java.util.ArrayList(0):
               (List)obj;
    }

    /**
     * It returns all the jobmanagers corresponding to a specified pool and
     * universe.
     *
     * @param universe the gvds universe with which it is associated.
     *
     * @return  list of <code>JobManager</code>, each referring to
     *          one jobmanager contact string. An empty list if no jobmanagers
     *          found.
     */
    public List getJobmanagers(String universe) {
        Object obj;
        return ((obj = getInfo(this.JOBMANAGER)) == null)?
               new java.util.ArrayList(0):
               this.getMatchingJMList((List)obj,universe);
    }


    /**
     * Sets an attribute associated with the remote site. It actually
     * adds to the list where there is a list maintained like for grid ftp servers,
     * jobmanagers, profiles, and LRCs.
     *
     * @param key    the attribute key, which is one of the predefined keys.
     * @param object the object containing the attribute value.
     *
     * @throws RuntimeException if the object passed for the key is not of
     *         valid type.
     *
     * @throws Exception if illegal key defined.
     *
     *
     * @see #HANDLE
     * @see #GRIDFTP
     * @see #GRIDLAUNCH
     * @see #JOBMANAGER
     * @see #LRC
     * @see #PROFILE
     * @see #SYSINFO
     * @see #WORKDIR
     */
    public void setInfo(int key, Object object) throws RuntimeException {

        //to denote if object is of valid type or not.
        boolean valid = true;

        switch (key) {
            case GRIDFTP:
                if (object != null && object instanceof GridFTPServer)
                    mGridFTPList.add(object);
                else
                    valid = false;
                break;

            case JOBMANAGER:
                if (object != null && object instanceof JobManager)
                    mJobManagerList.add(object);
                else
                    valid = false;
                break;

            case PROFILE:
                if (object != null && object instanceof Profile)
                    mProfileList.add(object);
                else
                    valid = false;
                break;

            case LRC:
                if (object != null && object instanceof LRC)
                    mLRCList.add(object);
                else
                    valid = false;
                break;

            case WORKDIR:
                if(object != null && object instanceof WorkDir)
                    mWorkDir = (WorkDir) object;
                else{
                    valid = false;
                    mWorkDir = null;
                }

                break;

            case GRIDLAUNCH:
                if(object != null && object instanceof String)
                    mGridLaunch = (String) object;
                else{
                    valid = false;
                    mGridLaunch = null;
                }
                break;

            case SYSINFO:

                if(object != null && object instanceof String)
                    mSysInfo = new SysInfo((String) object);
                else if(object != null && object instanceof SysInfo){
                    mSysInfo = (SysInfo)object;
                }
                else{
                    valid = false;
                    mSysInfo = null;
                }

                break;

            case HANDLE:
                if(object != null && object instanceof String)
                    mHandle = (String) object;
                else{
                    valid = false;
                    mHandle = null;
                }
                break;


            default:
                throw new RuntimeException(
                " Wrong site key. Please use one of the predefined key types");
        }

        //if object is not null , and valid == false
        //throw exception
        if(!valid && object != null){
            throw new RuntimeException("Invalid object passed for key " +
                                       SITEINFO[key]);
        }
    }


    /**
     * It removes a jobmanager from the pool. It calls the underlying equals
     * method of the associated jobmanager object to remove it.
     *
     * @param universe          the gvds universe with which it is associated.
     * @param jobManagerContact the contact string to the jobmanager.
     *
     * @return true if was able to remove successfully
     *         else false.
     */
    public boolean removeJobmanager(String universe, String jobManagerContact) {
        if (mJobManagerList == null) {
            return false;
        }

        JobManager jm = new JobManager();
        boolean val = false;

        try {
            jm.setInfo(JobManager.UNIVERSE, universe);
            jm.setInfo(JobManager.URL, jobManagerContact);
        }
        catch (Exception e) {
            //wonder why gaurang throws it
            LoggerFactory.loadSingletonInstance().
                log("Exception while removing jobmanager:" + e.getMessage(),
                    LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        synchronized(mJobManagerList){
            val = mJobManagerList.remove(jm);
        }
        jm  = null;
        return val;
    }

    /**
     * Removes a grid ftp server from the soft state associated with the pool.
     *
     * @param urlPrefix the urlprefix associated with the server.
     *
     * @return boolean
     */
    public boolean removeGridFtp(String urlPrefix){
        if(mGridFTPList == null)
            return false;

        GridFTPServer server = new GridFTPServer();
        boolean val = false;

        try{
            server.setInfo(GridFTPServer.GRIDFTP_URL, urlPrefix);
        }
        catch(Exception e){
            //wonder why gaurang throws it
            LoggerFactory.loadSingletonInstance().log(
                "Exception while removing jobmanager:" + e.getMessage(),
                LogManager.ERROR_MESSAGE_LEVEL);
            return false;

        }
        synchronized(mGridFTPList){
            val = mGridFTPList.remove(server);
            server = null;
        }
        return val;
    }

    /**
     * Returns a gridftp server from the list of gridftp servers associated with
     * the site. If more than one candidate GridFTPServer is found , then the
     * function returns the first matching <code>GridFTPServer</code>
     * unless parameter random is set to true.
     *
     * @param random boolean denoting whether to select a random gridftp server.
     *
     * @return the selected <code>GridFTPServer</code> corresponding to the
     *         grid ftp server,
     *         else null if list is null.
     *
     * @see org.griphyn.cPlanner.classes.GridFTPServer
     */
    public GridFTPServer selectGridFTP(boolean random) {
        List l = (List) this.getInfo(SiteInfo.GRIDFTP);
        //sanity check
        if(l == null || l.isEmpty())
            return null;

        int sel = (random == true)?
                  PegRandom.getInteger(l.size() - 1):
                  0;

        return (GridFTPServer) (l.get(sel));
    }

    /**
     * Returns an LRC from the list of LRCs associated with the site.
     * If more than one candidate LRC is found , then the function
     * the first matching <code>LRC</code< unless parameter random is set to true.
     *
     * @param random boolean denoting whether to select a random gridftp server.
     *
     * @return the selected <code>LRC</code> corresponding to the selected LRC.
     *         else null if list is null.
     *
     * @see org.griphyn.cPlanner.classes.LRC
     */
    public LRC selectLRC(boolean random) {
        List l = (List) this.getInfo(SiteInfo.LRC);
        //sanity check
        if(l == null || l.isEmpty())
            return null;

        int sel = (random == true)?
                  PegRandom.getInteger(l.size() - 1):
                  0;

        return (LRC) (l.get(sel));
    }


    /**
     * Returns a selected jobmanager corresponding to a particular VDS
     * universe.
     * If more than one candidate jobmanager is found , then the function
     * the first matching jobmanager unless parameter random is set to true.
     *
     * @param universe the VDS universe with which the jobmanager is associated.
     * @param random boolean denoting whether to select a random gridftp server.
     *
     * @return the selected jobmanager,
     *         else null if list is null.
     *
     * @see org.griphyn.cPlanner.classes.JobManager
     */
    public JobManager selectJobManager(String universe, boolean random) {
        List l = (List) this.getInfo(SiteInfo.JOBMANAGER);
        //sanity check
        if(l == null || l.isEmpty())
            return null;

        //match on the universe
        l = this.getMatchingJMList(l,universe);

        //do a sanity check again
        if(l == null || l.isEmpty())
            return null;

        int sel = (random == true)?
                  PegRandom.getInteger(l.size() - 1):
                  0;

        return (JobManager) (l.get(sel));
    }

    /**
     * Returns the textual description of the  contents of <code>SiteInfo</code>
     * object in the multiline format.
     *
     * @return the textual description in multiline format.
     */
    public String toMultiLine() {
        String output =  "site " + mHandle + "{\n";
        if(mSysInfo !=null) {
            output+="sysinfo \""+mSysInfo+"\"\n";
        }
        if (mGridLaunch != null) {
            output += "gridlaunch \"" + mGridLaunch + "\"\n";
        }
        if (mWorkDir != null) {
            output += mWorkDir.toMultiLine()+"\n";
        }
        if (!mGridFTPList.isEmpty()) {
            for (Iterator i = mGridFTPList.iterator(); i.hasNext(); ) {
                output += ( (GridFTPServer) i.next()).toMultiLine() + "\n";
            }
        }
        if (!mJobManagerList.isEmpty()) {
            for (Iterator i = mJobManagerList.iterator(); i.hasNext(); ) {
                output += ( (JobManager) i.next()).toMultiLine() +
                    "\n";
            }
        }
        if (!mLRCList.isEmpty()) {
            for (Iterator i = mLRCList.iterator(); i.hasNext(); ) {
                output += ( (LRC) i.next()).toMultiLine() + "\n";
            }
        }
        if (!mProfileList.isEmpty()) {
            for (Iterator i = mProfileList.iterator(); i.hasNext(); ) {
                output += ( (Profile) i.next()).toMultiLine() + "\n";
            }
        }
        output += "}\n";
        // System.out.println(output);
        return output;

    }



    /**
     * Returns the textual description of the  contents of <code>SiteInfo</code>
     * object.
     *
     * @return the textual description.
     */
    public String toString() {
        String output = "{\n";
        if(mSysInfo !=null) {
            output+="sysinfo \""+mSysInfo+"\"\n";
        }
        if (mGridLaunch != null) {
            output += "gridlaunch \"" + mGridLaunch + "\"\n";
        }
        if (mWorkDir != null) {
            output += mWorkDir.toString()+"\n";
        }
        if (!mGridFTPList.isEmpty()) {
            for (Iterator i = mGridFTPList.iterator(); i.hasNext(); ) {
                output += ( (GridFTPServer) i.next()).toString() + "\n";
            }
        }
        if (!mJobManagerList.isEmpty()) {
            for (Iterator i = mJobManagerList.iterator(); i.hasNext(); ) {
                output += ( (JobManager) i.next()).toString() +
                    "\n";
            }
        }
        if (!mLRCList.isEmpty()) {
            for (Iterator i = mLRCList.iterator(); i.hasNext(); ) {
                output += ( (LRC) i.next()).toString() + "\n";
            }
        }
        if (!mProfileList.isEmpty()) {
            for (Iterator i = mProfileList.iterator(); i.hasNext(); ) {
                output += ( (Profile) i.next()).toString() + "\n";
            }
        }
        output += "}\n";
        // System.out.println(output);
        return output;
    }

    /**
     * Returns the XML description of the  contents of <code>SiteInfo</code>
     * object.
     *
     * @return the xml description.
     */
    public String toXML() {
        String output = "";
        if (mGridLaunch != null) {
            output += " gridlaunch=\"" + mGridLaunch + "\"";
        }
        if(mSysInfo!=null)
        {
            output+=" sysinfo=\""+mSysInfo+"\"";
        }
        output += ">\n";
        if (!mProfileList.isEmpty()) {
            for (Iterator i = mProfileList.iterator(); i.hasNext(); ) {
                output += "    " + ( (Profile) i.next()).toXML() + "\n";
            }
        }
        if (!mLRCList.isEmpty()) {
            for (Iterator i = mLRCList.iterator(); i.hasNext(); ) {
                output += "    " + ( (LRC) i.next()).toXML() + "\n";
            }
        }
        if (!mGridFTPList.isEmpty()) {
            for (Iterator i = mGridFTPList.iterator(); i.hasNext(); ) {
                output += "    " + ( (GridFTPServer) i.next()).toXML() + "\n";
            }
        }
        if (!mJobManagerList.isEmpty()) {
            for (Iterator i = mJobManagerList.iterator(); i.hasNext(); ) {
                output += "    " + ( (JobManager) i.next()).toXML() +
                    "\n";
            }
        }

        if (mWorkDir != null) {
            output += "    " + mWorkDir.toXML() + "\n";
        }

        output += "  </site>\n";
        return output;
    }

    /**
     * Returns a list containing only those jobmanager entries that match a
     * particular universe.
     *
     * @param  superList  the list containing all the entries of type <code>
     *                    JobManager</code>.
     * @param  universe   the universe against which you want to match the
     *                    entries.
     *
     * @return List which is a subset of the elements in the superList
     */
    private List getMatchingJMList(List superList, String universe) {
        ArrayList subList = new ArrayList(0);

        for (Iterator i = superList.iterator(); i.hasNext(); ) {
            JobManager jbinfo = (JobManager) i.next();

            if (jbinfo.getInfo(JobManager.UNIVERSE).
                equalsIgnoreCase(universe)) {

                subList.add(jbinfo);
            }
        }

        return subList;

    }



}
