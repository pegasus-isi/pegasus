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


package edu.isi.pegasus.planner.catalog.site.impl.oldimpl;


import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes.PoolConfig;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes.GridFTPServer;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes.GridFTPBandwidth;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes.SiteInfo;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes.JobManager;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes.LRC;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes.WorkDir;

/**
 * This Class queries the GT2 based Monitoring and Discovery Service (MDS)
 * and stores the remote sites information into a single data class.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @author Karan Vahi vahi@isi.edu
 *
 * @version $Revision$
 */
public class MdsQuery {

    private String mdshost; //holds the hostname for MDS
    private int mdsport; //holds the port number for MDS
    private String mdsbaseDN; //holds the baseDN for the GRIS/GIIS
    private String DEFAULT_CTX = "com.sun.jndi.ldap.LdapCtxFactory";
    private Hashtable env; //Hashtable holding connection setting to the MDS
    // private GvdsPoolConfig poolconfig = new GvdsPoolConfig();

    private static final int Gvds_Pool_Id = 0;
    private static final int Gvds_Pool_Universe = 1;
    private static final int Gvds_Pool_WorkDir = 2;
    private static final int Gvds_Pool_Lrc = 3;
    private static final int Gvds_Pool_Gridlaunch = 4;
    private static final int Gvds_Pool_Storage = 5;
    private static final int Gvds_Pool_Profile = 6;

    private static final int Mds_Computer_Total_Free_NodeCount = 7;
    private static final int Mds_Computer_Total_NodeCount = 8;
    private static final int Mds_Gram_Job_Queue_MaxCount = 9;
    private static final int Mds_Gram_Job_Queue_MaxCpuTime = 10;
    private static final int Mds_Gram_Job_Queue_MaxRunningJobs = 11;
    private static final int Mds_Gram_Job_Queue_MaxJobsInQueue = 12;
    private static final int Mds_Memory_Ram_Total_SizeMB = 13;
    private static final int Mds_Memory_Ram_FreeSizeMB = 14;
    private static final int Mds_Service_Gram_SchedulerType = 15;
    private static final int Mds_Computer_Isa = 16;
    private static final int Mds_Os_Name = 17;

    private static final int Mds_Subnetid = 18;

    /*
     * TODO:sk to add constants which represent the attributes in the
     * MDS objectclass=gridftp-pair-bandwidth-info
     */
    private static final int Host_Subnet_Id = 19;
    private static final int Dest_Subnet_Id = 20;
    private static final int Avg_Bandwidth_range1 = 21;
    private static final int Avg_Bandwidth_range2 = 22;
    private static final int Avg_Bandwidth_range3 = 23;
    private static final int Avg_Bandwidth_range4 = 24;
    private static final int Avg_Bandwidth = 25;
    private static final int Max_Bandwidth = 26;
    private static final int Min_Bandwidth = 27;

    private ArrayList m_identifiers = new ArrayList(28);

    // private SearchControls constraints;

    /**
     * C'tor for the class.
     */
    public MdsQuery() {

    }

    /**
     * Valid C'tor for the class to create a MdsQuery object.
     * Sets the SECURITY_ATHENTICATION with simple authentication.
     * Sets the PROVIDER_URL to the MDS host and port.
     * Sets the INTITIAL CONTEXT FACTORY.
     *
     * @param host  the hostname of the machine on which a GRIS or GIIS is running.
     * @param port  the Port number on which a GRIS or GIIS is running.
     */
    public MdsQuery(String host, int port) {
        mdshost = host;
        mdsport = port;
        mdsbaseDN = new String();

        env = new Hashtable();
        env.put(Context.PROVIDER_URL, "ldap://" + mdshost + ":" + mdsport);
        env.put(Context.INITIAL_CONTEXT_FACTORY, DEFAULT_CTX);
        env.put(Context.SECURITY_AUTHENTICATION, "simple");

        m_identifiers.add("Gvds-Pool-Id");
        m_identifiers.add("Gvds-Pool-Universe");
        m_identifiers.add("Gvds-Pool-WorkDir");
        m_identifiers.add("Gvds-Pool-Lrc");
        m_identifiers.add("Gvds-Pool-GridLaunch");
        m_identifiers.add("Gvds-Pool-Storage");
        m_identifiers.add("Gvds-Pool-Profile");
        m_identifiers.add("Mds-Computer-Total-Free-nodeCount");
        m_identifiers.add("Mds-Computer-Total-nodeCount");
        m_identifiers.add("Mds-Gram-Job-Queue-maxcount");
        m_identifiers.add("Mds-Gram-Job-Queue-maxcputime");
        m_identifiers.add("Mds-Gram-Job-Queue-maxrunningjobs");
        m_identifiers.add("Mds-Gram-Job-Queue-maxjobsinqueue");
        m_identifiers.add("Mds-Memory-Ram-Total-sizeMB");
        m_identifiers.add("Mds-Memory-Ram-sizeMB");
        m_identifiers.add("Mds-Service-Gram-schedulertype");
        m_identifiers.add("Mds-Computer-isa");
        m_identifiers.add("Mds-Os-name");
        m_identifiers.add("Mds-Net-netaddr");

        /**
         * sk added the attributes here as well in the same order as they are defined
         * before as this is like a hash
         */
        m_identifiers.add("Host-Subnet-Id");
        m_identifiers.add("Dest-Subnet-Id");
        m_identifiers.add("Avg-Bandwidth-range1");
        m_identifiers.add("Avg-Bandwidth-range2");
        m_identifiers.add("Avg-Bandwidth-range3");
        m_identifiers.add("Avg-Bandwidth-range4");
        m_identifiers.add("Avg-Bandwidth");
        m_identifiers.add("Max-Bandwidth");
        m_identifiers.add("Min-Bandwidth");

        // constraints = new SearchControls();
        // constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
    }

    /**
     * Enables a user to set new or overide existing connection settings to the MDS.
     *
     * @param index Holds the index string for the connection environment.
     * @param value Holds the value corresponding to the index fro the connnection environment.
     */
    public void setLdapConnectionEnv(String index, String value) {
        env.put(index, value);
    }

    public void setLdapSearchConstraints() {

    }

    /**
     * Create and return a MDS LdapContext.
     *
     * @return LdapContext
     */

    public LdapContext connectMds() throws NamingException {
        LdapContext mdsctx = new InitialLdapContext(env, null);
        return mdsctx;
    }

    /**
     * Still Thinking how to eable this option.
     */
    public NamingEnumeration searchMDS(LdapContext mdsctx, String baseDN,
                                       String filter) {

        NamingEnumeration results = null;
        return results;

    }

    public PoolConfig StoreGvdsMdsInfo(NamingEnumeration results,
                                           String baseDN,
                                           PoolConfig poolconfig) throws
        NamingException, Exception {

        SiteInfo poolinfo = null;

        String jburl = null;
        mdsbaseDN = baseDN;
        if (results == null) {
            return null;
        }

        String dn;
        String attribute;
        Attributes attrs;
        Attribute at;
        SearchResult si;

        while (results.hasMoreElements()) {
            si = (SearchResult) results.next();
            attrs = si.getAttributes();

            if (si.getName().trim().length() == 0) {
                dn = baseDN;
            } else {
                dn = si.getName() + ", " + baseDN;
            }
//      System.out.println("dn: " + dn);
            if (dn.startsWith("Gvds-Vo-name") ||
                dn.startsWith("Gvds-Software-deployment")) {
                poolinfo = new SiteInfo();

                for (NamingEnumeration ae = attrs.getAll(); ae.hasMoreElements(); ) {
                    at = (Attribute) ae.next();

                    attribute = at.getID();

                    Enumeration vals = at.getAll();

                    while (vals.hasMoreElements()) {
                        int switchkey = m_identifiers.indexOf(attribute);
                        switch (switchkey) {
                            //Setup pool id
                            case 0:
                                String poolHandle = new String( (String) vals.
                                    nextElement());
                                if (poolconfig.getSites().containsKey(
                                    poolHandle)) {
                                    java.util.Date date = new java.util.Date();
                                    poolconfig.add(poolHandle + "-" +
                                        date.getTime(),
                                        poolinfo);
                                } else {
                                    poolconfig.add(poolHandle,
                                        poolinfo);
                                }

                                // poolconfig.setPoolConfig((String) vals.nextElement(),poolinfo);
                                break;

                                //Setup pool universe's info
                            case 1:
                                StringTokenizer st = new StringTokenizer( (
                                    String) vals.nextElement(),
                                    "@");
                                String universe = st.nextToken();
                                String url = st.nextToken();
                                String globus_version = st.nextToken();

                                JobManager jobmanagerinfo = new
                                    JobManager();

                                //setting the universe,globus version and the url mappings
                                jobmanagerinfo.setInfo(
                                    JobManager.URL, url);
                                jobmanagerinfo.setInfo(
                                    JobManager.UNIVERSE, universe);
                                jobmanagerinfo.setInfo(
                                    JobManager.GLOBUS_VERSION,
                                    globus_version);
                                poolinfo.setInfo(SiteInfo.JOBMANAGER,
                                    jobmanagerinfo);
                                break;

                                //Setup the pool workdir info
                            case 2:
                                WorkDir workdir = new WorkDir();
                                st = new StringTokenizer( (String) vals.
                                    nextElement(), "@");

                                String path = null;
                                String totalsize = null;
                                String freesize = null;

                                if (st.countTokens() == 1) {
                                    path = st.nextToken();
                                } else {
                                    path = st.nextToken();
                                    totalsize = st.nextToken();
                                    freesize = st.nextToken();

                                }
                                workdir.setInfo(WorkDir.
                                    WORKDIR, path);
                                workdir.setInfo(WorkDir.
                                    TOTAL_SIZE, totalsize);
                                workdir.setInfo(WorkDir.
                                    FREE_SIZE, freesize);
                                poolinfo.setInfo(SiteInfo.WORKDIR, workdir);
                                break;

                                //Setup the pool LRC info
                            case 3:
                                LRC lrc = new LRC( (String)
                                    vals.nextElement());
                                poolinfo.setInfo(SiteInfo.LRC, lrc);
                                break;

                                //Setup the pool GridLaunch Info
                            case 4:
                                poolinfo.setInfo(SiteInfo.GRIDLAUNCH,
                                    (String) vals.nextElement());
                                break;

                                //Setup the pool Storage info
                            case 5:
                                GridFTPServer gftp = new GridFTPServer();
                                st = new StringTokenizer( (String) vals.
                                    nextElement(), "@");

                                String gftp_url = null;
                                String gftp_globus_version = null;
                                String storage_totalsize = null;
                                String storage_freesize = null;
                                if (st.countTokens() == 2) {
                                    gftp_url = st.nextToken();
                                    gftp_globus_version = st.nextToken();
                                } else {
                                    gftp_url = st.nextToken();
                                    gftp_globus_version = st.nextToken();
                                    storage_totalsize = st.nextToken();
                                    storage_freesize = st.nextToken();
                                }
                                StringTokenizer stt = new StringTokenizer(
                                    gftp_url, "/");
                                String gridftpurl = stt.nextToken() + "//" +
                                    stt.nextToken();
                                String storagedir = "";
                                while (stt.hasMoreTokens()) {
                                    storagedir += "/" + stt.nextToken();
                                }
                                gftp.setInfo(GridFTPServer.GRIDFTP_URL,
                                    gridftpurl);
                                gftp.setInfo(GridFTPServer.STORAGE_DIR,
                                    storagedir);
                                gftp.setInfo(GridFTPServer.TOTAL_SIZE,
                                    storage_totalsize);
                                gftp.setInfo(GridFTPServer.FREE_SIZE,
                                    storage_freesize);
                                gftp.setInfo(GridFTPServer.GLOBUS_VERSION,
                                    gftp_globus_version);
                                poolinfo.setInfo(SiteInfo.GRIDFTP, gftp);
                                break;

                                //Setup the pool Profile Info
                            case 6:
                                st = new StringTokenizer( (String) vals.
                                    nextElement(),
                                    "@");
                                String namespace = st.nextToken();
                                String key = st.nextToken();
                                String value = st.nextToken();
                                Profile profile = new Profile(
                                    namespace, key, value);
                                poolinfo.setInfo(SiteInfo.PROFILE, profile);
                                break;

                            default:
                                vals.nextElement();
                        }
                    }
                }
            } else if (dn.startsWith("Mds-Job-Queue-name") ||
                       dn.startsWith("Mds-Software-deployment=jobmanager")) {

                StringTokenizer dnst = new StringTokenizer(dn, ",");
                if (dn.startsWith("Mds-Job-Queue-name")) {
                    dnst.nextToken();
                }
                String jbmanager = dnst.nextToken();
                String jbhost = dnst.nextToken();
                jburl = jbhost.substring(jbhost.indexOf("=") + 1) + "/" +
                    jbmanager.substring(jbmanager.indexOf("=") + 1);

                ArrayList jobmanagers = null;
                JobManager jobmanager = null;
                for (Iterator i = poolconfig.getSites().values().iterator();
                     i.hasNext(); ) {
                    poolinfo = (SiteInfo) i.next();
                    if ( (jobmanagers = (ArrayList) poolinfo.getInfo(
                        SiteInfo.JOBMANAGER)) != null) {
                        if (!jobmanagers.isEmpty()) {
                            for (Iterator j = jobmanagers.iterator(); j.hasNext(); ) {

                                jobmanager = (JobManager) j.next();
                                if (jobmanager.getInfo(
                                    JobManager.URL).
                                    equalsIgnoreCase(jburl)) {
                                    for (NamingEnumeration ae = attrs.getAll();
                                         ae.hasMoreElements(); ) {
                                        at = (Attribute) ae.next();
                                        attribute = at.getID();
                                        Enumeration vals = at.getAll();
                                        while (vals.hasMoreElements()) {
                                            int switchkey = m_identifiers.
                                                indexOf(attribute);
                                            switch (switchkey) {
                                                //Setup other jobmanager Related Information.
                                                case 7:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.IDLE_NODES,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                case 8:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.TOTAL_NODES,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                case 9:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.MAX_COUNT,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                case 10:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.MAX_CPU_TIME,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                case 11:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.RUNNING_JOBS,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                case 12:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.
                                                        JOBS_IN_QUEUE,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                case 13:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.TOTAL_MEM,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                case 14:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.FREE_MEM,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                case 15:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.
                                                        JOBMANAGER_TYPE,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                case 16:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.ARCH_TYPE,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                case 17:
                                                    jobmanager.
                                                        setInfo(
                                                        JobManager.OS_TYPE,
                                                        (String) vals.
                                                        nextElement());
                                                    break;

                                                default:
                                                    vals.nextElement();

                                            }
                                        }
                                    }
                                }

                            } //for loop
                        }
                    }
                }
            }

            /*
             * sk added a case where the dn starts with 'Dest-Subnet-Id'
             * to gather destination bandwidth information for each gridftp server
             */

            else if (dn.startsWith("Dest-Subnet-Id")) {

                GridFTPBandwidth gridftp_bandwidth = new
                    GridFTPBandwidth();
                String dest_subnet_id = null;
                boolean flag = false; //flag to check if any elements occur

                StringTokenizer dnst = new StringTokenizer(dn, ",");
                if (dn.startsWith("Dest-Subnet-Id")) {
                    dnst.nextToken();
                }

                String gridhost = dnst.nextToken();
                String hosturl = gridhost.substring(gridhost.indexOf("=") + 1);

                ArrayList gridftpservers = null;
                GridFTPServer gridftpserver = null;
                for (Iterator i = poolconfig.getSites().values().iterator();
                     i.hasNext(); ) {
                    poolinfo = (SiteInfo) i.next();
                    if ( (gridftpservers = (ArrayList) poolinfo.getInfo(
                        SiteInfo.GRIDFTP)) != null) {
                        if (!gridftpservers.isEmpty()) {
                            for (Iterator j = gridftpservers.iterator();
                                 j.hasNext(); ) {
                                gridftpserver = (GridFTPServer) j.next();

                                /**
                                 * calculate the gridftpserver url in the form smarty.isi.edu
                                 */
                                String url = gridftpserver.getInfo(0);
                                String halfurl = url.substring(url.indexOf("/") +
                                    2);
                                String finalurl = halfurl.substring(0,
                                    halfurl.indexOf("/"));
                                //System.out.println("In url="+hosturl +" grid url="+finalurl);
                                //if (finalurl.equalsIgnoreCase(hosturl))
                                //means that the particular gridftpserver object has been found among the elements of the arraylist maintained in
                                //the poolinfo class
                                {
                                    flag = true;

                                    //System.out.println("Url has matched "+hosturl);
                                    for (NamingEnumeration ae = attrs.getAll();
                                         ae.hasMoreElements(); ) { //ae iterates over the attributes
                                        at = (Attribute) ae.next(); //get each attribute
                                        attribute = at.getID();
                                        Enumeration vals = at.getAll(); //get all the values of that attribute !

                                        while (vals.hasMoreElements()) {
                                            //form a GridFTPBandwidth object and then
                                            // call gridftpserver.setGridFtpBandwidthInfo(dest_subnet_id, object);

                                            boolean intflag = false;
                                            int switchkey = m_identifiers.
                                                indexOf(attribute);
                                            switch (switchkey) {
                                                /**
                                                 * populate the gridftp_bandwidth object with the attributes
                                                 * and then store this object in the hashmap maintained in the gridftpserver object
                                                 */

                                                //Host-Subnet-Id
                                                case 19:

                                                    //neednt store this information
                                                    vals.nextElement();
                                                    break;
                                                    //Dest-Subnet-Id
                                                case 20:
                                                    dest_subnet_id = (String)
                                                        vals.nextElement();
                                                    gridftp_bandwidth.
                                                        setInfo(
                                                        GridFTPBandwidth.
                                                        DEST_ID, dest_subnet_id);
                                                    break;
                                                    //Avg-Bandwidth-range1
                                                case 21:
                                                    gridftp_bandwidth.
                                                        setInfo(
                                                        GridFTPBandwidth.
                                                        AVG_BW_RANGE1,
                                                        (String) vals.
                                                        nextElement());
                                                    break;
                                                    //Avg-Bandwidth-range2
                                                case 22:
                                                    gridftp_bandwidth.
                                                        setInfo(
                                                        GridFTPBandwidth.
                                                        AVG_BW_RANGE2,
                                                        (String) vals.
                                                        nextElement());
                                                    break;
                                                    //Avg-Bandwidth-range3
                                                case 23:
                                                    gridftp_bandwidth.
                                                        setInfo(
                                                        GridFTPBandwidth.
                                                        AVG_BW_RANGE3,
                                                        (String) vals.
                                                        nextElement());
                                                    break;
                                                    //Avg-Bandwidth-range4
                                                case 24:
                                                    gridftp_bandwidth.
                                                        setInfo(
                                                        GridFTPBandwidth.
                                                        AVG_BW_RANGE4,
                                                        (String) vals.
                                                        nextElement());
                                                    break;
                                                    //Avg-Bandwidth
                                                case 25:
                                                    gridftp_bandwidth.
                                                        setInfo(
                                                        GridFTPBandwidth.
                                                        AVG_BW,
                                                        (String) vals.nextElement());
                                                    break;
                                                    //Max-Bandwidth
                                                case 26:
                                                    gridftp_bandwidth.
                                                        setInfo(
                                                        GridFTPBandwidth.
                                                        MAX_BW,
                                                        (String) vals.nextElement());
                                                    break;
                                                    //Min-Bandwidth
                                                case 27:
                                                    gridftp_bandwidth.
                                                        setInfo(
                                                        GridFTPBandwidth.
                                                        MIN_BW,
                                                        (String) vals.nextElement());
                                                    break;
                                                default:
                                                    intflag = true;
                                                    break;
                                            }
                                            if (intflag) {
                                                break;
                                            }
                                        } //end of While

                                        //now add the gridftp_bandwidth object in the hash maintained in the GvdsPoolGridFtp object
                                        /*
                                                                 if(flag)
                                                                 {
                                         gridftpserver.setInfo(dest_subnet_id,gridftp_bandwidth);
                                                                 }*/
                                    }

                                } //end of if matching the appropriate gridftpserver
                            } //for loop
                        }
                    }
                }
                if (flag) {
                    gridftpserver.setGridFTPBandwidthInfo(gridftp_bandwidth); //set the gridftp_bandwidth object in GvdsPoolGridFtp class
                }
            }

        }
        return poolconfig;
    }

    /**
     * Displays the result on stdout instead of putting it in data classes.
     *
     * @param results Takes a NamingEnumeration returned by the MDS search
     * @param baseDN Takes the baseDN provided to the MDS search.
     *
     */
    public void displayResults(NamingEnumeration results, String baseDN) throws
        NamingException {
        mdsbaseDN = baseDN;
        if (results == null) {
            return;
        }

        String dn;
        String attribute;
        Attributes attrs;
        Attribute at;
        SearchResult si;

        while (results.hasMoreElements()) {
            si = (SearchResult) results.next();
            attrs = si.getAttributes();

            if (si.getName().trim().length() == 0) {
                dn = baseDN;
            } else {
                dn = si.getName() + ", " + baseDN;
            }
            System.out.println("dn: " + dn);

            for (NamingEnumeration ae = attrs.getAll(); ae.hasMoreElements(); ) {
                at = (Attribute) ae.next();

                attribute = at.getID();

                Enumeration vals = at.getAll();
                while (vals.hasMoreElements()) {
                    System.out.println(attribute + ": " + vals.nextElement());
                }
            }
            System.out.println();
        }
    }

}
