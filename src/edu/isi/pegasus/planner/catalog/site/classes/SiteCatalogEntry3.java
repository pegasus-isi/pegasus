/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.catalog.site.classes;

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.classes.VDSSysInfo2NMI;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway.JOB_TYPE;
import edu.isi.pegasus.planner.catalog.transformation.classes.NMI2VDSSysInfo;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegRandom;
import edu.isi.pegasus.planner.namespace.Namespace;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This data class describes a site in the site catalog.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SiteCatalogEntry3 extends AbstractSiteData {

    /** The name of the environment variable PEGASUS_BIN_DIR. */
    public static final String PEGASUS_BIN_DIR = "PEGASUS_BIN_DIR";

    /** The name of the environment variable PEGASUS_HOME. */
    public static final String PEGASUS_HOME = "PEGASUS_HOME";

    /** The name of the environment variable VDS_HOME. */
    public static final String VDS_HOME = "VDS_HOME";

    /** The site identifier. */
    private String mID;

    /** The System Information for the Site. */
    private SysInfo mSysInfo;

    /** The profiles asscociated with the site. */
    private Profiles mProfiles;

    /** The handle to the head node filesystem. */
    private HeadNodeFS mHeadFS;

    /** The handle to the worker node filesystem. */
    private WorkerNodeFS mWorkerFS;

    /** Map of grid gateways at the site for submitting different job types. */
    private Map<GridGateway.JOB_TYPE, GridGateway> mGridGateways;

    /** The list of replica catalog associated with the site. */
    private List<ReplicaCatalog> mReplicaCatalogs;

    /** The default constructor. */
    public SiteCatalogEntry3() {
        this("");
    }

    /**
     * The overloaded constructor.
     *
     * @param id the site identifier.
     */
    public SiteCatalogEntry3(String id) {
        initialize(id);
    }

    /**
     * Not implmented as yet.
     *
     * @return UnsupportedOperationException
     */
    public Iterator getFileServerIterator() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Not implemented as yet.
     *
     * @return UnsupportedOperationException
     */
    public List getFileServers() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Not implemented as yet
     *
     * @return UnsupportedOperationException
     */
    public List getGridGateways() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Initializes the object.
     *
     * @param id the site identifier.
     */
    public void initialize(String id) {
        mID = id;
        mSysInfo = new SysInfo();
        mProfiles = new Profiles();
        mGridGateways = new HashMap();
        mReplicaCatalogs = new LinkedList();
    }

    /**
     * Sets the site handle for the site
     *
     * @param id the site identifier.
     */
    public void setSiteHandle(String id) {
        mID = id;
    }

    /**
     * Returns the site handle for the site
     *
     * @return the site identifier.
     */
    public String getSiteHandle() {
        return mID;
    }

    /**
     * Sets the System Information associated with the Site.
     *
     * @param sysinfo the system information of the site.
     */
    public void setSysInfo(SysInfo sysinfo) {
        mSysInfo = sysinfo;
    }

    /**
     * Returns the System Information associated with the Site.
     *
     * @return SysInfo the system information.
     */
    public SysInfo getSysInfo() {
        return mSysInfo;
    }

    /**
     * Sets the architecture of the site.
     *
     * @param arch the architecture.
     */
    public void setArchitecture(SysInfo.Architecture arch) {
        mSysInfo.setArchitecture(arch);
    }

    /**
     * Returns the architecture of the site.
     *
     * @return the architecture.
     */
    public SysInfo.Architecture getArchitecture() {
        return mSysInfo.getArchitecture();
    }

    /**
     * Sets the OS of the site.
     *
     * @param os the os of the site.
     */
    public void setOS(SysInfo.OS os) {
        mSysInfo.setOS(os);
    }

    /**
     * Returns the OS of the site.
     *
     * @return the OS
     */
    public SysInfo.OS getOS() {
        return mSysInfo.getOS();
    }

    /**
     * Sets the sysinfo for the site.
     *
     * @param sysinfo
     */
    public void setVDSSysInfo(VDSSysInfo sysinfo) {
        this.setSysInfo(VDSSysInfo2NMI.vdsSysInfo2NMI(sysinfo));
    }

    /**
     * Returns the sysinfo for the site.
     *
     * @return getVDSSysInfo
     */
    public VDSSysInfo getVDSSysInfo() {
        return NMI2VDSSysInfo.nmiToVDSSysInfo(mSysInfo);
    }

    /**
     * Sets the OS release of the site.
     *
     * @param release the os releaseof the site.
     */
    public void setOSRelease(String release) {
        mSysInfo.setOSRelease(release);
    }

    /**
     * Returns the OS release of the site.
     *
     * @return the OS
     */
    public String getOSRelease() {
        return mSysInfo.getOSRelease();
    }

    /**
     * Sets the OS version of the site.
     *
     * @param version the os versionof the site.
     */
    public void setOSVersion(String version) {
        mSysInfo.setOSVersion(version);
    }

    /**
     * Returns the OS version of the site.
     *
     * @return the OS
     */
    public String getOSVersion() {
        return mSysInfo.getOSVersion();
    }

    /**
     * Sets the glibc version on the site.
     *
     * @param version the glibc version of the site.
     */
    public void setGlibc(String version) {
        mSysInfo.setGlibc(version);
    }

    /**
     * Returns the glibc version of the site.
     *
     * @return the OS
     */
    public String getGlibc() {
        return mSysInfo.getGlibc();
    }

    /**
     * Sets the headnode filesystem.
     *
     * @param system the head node filesystem.
     */
    public void setHeadNodeFS(HeadNodeFS system) {
        mHeadFS = system;
    }

    /**
     * Returns the headnode filesystem.
     *
     * @return the head node filesystem.
     */
    public HeadNodeFS getHeadNodeFS() {
        return mHeadFS;
    }

    /**
     * Sets the worker node filesystem.
     *
     * @param system the head node filesystem.
     */
    public void setWorkerNodeFS(WorkerNodeFS system) {
        mWorkerFS = system;
    }

    /**
     * Returns the worker node filesystem.
     *
     * @return the worker node filesystem.
     */
    public WorkerNodeFS getWorkerNodeFS() {
        return mWorkerFS;
    }

    /**
     * Returns the work directory for the compute jobs on a site.
     *
     * <p>Currently, the work directory is picked up from the head node shared filesystem.
     *
     * @return the internal mount point.
     */
    public String getInternalMountPointOfWorkDirectory() {
        return this.getHeadNodeFS()
                .getScratch()
                .getSharedDirectory()
                .getInternalMountPoint()
                .getMountPoint();
    }

    /**
     * Adds a profile.
     *
     * @param p the profile to be added
     */
    public void addProfile(Profile p) {
        // retrieve the appropriate namespace and then add
        mProfiles.addProfile(p);
    }

    /**
     * Sets the profiles associated with the file server.
     *
     * @param profiles the profiles.
     */
    public void setProfiles(Profiles profiles) {
        mProfiles = profiles;
    }

    /**
     * Returns the profiles associated with the site.
     *
     * @return profiles.
     */
    public Profiles getProfiles() {
        return mProfiles;
    }

    /**
     * Returns the value of VDS_HOME for a site.
     *
     * @return value if set else null.
     */
    @Deprecated
    public String getVDSHome() {

        String s = this.getEnvironmentVariable(VDS_HOME);
        if (s != null && s.length() > 0) {
            return s;
        }

        // fall back on bin dir - this is to ensure  a smooth transition to FHS
        s = this.getEnvironmentVariable(PEGASUS_BIN_DIR);
        if (s != null && s.length() > 0) {
            File f = new File(s + "/..");
            return f.getAbsolutePath();
        }

        return null;
    }

    /**
     * Returns the value of PEGASUS_HOME for a site.
     *
     * @return value if set else null.
     */
    @Deprecated
    public String getPegasusHome() {

        String s = this.getEnvironmentVariable(PEGASUS_HOME);
        if (s == null || s.length() == 0) {
            // fall back on bin dir - this is to ensure  a smooth transition to FHS
            s = this.getEnvironmentVariable(PEGASUS_BIN_DIR);
            if (s != null && s.length() > 0) {
                s += "/..";
            }
        }

        // normalize the path
        if (s != null && s.length() > 0) {
            File f = new File(s);
            try {
                s = f.getAbsolutePath();
            } catch (Exception e) {
                // ignore - just leave s alone
            }
        } else {
            s = null;
        }

        return s;
    }

    /**
     * Returns an environment variable associated with the site.
     *
     * @param variable the environment variable whose value is required.
     * @return value of the environment variable if found, else null
     */
    public String getEnvironmentVariable(String variable) {
        Namespace n = this.mProfiles.get(Profiles.NAMESPACES.env);
        String value = (n == null) ? null : (String) n.get(variable);

        // change the preference order because of JIRA PM-471
        if (value == null) {
            // fall back only for local site the value in the env
            String handle = this.getSiteHandle();
            if (handle != null && handle.equals("local")) {
                // try to retrieve value from environment
                // for local site.
                value = System.getenv(variable);
            }
        }

        return value;
    }

    /**
     * Returns a grid gateway object corresponding to a job type.
     *
     * @param type the job type
     * @return GridGateway
     */
    public GridGateway getGridGateway(GridGateway.JOB_TYPE type) {
        return mGridGateways.get(type);
    }

    /**
     * Selects a grid gateway object corresponding to a job type. It also defaults to other
     * GridGateways if grid gateway not found for that job type.
     *
     * @param type the job type
     * @return GridGateway
     */
    public GridGateway selectGridGateway(GridGateway.JOB_TYPE type) {
        GridGateway g = this.getGridGateway(type);
        if (g == null) {
            if (type == JOB_TYPE.transfer
                    || type == JOB_TYPE.cleanup
                    || type == JOB_TYPE.register) {
                return this.selectGridGateway(JOB_TYPE.auxillary);
            } else if (type == JOB_TYPE.auxillary) {
                return this.selectGridGateway(JOB_TYPE.compute);
            }
        }
        return g;
    }

    /**
     * Return an iterator to value set of the Map.
     *
     * @return Iterator<GridGateway>
     */
    public Iterator<GridGateway> getGridGatewayIterator() {
        return mGridGateways.values().iterator();
    }

    /**
     * Add a GridGateway to the site.
     *
     * @param g the grid gateway to be added.
     */
    public void addGridGateway(GridGateway g) {
        mGridGateways.put(g.getJobType(), g);
    }

    /**
     * This is a soft state remove, that removes a GridGateway from a particular site.
     *
     * @param contact the contact string for the grid gateway.
     * @return true if was able to remove the jobmanager from the cache false if unable to remove,
     *     or the matching entry is not found or if the implementing class does not maintain a soft
     *     state.
     */
    public boolean removeGridGateway(String contact) {
        // iterate through the entry set
        for (Iterator it = this.mGridGateways.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Entry) it.next();
            GridGateway g = (GridGateway) entry.getValue();
            if (g.getContact().equals(contact)) {
                it.remove();
                return true;
            }
        }
        return false;
    }

    /**
     * Return an iterator to the replica catalog associated with the site.
     *
     * @return Iterator<ReplicaCatalog>
     */
    public Iterator<ReplicaCatalog> getReplicaCatalogIterator() {
        return mReplicaCatalogs.iterator();
    }

    /**
     * Add a Replica Catalog to the site.
     *
     * @param catalog the replica catalog to be added.
     */
    public void addReplicaCatalog(ReplicaCatalog catalog) {
        mReplicaCatalogs.add(catalog);
    }

    /**
     * Selects a Random ReplicaCatalog.
     *
     * @return <code>ReplicaCatalog</object> if more than one associates else
     *         returns null.
     */
    public ReplicaCatalog selectReplicaCatalog() {

        return (this.mReplicaCatalogs == null || this.mReplicaCatalogs.size() == 0)
                ? null
                : this.mReplicaCatalogs.get(PegRandom.getInteger(this.mReplicaCatalogs.size() - 1));
    }

    /**
     * Writes out the xml description of the object.
     *
     * @param writer is a Writer opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent the indent to be used.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer writer, String indent) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");
        String newIndent = indent + "\t";

        // write out the  xml element
        writer.write(indent);
        writer.write("<site ");
        writeAttribute(writer, "handle", getSiteHandle());
        writeAttribute(writer, "arch", getArchitecture().toString());
        writeAttribute(writer, "os", getOS().toString());

        String val = null;
        if ((val = this.getOSRelease()) != null) {
            writeAttribute(writer, "osrelease", val);
        }

        if ((val = this.getOSVersion()) != null) {
            writeAttribute(writer, "osversion", val);
        }

        if ((val = this.getGlibc()) != null) {
            writeAttribute(writer, "glibc", val);
        }

        writer.write(">");
        writer.write(newLine);

        // list all the gridgateways
        for (Iterator<GridGateway> it = this.getGridGatewayIterator(); it.hasNext(); ) {
            it.next().toXML(writer, newIndent);
        }

        HeadNodeFS fs = null;
        if ((fs = this.getHeadNodeFS()) != null) {
            fs.toXML(writer, newIndent);
        }

        WorkerNodeFS wfs = null;
        if ((wfs = this.getWorkerNodeFS()) != null) {
            wfs.toXML(writer, newIndent);
        }

        // list all the replica catalogs associate
        for (Iterator<ReplicaCatalog> it = this.getReplicaCatalogIterator(); it.hasNext(); ) {
            it.next().toXML(writer, newIndent);
        }

        this.getProfiles().toXML(writer, newIndent);

        writer.write(indent);
        writer.write("</site>");
        writer.write(newLine);
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        SiteCatalogEntry3 obj;
        try {
            obj = (SiteCatalogEntry3) super.clone();
            obj.initialize(this.getSiteHandle());
            obj.setSysInfo((SysInfo) this.getSysInfo().clone());

            // list all the gridgateways
            for (Iterator<GridGateway> it = this.getGridGatewayIterator(); it.hasNext(); ) {
                obj.addGridGateway((GridGateway) it.next().clone());
            }

            HeadNodeFS fs = null;
            if ((fs = this.getHeadNodeFS()) != null) {
                obj.setHeadNodeFS((HeadNodeFS) fs.clone());
            }

            WorkerNodeFS wfs = null;
            if ((wfs = this.getWorkerNodeFS()) != null) {
                obj.setWorkerNodeFS((WorkerNodeFS) wfs.clone());
            }

            // list all the replica catalogs associate
            for (Iterator<ReplicaCatalog> it = this.getReplicaCatalogIterator(); it.hasNext(); ) {
                obj.addReplicaCatalog((ReplicaCatalog) it.next().clone());
            }

            obj.setProfiles((Profiles) this.mProfiles.clone());

        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return obj;
    }

    /**
     * Accept method for the visitor interface
     *
     * @param visitor the visitor
     * @throws IOException in case of error
     */
    public void accept(SiteDataVisitor visitor) throws IOException {

        throw new java.lang.UnsupportedOperationException(
                "Accept on the Visitor interface not implemented ");
        /*
         visitor.visit( this );

        //list all the gridgateways
         for( Iterator<GridGateway> it = this.getGridGatewayIterator(); it.hasNext(); ){
             it.next().accept(visitor);
         }

         HeadNodeFS fs = null;
         if( (fs = this.getHeadNodeFS()) != null ){
             fs.accept( visitor );
         }


         WorkerNodeFS wfs = null;
         if( ( wfs = this.getWorkerNodeFS() ) != null ){
             wfs.accept(visitor);
         }

         //list all the replica catalogs associate
         for( Iterator<ReplicaCatalog> it = this.getReplicaCatalogIterator(); it.hasNext(); ){
             it.next().accept(visitor);
         }

         //profiles are handled in the depart method
         visitor.depart(this);
          */
    }
}
