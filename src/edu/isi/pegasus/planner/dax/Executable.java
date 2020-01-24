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
package edu.isi.pegasus.planner.dax;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.XMLWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * The Transformation Catalog object the represent the entries in the DAX transformation section.
 *
 * @author gmehta
 * @version $Revision$
 */
public class Executable extends CatalogType {

    /** ARCH Types */
    public static enum ARCH {
        X86,
        x86,
        X86_64,
        x86_64,
        PPC,
        ppc,
        PPC_64,
        ppc_64,
        IA64,
        ia64,
        SPARCV7,
        sparcv7,
        SPARCV9,
        sparcv9,
        ppc64le
    }

    /** OS Types */
    public static enum OS {
        LINUX,
        linux,
        SUNOS,
        sunos,
        AIX,
        aix,
        MACOSX,
        macosx,
        WINDOWS,
        windows
    }
    /** Namespace of the executable */
    protected String mNamespace;
    /** Name of the executable */
    protected String mName;
    /** Version of the executable */
    protected String mVersion;
    /** Architecture the executable is compiled for */
    protected ARCH mArch;
    /** Os the executable is compiled for */
    protected OS mOs;
    /** Os release the executable is compiled for */
    protected String mOsRelease;
    /** OS version the executable is compiled for */
    protected String mOsVersion;
    /** Glibc the executable is compiled for */
    protected String mGlibc;
    /** Flag to mark if the executable is installed or can be staged. */
    protected boolean mInstalled = true;

    /** List of Notification objects */
    protected List<Invoke> mInvokes;

    /**
     * Create a new executable
     *
     * @param name
     */
    public Executable(String name) {
        this("", name, "");
    }

    /**
     * Copy Constructor
     *
     * @param e
     */
    public Executable(Executable e) {
        super(e);
        this.mNamespace = e.mNamespace;
        this.mName = e.mName;
        this.mVersion = e.mVersion;
        this.mArch = e.mArch;
        this.mOs = e.mOs;
        this.mOsRelease = e.mOsRelease;
        this.mOsVersion = e.mOsVersion;
        this.mGlibc = e.mGlibc;
        this.mInstalled = e.mInstalled;
        this.mInvokes = new LinkedList<Invoke>(e.mInvokes);
    }

    /**
     * Create a new Executable
     *
     * @param namespace
     * @param name
     * @param version
     */
    public Executable(String namespace, String name, String version) {
        super();
        mNamespace = (namespace == null) ? "" : namespace;
        mName = (name == null) ? "" : name;
        mVersion = (version == null) ? "" : version;
        mInvokes = new LinkedList<Invoke>();
    }

    /**
     * Get the name of the executable
     *
     * @return
     */
    public String getName() {
        return mName;
    }

    /**
     * Get the namespace of the executable
     *
     * @return
     */
    public String getNamespace() {
        return mNamespace;
    }

    /**
     * Get the version of the executable
     *
     * @return
     */
    public String getVersion() {
        return mVersion;
    }

    /**
     * Return the list of Notification objects
     *
     * @return List<Invoke>
     */
    public List<Invoke> getInvoke() {
        return mInvokes;
    }

    /**
     * Return the list of Notification objects (same as getInvoke)
     *
     * @return List<Invoke>
     */
    public List<Invoke> getNotification() {
        return getInvoke();
    }

    /**
     * Add a Notification for this Executable same as addNotification
     *
     * @param when
     * @param what
     * @return Executable
     */
    public Executable addInvoke(Invoke.WHEN when, String what) {
        Invoke i = new Invoke(when, what);
        mInvokes.add(i);
        return this;
    }

    /**
     * Add a Notification for this Executable same as addInvoke
     *
     * @param when
     * @param what
     * @return Executable
     */
    public Executable addNotification(Invoke.WHEN when, String what) {
        return addInvoke(when, what);
    }

    /**
     * Add a Notification for this Executable Same as add Notification
     *
     * @param invoke
     * @return Executable
     */
    public Executable addInvoke(Invoke invoke) {
        mInvokes.add(invoke.clone());
        return this;
    }

    /**
     * Add a Notification for this Executable Same as addInvoke
     *
     * @param invoke
     * @return Executable
     */
    public Executable addNotification(Invoke invoke) {
        return addInvoke(invoke);
    }

    /**
     * Add a List of Notifications for this Executable Same as addNotifications
     *
     * @param invokes
     * @return Executable
     */
    public Executable addInvokes(List<Invoke> invokes) {
        for (Invoke invoke : invokes) {
            this.addInvoke(invoke);
        }
        return this;
    }

    /**
     * Add a List of Notifications for this Executable. Same as addInvokes
     *
     * @param invokes
     * @return Executable
     */
    public Executable addNotifications(List<Invoke> invokes) {
        return addInvokes(invokes);
    }

    /**
     * Set the architecture the executable is compiled for
     *
     * @param arch
     * @return Executable
     */
    public Executable setArchitecture(ARCH arch) {
        mArch = arch;
        return this;
    }

    /**
     * Set the OS the executable is compiled for
     *
     * @param os
     * @return
     */
    public Executable setOS(OS os) {
        mOs = os;
        return this;
    }

    /**
     * Set the osrelease the executable is compiled for
     *
     * @param osrelease
     * @return
     */
    public Executable setOSRelease(String osrelease) {
        mOsRelease = osrelease;
        return this;
    }

    /**
     * Set the osversion the executable is compiled for
     *
     * @param osversion
     * @return
     */
    public Executable setOSVersion(String osversion) {
        mOsVersion = osversion;
        return this;
    }

    /**
     * Set the glibc this executable is compiled for
     *
     * @param glibc
     * @return
     */
    public Executable setGlibc(String glibc) {
        mGlibc = glibc;
        return this;
    }

    /**
     * set the installed flag on the executable. Default is installed
     *
     * @return
     */
    public Executable setInstalled() {
        mInstalled = true;
        return this;
    }

    /**
     * Unset the installed flag on the executable. Default is installed.
     *
     * @return
     */
    public Executable unsetInstalled() {
        mInstalled = false;
        return this;
    }

    /** Set the installed flag on the executable. Default is installed */
    public Executable setInstalled(boolean installed) {
        mInstalled = installed;
        return this;
    }

    /**
     * Check if the executable is of type installed.
     *
     * @return
     */
    public boolean getInstalled() {
        return mInstalled;
    }

    /**
     * Get the architecture the Executable is compiled for
     *
     * @return
     */
    public ARCH getArchitecture() {
        return mArch;
    }

    /**
     * Get the OS the Executable is compiled for
     *
     * @return
     */
    public OS getOS() {
        return mOs;
    }

    /**
     * Get the OS release set for this executable. Returns empty string if not set
     *
     * @return
     */
    public String getOsRelease() {
        return (mOsRelease == null) ? "" : mOsRelease;
    }

    /**
     * Get the OS version set for this executable.
     *
     * @return
     */
    public String getOsVersion() {
        return (mOsVersion == null) ? "" : mOsVersion;
    }

    /**
     * Get the Glibc version if any set for this file. Returns empty string if not set
     *
     * @return
     */
    public String getGlibc() {
        return (mGlibc == null) ? "" : mGlibc;
    }

    public boolean isExecutable() {
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Executable other = (Executable) obj;
        if ((this.mNamespace == null)
                ? (other.mNamespace != null)
                : !this.mNamespace.equals(other.mNamespace)) {
            return false;
        }
        if ((this.mName == null) ? (other.mName != null) : !this.mName.equals(other.mName)) {
            return false;
        }
        if ((this.mVersion == null)
                ? (other.mVersion != null)
                : !this.mVersion.equals(other.mVersion)) {
            return false;
        }
        if (this.mArch != other.mArch) {
            return false;
        }
        if (this.mOs != other.mOs) {
            return false;
        }
        if ((this.mOsRelease == null)
                ? (other.mOsRelease != null)
                : !this.mOsRelease.equals(other.mOsRelease)) {
            return false;
        }
        if ((this.mOsVersion == null)
                ? (other.mOsVersion != null)
                : !this.mOsVersion.equals(other.mOsVersion)) {
            return false;
        }
        if ((this.mGlibc == null) ? (other.mGlibc != null) : !this.mGlibc.equals(other.mGlibc)) {
            return false;
        }
        if (this.mInstalled != other.mInstalled) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + (this.mNamespace != null ? this.mNamespace.hashCode() : 0);
        hash = 53 * hash + (this.mName != null ? this.mName.hashCode() : 0);
        hash = 53 * hash + (this.mVersion != null ? this.mVersion.hashCode() : 0);
        hash = 53 * hash + (this.mArch != null ? this.mArch.hashCode() : 0);
        hash = 53 * hash + (this.mOs != null ? this.mOs.hashCode() : 0);
        hash = 53 * hash + (this.mOsRelease != null ? this.mOsRelease.hashCode() : 0);
        hash = 53 * hash + (this.mOsVersion != null ? this.mOsVersion.hashCode() : 0);
        hash = 53 * hash + (this.mGlibc != null ? this.mGlibc.hashCode() : 0);
        hash = 53 * hash + (this.mInstalled ? 1 : 0);
        return hash;
    }

    @Override
    public String toString() {
        return mNamespace + "::" + mName + ":" + mVersion;
    }

    @Override
    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    @Override
    public void toXML(XMLWriter writer, int indent) {
        if (mProfiles.isEmpty() && mPFNs.isEmpty() && mMetadata.isEmpty()) {
            mLogger.log(
                    "The executable element for "
                            + mName
                            + " must have atleast 1 profile, 1 pfn or 1 metadata entry. Skipping empty executable element",
                    LogManager.WARNING_MESSAGE_LEVEL);
        } else {
            writer.startElement("executable", indent);
            if (mNamespace != null && !mNamespace.isEmpty()) {
                writer.writeAttribute("namespace", mNamespace);
            }
            writer.writeAttribute("name", mName);
            if (mVersion != null && !mVersion.isEmpty()) {
                writer.writeAttribute("version", mVersion);
            }
            if (mInstalled) {
                writer.writeAttribute("installed", "true");
            } else {
                writer.writeAttribute("installed", "false");
            }
            if (mArch != null) {
                writer.writeAttribute("arch", mArch.toString().toLowerCase());
            }
            if (mOs != null) {
                writer.writeAttribute("os", mOs.toString().toLowerCase());
            }
            if (mOsRelease != null && !mOsRelease.isEmpty()) {
                writer.writeAttribute("osrelease", mOsRelease);
            }
            if (mOsVersion != null && !mOsVersion.isEmpty()) {
                writer.writeAttribute("osversion", mOsVersion);
            }
            if (mGlibc != null && !mGlibc.isEmpty()) {
                writer.writeAttribute("glibc", mGlibc);
            }
            super.toXML(writer, indent);
            for (Invoke i : mInvokes) {
                i.toXML(writer, indent + 1);
            }
            writer.endElement(indent);
        }
    }
}
