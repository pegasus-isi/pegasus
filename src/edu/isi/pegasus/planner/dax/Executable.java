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
package edu.isi.pegasus.planner.dax;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.XMLWriter;

/**
 * The Transformation Catalog object the represent the entries in the DAX transformation section.
 * @author gmehta
 * @version $Revision$
 */
public class Executable extends CatalogType {

    /**
     * ARCH Types
     */
    public static enum ARCH {

        X86, x86, X86_64, x86_64, PPC, ppc, PPC_64, ppc_64, IA64, ia64, SPARCV7, sparcv7, SPARCV9, sparcv9
    }

    /**
     * OS Types
     */
    public static enum OS {

        LINUX, linux, SUNOS, sunos, AIX, aix, MACOSX, macosx, WINDOWS, windows
    }
    /**
     * Namespace of the executable
     */
    protected String mNamespace;
    /**
     * Name of the executable
     */
    protected String mName;
    /**
     * Version of the executable
     */
    protected String mVersion;
    /**
     * Architecture the executable is compiled for
     */
    protected ARCH mArch;
    /**
     * Os the executable is compiled for
     */
    protected OS mOs;
    /**
     * Os release the executable is compiled for
     */
    protected String mOsRelease;
    /**
     * OS version the executable is compiled for
     */
    protected String mOsVersion;
    /**
     * Glibc the executable is compiled for
     */
    protected String mGlibc;
    /**
     * Flag to mark if the executable is installed or can be staged.
     */
    protected boolean mInstalled = true;

    /**
     * Create a new executable
     * @param name
     */
    public Executable(String name) {
        this("", name, "");
    }

    /**
     * Create a new Executable
     * @param namespace
     * @param name
     * @param version
     */
    public Executable(String namespace, String name, String version) {
        mNamespace = (namespace == null) ? "" : namespace;
        mName = (name == null) ? "" : name;
        mVersion = (version == null) ? "" : version;
    }

    /**
     * Get the name of the executable
     * @return
     */
    public String getName() {
        return mName;
    }

    /**
     * Get the namespace of the executable
     * @return
     */
    public String getNamespace() {
        return mNamespace;
    }

    /**
     * Get the version of the executablle
     * @return
     */
    public String getVersion() {
        return mVersion;
    }

    /**
     * Set the architecture the executable is compiled for
     * @param arch
     * @return
     */
    public Executable setArchitecture(ARCH arch) {
        mArch = arch;
        return this;
    }

    /**
     * Set the OS the executable is compiled for
     * @param os
     * @return
     */
    public Executable setOS(OS os) {
        mOs = os;
        return this;
    }

    /**
     * Set the osrelease the executable is compiled for
     * @param osrelease
     * @return
     */
    public Executable setOSRelease(String osrelease) {
        mOsRelease = osrelease;
        return this;
    }

    /**
     * Set the osversion the executable is compiled for
     * @param osversion
     * @return
     */
    public Executable setOSVersion(String osversion) {
        mOsVersion = osversion;
        return this;
    }

    /**
     * Set the glibc this executable is compiled for
     * @param glibc
     * @return
     */
    public Executable setGlibc(String glibc) {
        mGlibc = glibc;
        return this;
    }

    /**
     * set the installed flag on the executable. Default is installed
     * @return
     */
    public Executable setInstalled() {
        mInstalled = true;
        return this;
    }

    /**
     * Unset the installed flag on the executable. Default is installed.
     * @return
     */
    public Executable unsetInstalled() {
        mInstalled = false;
        return this;
    }

    /**
     * Set the installed flag on the executable. Default is installed
     */
    public Executable setInstalled(boolean installed) {
        mInstalled = installed;
        return this;
    }

    /**
     * Check if the executable is of type installed.
     * @return
     */
    public boolean getInstalled() {
        return mInstalled;
    }

    /**
     * Get the architecture the Executable is compiled for
     * @return
     */
    public ARCH getArchitecture() {
        return mArch;
    }

    /**
     * Get the OS the Executable is compiled for
     * @return
     */
    public OS getOS() {
        return mOs;
    }

    /**
     * Get the OS release set for this executable. Returns empty string if not set
     * @return
     */
    public String getOsRelease() {
        return (mOsRelease == null) ? "" : mOsRelease;
    }

    /**
     * Get the OS version set for this executable.
     * @return
     */
    public String getOsVersion() {
        return (mOsVersion == null) ? "" : mOsVersion;
    }

    /**
     * Get the Glibc version if any set for this file. Returns empty string if not set
     * @return
     */
    public String getGlibc() {
        return (mGlibc == null) ? "" : mGlibc;
    }

    @Override
    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    @Override
    public void toXML(XMLWriter writer, int indent) {
        if (mProfiles.isEmpty() && mPFNs.isEmpty() && mMetadata.isEmpty()) {
            mLogger.log("The executable element for " + mName + " must have atleast 1 profile, 1 pfn or 1 metadata entry. Skipping empty executable element", LogManager.WARNING_MESSAGE_LEVEL);
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
            writer.endElement(indent);
        }

    }
}
