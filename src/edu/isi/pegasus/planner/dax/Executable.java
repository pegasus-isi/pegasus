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

import edu.isi.pegasus.common.util.XMLWriter;

/**
 *
 * @author gmehta
 */
public class Executable extends CatalogType {

    public static enum ARCH {

        x86, x86_64, ppc, ppc_64, ia64, sparcv7, sparcv9
    }

    public static enum OS {

        LINUX, SUNOS, AIX, MACOS, DARWIN, WINDOWS, SOLARIS
    }
    protected String mNamespace;
    protected String mName;
    protected String mVersion;
    protected ARCH mArch;
    protected OS mOs;
    protected String mOsRelease;
    protected String mOsVersion;
    protected String mGlibc;

    public Executable(String name) {
        this("", name, "");
    }

    public Executable(String namespace, String name, String version) {
        mNamespace = (namespace == null) ? "" : namespace;
        mName = (name == null) ? "" : name;
        mVersion = (version == null) ? "" : null;

    }

    public String getName() {
        return mName;
    }

    public String getNamespace() {
        return mNamespace;
    }

    public String getVersion() {
        return mVersion;
    }

    public Executable setArchitecture(ARCH arch) {
        mArch = arch;
        return this;
    }

    public Executable setOS(OS os) {
        mOs = os;
        return this;
    }

    public Executable setOSRelease(String osrelease) {
        mOsRelease = osrelease;
        return this;
    }

    public Executable setOSVersion(String osversion) {
        mOsVersion = osversion;
        return this;
    }

    public Executable setGlibc(String glibc) {
        mGlibc = glibc;
        return this;
    }

    public ARCH getARCH() {
        return mArch;
    }

    public OS getOS() {
        return mOs;
    }

    public String getOsRelease() {
        return (mOsRelease == null) ? "" : mOsRelease;
    }

    public String getOsVersion() {
        return (mOsVersion == null) ? "" : mOsVersion;
    }

    public String getGlibc() {
        return (mGlibc == null) ? "" : mGlibc;
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    @Override
    public void toXML(XMLWriter writer, int indent) {
        if (mProfiles.isEmpty() || mPFNs.isEmpty() || mMetadata.isEmpty()) {
            throw new RuntimeException("The file element for file " + mName + " must have atleast 1 profile or 1 pfn or 1 metadata entry");
        }
        writer.startElement("executable", indent);
        if (mNamespace != null && !mNamespace.isEmpty()) {
            writer.writeAttribute("namespace", mNamespace);
        }
        writer.writeAttribute("name", mName);
        if (mVersion != null && !mVersion.isEmpty()) {
            writer.writeAttribute("version", mVersion);
        }
        if (mArch != ARCH.x86) {
            writer.writeAttribute("arch", mArch.toString().toLowerCase());
        }
        if (mOs != OS.LINUX) {
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
