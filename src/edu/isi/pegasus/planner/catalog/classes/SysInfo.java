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
package edu.isi.pegasus.planner.catalog.classes;

/**
 * A container class to keep system information associated with a Site entry in the Site Catalog or
 * a Transformation in the Transformation Catalog.
 *
 * <p>The class follows the NMI conventions for specifying Architecture/ OS and OS release.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SysInfo implements Cloneable {

    /** Enumerates the new OS types supported in Pegasus. */
    public enum OS {
        linux,
        sunos,
        aix,
        macosx,
        windows
    }

    /** Enumerates the new OS Release supported in Pegasus. */
    public enum OS_RELEASE {
        rhel,
        deb,
        ubuntu,
        fedora,
        fc,
        suse,
        sles,
        freebsd,
        macos,
        sunos
    }

    /** Enumerates the new architecture types supported in Pegasus. */
    public enum Architecture {
        x86,
        x86_64,
        ppc,
        ppc_64,
        ia64,
        sparcv7,
        sparcv9,
        amd64,
        ppc64le
    }

    /**
     * Computes OS from a release
     *
     * @param release
     * @return
     */
    public static OS computeOS(OS_RELEASE release) {
        OS os = OS.linux;
        switch (release) {
            case rhel:
            case deb:
            case ubuntu:
            case fc:
            case suse:
            case sles:
            case freebsd:
                os = OS.linux;
                break;
            case macos:
                os = OS.macosx;
                break;
            case sunos:
                os = OS.sunos;
                break;
            default:
                throw new RuntimeException("Unable to compute OS from release " + release);
        }
        return os;
    }

    /** The default OS the entry is associated with if none is specified */
    public static final OS DEFAULT_OS = OS.linux;

    /** The default OS the entry is associated with if none is specified */
    public static final OS_RELEASE DEFAULT_OS_RELEASE = null;

    /** The default Architecture the entry is associated with if none is specified */
    public static final Architecture DEFAULT_ARCHITECTURE = Architecture.x86_64;

    /** The architecture. */
    protected Architecture mArchitecture;

    /** The Operating System. */
    protected OS mOS;

    /** The Operating System Release. Optional. */
    protected OS_RELEASE mOSRelease;

    /** The Operating System Version. Optional. */
    protected String mOSVersion;

    /** The Glibc version. Optional. */
    protected String mGlibc;

    /** The default constructor. */
    public SysInfo() {
        mArchitecture = SysInfo.DEFAULT_ARCHITECTURE;
        mOS = SysInfo.DEFAULT_OS;
        mOSRelease = SysInfo.DEFAULT_OS_RELEASE;
        mOSVersion = "";
        mGlibc = "";
    }
    /**
     * This constructor takes the system information in the format arch::os:osversion:glibc
     *
     * @param system the system information string
     */
    public SysInfo(String system) {
        if (system != null) {
            String s1[] = system.split("::", 2);
            if (s1.length == 2) {
                if (isValidArchitecture(s1[0].trim())) {
                    mArchitecture = Architecture.valueOf(s1[0].trim());
                } else {
                    throw new IllegalStateException(
                            "Error: Illegal Architecture defined. Please specify one of the predefined types \n [x86, x86_64, ppc, ppc_64, ia64,  sparcv7, sparcv9, amd64]");
                }
                String s2[] = s1[1].split(":", 3);
                if (isValidOS(s2[0].trim())) {
                    mOS = OS.valueOf(s2[0].trim());
                } else {
                    throw new IllegalStateException(
                            "Error: Illegal Operating System defined. Please specify one of the predefined types \n [LINUX, SUNOS, AIX, MACOSX, WINDOWS]");
                }
                for (int i = 1; i < s2.length; i++) {
                    if (i == 1) {
                        mOSVersion = s2[i];
                    }
                    if (i == 2) {
                        mGlibc = s2[i];
                    }
                }
            } else {
                throw new IllegalStateException("Error : Please check your system info string");
            }
        } else {
            mArchitecture = SysInfo.DEFAULT_ARCHITECTURE;
            mOS = SysInfo.DEFAULT_OS;
            mOSRelease = SysInfo.DEFAULT_OS_RELEASE;
            mOSVersion = "";
            mGlibc = "";
        }
    }
    /**
     * Checks if the architecture is a valid supported architecture
     *
     * @param arch architecture
     * @return true if it is a valid supported architecture, false otherwise
     */
    private static boolean isValidArchitecture(String arch) {
        for (Architecture architecture : Architecture.values()) {
            if (architecture.toString().equals(arch)) return true;
        }
        return false;
    }
    /**
     * Checks if the operating system is a valid supported operating system
     *
     * @param os operating system
     * @return true if it is a valid supported operating system, false otherwise
     */
    private static boolean isValidOS(String os) {
        for (OS osystem : OS.values()) {
            if (osystem.toString().equals(os)) return true;
        }
        return false;
    }

    /**
     * Sets the architecture of the site.
     *
     * @param arch the architecture.
     */
    public void setArchitecture(Architecture arch) {
        mArchitecture = arch;
    }

    /**
     * Returns the architecture of the site.
     *
     * @return the architecture.
     */
    public Architecture getArchitecture() {
        return mArchitecture;
    }

    /**
     * Sets the OS of the site.
     *
     * @param os the os of the site.
     */
    public void setOS(OS os) {
        mOS = os;
    }

    /**
     * Returns the OS of the site.
     *
     * @return the OS
     */
    public OS getOS() {
        return mOS;
    }

    /**
     * Sets the OS release of the site.
     *
     * @param release the os releaseof the site.
     */
    public void setOSRelease(String release) {
        if (release.length() == 0) {
            // set the default release to rhel
            mOSRelease = SysInfo.DEFAULT_OS_RELEASE;
            return;
        }
        mOSRelease = OS_RELEASE.valueOf(release);
    }

    /**
     * Returns the OS release of the site.
     *
     * @return the OS
     */
    public String getOSRelease() {
        return (mOSRelease == null) ? "" : mOSRelease.toString();
    }

    /**
     * Sets the OS version of the site.
     *
     * @param version the os versionof the site.
     */
    public void setOSVersion(String version) {
        mOSVersion = version;
    }

    /**
     * Returns the OS version of the site.
     *
     * @return the OS
     */
    public String getOSVersion() {
        return mOSVersion;
    }

    /**
     * Sets the glibc version on the site.
     *
     * @param version the glibc version of the site.
     */
    public void setGlibc(String version) {
        mGlibc = version;
    }

    /**
     * Returns the glibc version of the site.
     *
     * @return the OS
     */
    public String getGlibc() {
        return mGlibc;
    }

    /**
     * Check if the system information matches.
     *
     * @param obj to be compared.
     * @return boolean
     */
    public boolean equals(Object obj) {
        boolean result = false;
        if (obj instanceof SysInfo) {
            SysInfo sysinfo = (SysInfo) obj;

            result =
                    this.getArchitecture().equals(sysinfo.getArchitecture())
                            && this.getOS().equals(sysinfo.getOS())
                            && this.getOSRelease().equals(sysinfo.getOSRelease())
                            && this.getOSVersion().equals(sysinfo.getOSVersion())
                            && this.getGlibc().equals(sysinfo.getGlibc());
        }
        return result;
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        SysInfo obj = null;
        try {
            obj = (SysInfo) super.clone();
            obj.setArchitecture(this.getArchitecture());
            obj.setOS(this.getOS());

            obj.setOSRelease(this.getOSRelease());
            obj.setOSVersion(this.getOSVersion());
            obj.setGlibc(this.getGlibc());
        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return obj;
    }

    /**
     * Returns the output of the data class as string.
     *
     * @return String
     */
    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append("{");
        s.append("arch=" + this.getArchitecture());
        s.append(" os=" + this.getOS());

        String release = this.getOSRelease();
        if (release != null && release.length() > 0) {
            s.append(" osrelease=" + release);
        }

        String version = this.getOSVersion();
        if (version != null && version.length() > 0) {
            s.append(" osversion=" + version);
        }
        s.append("}");
        return s.toString();
    }
}
