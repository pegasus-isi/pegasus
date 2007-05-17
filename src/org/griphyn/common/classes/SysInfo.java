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
package org.griphyn.common.classes;

/**
 * This class keeps the system information associated with a
 * resource or transformation.
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */

public class SysInfo{

    /**
     * Architecture of the system.
     */
    private Arch arch;

    /**
     * Os of the system.
     */
    private Os os;

    /**
     * Os version of the system.
     */
    private String osversion;

    /**
     * Glibc version of the system
     */
    private String glibc;

    /**
     * The secondary convenience constructor.
     * @param arch Arch  The architecture of the system.
     * @param os Os The os of the system.
     * @param osversion String  The os version of the system.
     * @param glibc String The glibc version of the system.
     * @see Arch
     * @see Os
     */
    public SysInfo(Arch arch, Os os, String osversion, String glibc) {
        this.arch = (arch == null) ? Arch.INTEL32 : arch;
        this.os = (os == null) ? Os.LINUX : os;

        this.osversion = (osversion == null || osversion.equals("") ) ?
                          null:
                          osversion;

        this.glibc = (glibc == null || glibc.equals(""))?
                      null:
                      glibc;
    }

    /**
     * Another convenience constructor that uses all entries as strings.
     * @param arch String
     * @param os String
     * @param osversion String
     * @param glibc String
     */
    public SysInfo(String arch, String os, String osversion, String glibc) {
        this.arch = (arch == null) ? Arch.INTEL32 : Arch.fromString(arch);
        this.os = (os == null) ? Os.LINUX : Os.fromString(os);
        this.osversion = (osversion == null || osversion.equals("") ) ?
                          null:
                          osversion;

        this.glibc = (glibc == null || glibc.equals(""))?
                      null:
                      glibc;
    }

    public SysInfo(String system) {
        if (system != null) {
            String s1[] = system.split("::", 2);
            if (s1.length == 2) {
                arch = Arch.fromString(s1[0]);
                String s2[] = s1[1].split(":", 3);
                os = Os.fromString(s2[0]);
                for (int i = 1; i < s2.length; i++) {
                    if (i == 1) {
                        osversion = s2[i];
                    }
                    if (i == 2) {
                        glibc = s2[i];
                    }
                }
            } else {
                throw new IllegalStateException(
                    "Error : Please check your system info string");
            }
        } else {
            this.arch = Arch.INTEL32;
            this.os = Os.LINUX;
        }
    }

    /**
     * The default constructor.
     * Sets the sysinfo to INTEL32::LINUX
     */
    public SysInfo() {
        this.arch=Arch.INTEL32;
        this.os=Os.LINUX;
    }

    /**
     * Sets the architecture of the system.
     * @param arch Arch
     * @see Arch
     */
    public void setArch(Arch arch) {
        this.arch = (arch == null) ? Arch.INTEL32 : arch;
    }

    /**
     * Sets the Os of the sytem.
     * @param os Os
     * @see Os
     */
    public void setOs(Os os) {
        this.os = (os == null) ? Os.LINUX : os;
    }

    /**
     * Sets the Os version of the system.
     * @param osversion String
     */
    public void setOsversion(String osversion) {
        this.osversion = osversion;
    }

    /**
     * Sets the glibc version of the system
     * @param glibc String
     */
    public void setGlibc(String glibc) {
        this.glibc = glibc;
    }

    /**
     * Returns the architecture of the sytem.
     * @return Arch
     * @see Arch
     */
    public Arch getArch() {
        return arch;
    }

    /**
     * Returns the os type of the system.
     * @return Os
     * @see Os
     */
    public Os getOs() {
        return os;
    }

    /**
     * Returns the os version of the system.
     * @return String
     */
    public String getOsversion() {
        return osversion;
    }

    /**
     * Retuns the glibc version of the system.
     * @return String
     */
    public String getGlibc() {
        return glibc;
    }

    /**
     * Return a copy of this Sysinfo object
     * @return Object
     */
    public Object clone() {
        return new SysInfo(arch, os, osversion, glibc);
    }

    /**
     * Check if the system information matches.
     * @param obj to be compared.
     * @return boolean
     */
    public boolean equals(Object obj) {
        boolean result = false;
        if(obj instanceof SysInfo){
            SysInfo sysinfo = (SysInfo)obj;
            result = (arch.equals(sysinfo.getArch())
                      && os.equals(sysinfo.getOs()));
        }
        return result;
    }
    /**
     * Returns the output of the data class as string.
     * @return String
     */
    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append(arch + "::" + os);
        if (osversion != null) {
            s.append(":" + osversion);
        }
        if (glibc != null) {
            s.append(":" + glibc);
        }
        return s.toString();
    }
}
