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

import edu.isi.pegasus.planner.catalog.transformation.classes.Arch;
import edu.isi.pegasus.planner.catalog.transformation.classes.Os;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import java.util.HashMap;
import java.util.Map;

/**
 * An Adapter class that translates the old ( VDS era ) Arch and Os objects to the new NMI based
 * Architecture and OS objects.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class VDSSysInfo2NMI {

    /** The separator used to combine OS version and release. */
    public static final String OS_COMBINE_SEPARATOR = "_";

    /** The map storing architecture to corresponding NMI architecture platforms. */
    private static Map mVDSArchToNMIArch = null;

    /**
     * Singleton access to the architecture to NMI arch map.
     *
     * @return Map mapping VDS Arch to NMI architecture
     */
    private static Map<Arch, SysInfo.Architecture> vdsArchToNMIArchMap() {
        // singleton access
        if (mVDSArchToNMIArch == null) {
            mVDSArchToNMIArch = new HashMap();
            mVDSArchToNMIArch.put(Arch.INTEL32, SysInfo.Architecture.x86);
            mVDSArchToNMIArch.put(Arch.INTEL64, SysInfo.Architecture.x86_64);
            mVDSArchToNMIArch.put(Arch.AMD64, SysInfo.Architecture.amd64);
            mVDSArchToNMIArch.put(Arch.SPARCV7, SysInfo.Architecture.sparcv7);
            mVDSArchToNMIArch.put(Arch.SPARCV9, SysInfo.Architecture.sparcv9);
        }
        return mVDSArchToNMIArch;
    }

    /** The map storing OS to corresponding NMI OS platforms. */
    private static Map<Os, SysInfo.OS> mVDSOSToNMIOS = null;

    /**
     * Singleton access to the os to NMI os map.
     *
     * @return Map mapping VDS Os to NMI OS
     */
    private static Map<Os, SysInfo.OS> vdsOsToNMIOSMap() {
        // singleton access
        if (mVDSOSToNMIOS == null) {
            mVDSOSToNMIOS = new HashMap();
            mVDSOSToNMIOS.put(Os.LINUX, SysInfo.OS.linux);
            mVDSOSToNMIOS.put(Os.AIX, SysInfo.OS.aix);
            mVDSOSToNMIOS.put(Os.SUNOS, SysInfo.OS.sunos);
            mVDSOSToNMIOS.put(Os.WINDOWS, SysInfo.OS.windows);
        }
        return mVDSOSToNMIOS;
    }

    /**
     * Converts VDS SysInfo to NMI based SysInfo object
     *
     * @param sysinfo VDS based SysInfo object
     * @return NMI SysInfo object.
     */
    public static SysInfo vdsSysInfo2NMI(VDSSysInfo sysinfo) {
        SysInfo result = new SysInfo();
        result.setArchitecture(vdsArchToNMIArch(sysinfo.getArch()));
        result.setOS(vdsOsToNMIOS(sysinfo.getOs()));

        String glibc = sysinfo.getGlibc();
        if (glibc != null) {
            result.setGlibc(glibc);
        }

        // what we call os release and version now was called os version!
        String osVersion = sysinfo.getOsversion();
        if (osVersion != null && osVersion.length() != 0) {

            if (osVersion.contains(OS_COMBINE_SEPARATOR)) {
                // split on _
                int last = osVersion.lastIndexOf(OS_COMBINE_SEPARATOR);
                result.setOSRelease(osVersion.substring(0, last));
                result.setOSVersion(osVersion.substring(last + 1));
            } else {
                result.setOSRelease(osVersion);
            }
        }

        return result;
    }

    /**
     * Returns the NMI Architecture object corresponding to the VDS Arch object
     *
     * @param arch architecture in the VDS format.
     * @return NMI Architecture
     */
    public static SysInfo.Architecture vdsArchToNMIArch(Arch arch) {
        return vdsArchToNMIArchMap().get(arch);
    }

    /**
     * Returns the NMI Architecture object corresponding to the VDS Arch object
     *
     * @param arch architecture in the VDS format.
     * @return NMI Architecture
     */
    public static SysInfo.Architecture vdsArchToNMIArch(String arch) {
        return vdsArchToNMIArchMap().get(Arch.fromString(arch));
    }

    /**
     * Returns the NMI OS object corresponding to the VDS Os object
     *
     * @param os os in the VDS format.
     * @return NMI OS
     */
    public static SysInfo.OS vdsOsToNMIOS(Os os) {
        return vdsOsToNMIOSMap().get(os);
    }

    /**
     * Returns the NMI OS object corresponding to the VDS Os object
     *
     * @param os os in the VDS format.
     * @return NMI OS
     */
    public static SysInfo.OS vdsOsToNMIOS(String os) {
        return vdsOsToNMIOSMap().get(Os.fromValue(os));
    }

    public static void main(String[] args) {
        VDSSysInfo v = new VDSSysInfo();
        v.setArch(Arch.AMD64);
        v.setOs(Os.LINUX);
        v.setOsversion("rhel_4");
        SysInfo s = VDSSysInfo2NMI.vdsSysInfo2NMI(v);
        System.out.println(s.getOSRelease());
        System.out.println(s.getOSVersion());

        v.setOsversion("rhel_");
        s = VDSSysInfo2NMI.vdsSysInfo2NMI(v);
        System.out.println(s.getOSRelease());
        System.out.println(s.getOSVersion());
    }
}
