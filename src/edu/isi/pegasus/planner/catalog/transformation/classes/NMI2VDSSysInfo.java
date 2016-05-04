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

package edu.isi.pegasus.planner.catalog.transformation.classes;


import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import java.util.HashMap;
import java.util.Map;

/**
 * An Adapter class that translates the new NMI based Architecture and OS
 * specifications to VDS ( VDS era ) Arch and Os objects
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class NMI2VDSSysInfo {

    /**
     * The map storing architecture to corresponding NMI architecture platforms.
     */
    private static Map< SysInfo.Architecture,Arch > mNMIArchToVDSArchMap = null;


    /**
     * The separator used to combine OS version and release.
     */
    public static final String OS_COMBINE_SEPARATOR = "_";

    /**
     * Singleton access to the  NMI arch to VDS arch map.
     *
     * @return Map mapping NMI Architecture to VDS Arch object.
     */
    public static Map<SysInfo.Architecture, Arch> NMIArchToVDSArchMap(){
        //singleton access
        if( mNMIArchToVDSArchMap == null ){
            mNMIArchToVDSArchMap = new HashMap< SysInfo.Architecture,Arch >();
            mNMIArchToVDSArchMap.put( SysInfo.Architecture.x86, Arch.INTEL32  );
            mNMIArchToVDSArchMap.put( SysInfo.Architecture.x86_64, Arch.INTEL64  );
            mNMIArchToVDSArchMap.put( SysInfo.Architecture.amd64, Arch.AMD64  );
            //mNMIArchToVDSArch.put( Architecture.x86_64, Arch.AMD64 );
            
            //VDS arch INTEL64 actually meant IA64
            mNMIArchToVDSArchMap.put( SysInfo.Architecture.ia64, Arch.INTEL64 );

            mNMIArchToVDSArchMap.put( SysInfo.Architecture.sparcv7, Arch.SPARCV7 );
            mNMIArchToVDSArchMap.put( SysInfo.Architecture.sparcv9, Arch.SPARCV9 );
        }
        return mNMIArchToVDSArchMap;
    }


    /**
     * The map storing OS to corresponding NMI OS platforms.
     */
    private static Map<SysInfo.OS,Os> mNMIOSToVDSOSMap = null;

    /**
     * Singleton access to the os to NMI os map.
     *
     *
     * @return Map mapping NMI OS to VDS Os object.
     */
    public static Map<SysInfo.OS,Os> NMIOSToVDSOSMap(){
        //singleton access
        if( mNMIOSToVDSOSMap == null ){
            mNMIOSToVDSOSMap = new HashMap<SysInfo.OS,Os>();
            //mNMIOSToVDSOS.put( "rhas_3", Os.LINUX );
            mNMIOSToVDSOSMap.put( SysInfo.OS.linux, Os.LINUX );
            mNMIOSToVDSOSMap.put( SysInfo.OS.windows, Os.WINDOWS );
            mNMIOSToVDSOSMap.put( SysInfo.OS.aix, Os.AIX );
            mNMIOSToVDSOSMap.put( SysInfo.OS.sunos, Os.SUNOS );
        }
        return mNMIOSToVDSOSMap;
    }

    /**
     * Returns the VDSSysInfo object.
     *
     * @param sysinfo   the sysinfo object
     *
     * @return VDSSysInfo object
     */
    public static VDSSysInfo nmiToVDSSysInfo(SysInfo sysinfo) {
        VDSSysInfo result = new VDSSysInfo();
        result.setArch( nmiArchToVDSArch( sysinfo.getArchitecture() ) );
        result.setOs( nmiOSToVDSOS( sysinfo.getOS() ) );
        result.setGlibc( sysinfo.getGlibc() );

        //in VDS days os version was release and version.
        StringBuffer osVersion = new StringBuffer();
        String rel = sysinfo.getOSRelease();
        if( rel != null && rel.length() != 0 ){
            osVersion.append( rel );

            String ver = sysinfo.getOSVersion();
            if( ver != null && ver.length() != 0 ){
                //combine version and release
                osVersion.append( NMI2VDSSysInfo.OS_COMBINE_SEPARATOR );
                osVersion.append( ver );
            }
        }

        result.setOsversion( osVersion.toString() );

        return result;
    }



    /**
     * Returns the VDS VDSSysInfo object corresponding to the NMI arch and OS
     *
     * @param arch  architecture in the new NMI format
     * @param os    the os in NMI format
     * @param glibc the glibc version
     *
     * @return the VDSSysInfo object
     */
    public static VDSSysInfo nmiToVDSSysInfo( SysInfo.Architecture arch, SysInfo.OS os, String glibc ){
        VDSSysInfo result = new VDSSysInfo();
        result.setArch( nmiArchToVDSArch(arch) );
        result.setOs( nmiOSToVDSOS( os ) );
        result.setGlibc(glibc);
        return result;
    }

    /**
     * Returns the  the VDS Arch object corresponding to the new
     * NMI Architecture object .
     *
     * @param arch  architecture in the new NMI format.
     *
     * @return Arch
     */
    public static Arch nmiArchToVDSArch( SysInfo.Architecture arch ){
        return NMIArchToVDSArchMap().get( arch );
    }

    /**
     * Returns the VDS Arch object corresponding to the new
     * NMI Architecture object .
     *
     * @param arch  architecture in the new NMI format.
     *
     * @return Arch
     */
    public static Arch nmiArchToVDSArch( String arch ){
        return NMIArchToVDSArchMap().get( SysInfo.Architecture.valueOf(arch)  );
    }

    /**
     * Returns the VDS Os object corresponding to the new
     * NMI OS object .
     *
     * @param os  the os in the new NMI format.
     *
     * @return the VDS description of OS
     */
    public static Os nmiOSToVDSOS( SysInfo.OS os ){
        return NMIOSToVDSOSMap().get( os );
    }


    /**
     * Returns the VDS Os object corresponding to the new
     * NMI OS object .
     *
     * @param os  the os in the new NMI format.
     *
     * @return the VDS description of OS
     */
    public static Os nmiOSToVDSOS( String os ){
        return NMIOSToVDSOSMap().get( SysInfo.OS.valueOf(os) );
    }



}
