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

import edu.isi.pegasus.planner.catalog.classes.Architecture;
import edu.isi.pegasus.planner.catalog.classes.OS;

import java.util.HashMap;
import java.util.Map;

/**
 * An Adapter class that translates the new NMI based Architecture and OS
 * specifications to VDS ( VDS era ) Arch and Os objects
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class NMI2SysInfo {

    /**
     * The map storing architecture to corresponding NMI architecture platforms.
     */
    private static Map< Architecture,Arch > mNMIArchToVDSArchMap = null;

    /**
     * Singleton access to the  NMI arch to VDS arch map.
     *
     * @return Map mapping NMI Architecture to VDS Arch object.
     */
    public static Map<Architecture, Arch> NMIArchToVDSArchMap(){
        //singleton access
        if( mNMIArchToVDSArchMap == null ){
            mNMIArchToVDSArchMap = new HashMap< Architecture,Arch >();
            mNMIArchToVDSArchMap.put( Architecture.x86, Arch.INTEL32  );
            mNMIArchToVDSArchMap.put( Architecture.x86_64, Arch.INTEL64  );
            mNMIArchToVDSArchMap.put( Architecture.amd64, Arch.AMD64  );
            //mNMIArchToVDSArch.put( Architecture.x86_64, Arch.AMD64 );
            
            //VDS arch INTEL64 actually meant IA64
            mNMIArchToVDSArchMap.put(Architecture.ia64, Arch.INTEL64 );

            mNMIArchToVDSArchMap.put(Architecture.sparcv7, Arch.SPARCV7 );
            mNMIArchToVDSArchMap.put(Architecture.sparcv9, Arch.SPARCV9 );
        }
        return mNMIArchToVDSArchMap;
    }


    /**
     * The map storing OS to corresponding NMI OS platforms.
     */
    private static Map<OS,Os> mNMIOSToVDSOSMap = null;

    /**
     * Singleton access to the os to NMI os map.
     *
     *
     * @return Map mapping NMI OS to VDS Os object.
     */
    public static Map<OS,Os> NMIOSToVDSOSMap(){
        //singleton access
        if( mNMIOSToVDSOSMap == null ){
            mNMIOSToVDSOSMap = new HashMap<OS,Os>();
            //mNMIOSToVDSOS.put( "rhas_3", Os.LINUX );
            mNMIOSToVDSOSMap.put( OS.LINUX, Os.LINUX );
            mNMIOSToVDSOSMap.put( OS.WINDOWS, Os.WINDOWS );
            mNMIOSToVDSOSMap.put( OS.AIX, Os.AIX );
            mNMIOSToVDSOSMap.put( OS.SUNOS, Os.SUNOS );
        }
        return mNMIOSToVDSOSMap;
    }

    /**
     * Returns the VDS SysInfo object corresponding to the NMI arch and OS
     *
     * @param arch  architecture in the new NMI format
     * @param os    the os in NMI format
     * @param glibc the glibc version
     *
     * @return the SysInfo object
     */
    public static SysInfo nmiToSysInfo( Architecture arch, OS os, String glibc ){
        SysInfo result = new SysInfo();
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
    public static Arch nmiArchToVDSArch( Architecture arch ){
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
        return NMIArchToVDSArchMap().get( Architecture.valueOf(arch)  );
    }

    /**
     * Returns the VDS Os object corresponding to the new
     * NMI OS object .
     *
     * @param os  the os in the new NMI format.
     *
     * @return the VDS description of OS
     */
    public static Os nmiOSToVDSOS( OS os ){
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
        return NMIOSToVDSOSMap().get( OS.valueOf(os) );
    }



}
