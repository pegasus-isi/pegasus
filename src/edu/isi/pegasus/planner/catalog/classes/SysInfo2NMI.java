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

package edu.isi.pegasus.planner.catalog.classes;

import edu.isi.pegasus.planner.catalog.transformation.classes.Arch;
import edu.isi.pegasus.planner.catalog.transformation.classes.Os;

import java.util.HashMap;
import java.util.Map;

/**
 * An Adapter class that translates the old ( VDS era ) Arch and Os objects
 * to the new NMI based Architecture and OS objects.
 *
 * @author Karan Vahi
 * @version  $ID$
 */
public class SysInfo2NMI {

     /**
     * The map storing architecture to corresponding NMI architecture platforms.
     */
    private static Map mVDSArchToNMIArch = null;

    /**
     * Singleton access to the architecture to NMI arch map.
     *
     * @return Map mapping VDS Arch to NMI architecture
     */
    private static Map<Arch,Architecture> vdsArchToNMIArchMap(){
        //singleton access
        if( mVDSArchToNMIArch == null ){
            mVDSArchToNMIArch = new HashMap();
            mVDSArchToNMIArch.put( Arch.INTEL32, Architecture.x86 );
            mVDSArchToNMIArch.put( Arch.INTEL64, Architecture.x86_64 );
            mVDSArchToNMIArch.put( Arch.AMD64, Architecture.x86_64 );
            mVDSArchToNMIArch.put( Arch.SPARCV7, Architecture.sparcv7 );
            mVDSArchToNMIArch.put( Arch.SPARCV9, Architecture.sparcv9 );
        }
        return mVDSArchToNMIArch;
    }



    /**
     * The map storing OS to corresponding NMI OS platforms.
     */
    private static Map<Os,OS> mVDSOSToNMIOS = null;

    /**
     * Singleton access to the os to NMI os map.
     *
     * @return Map mapping VDS Os to NMI OS
     */
    private static Map<Os,OS> vdsOsToNMIOSMap(){
        //singleton access
        if( mVDSOSToNMIOS == null ){
            mVDSOSToNMIOS = new HashMap();
            mVDSOSToNMIOS.put( Os.LINUX, OS.LINUX );
            mVDSOSToNMIOS.put( Os.AIX, OS.AIX );
            mVDSOSToNMIOS.put( Os.SUNOS, OS.SUNOS );
            mVDSOSToNMIOS.put( Os.WINDOWS, OS.WINDOWS );
        }
        return mVDSOSToNMIOS;
    }

    /**
     * Returns the NMI Architecture object corresponding to the VDS Arch
     * object
     *
     * @param arch  architecture in the VDS format.
     *
     * @return NMI Architecture
     */
    public static Architecture vdsArchToNMIArch( Arch arch ){
        return vdsArchToNMIArchMap().get( arch );
    }

    /**
     * Returns the NMI Architecture object corresponding to the VDS Arch
     * object
     *
     * @param arch  architecture in the VDS format.
     *
     * @return NMI Architecture
     */
    public static Architecture vdsArchToNMIArch( String arch ){
        return vdsArchToNMIArchMap().get( Arch.fromString(arch) );
    }

    /**
     * Returns the NMI OS object corresponding to the VDS Os
     * object
     *
     * @param os  os in the VDS format.
     *
     * @return NMI OS
     */
    public static OS vdsOsToNMIOS( Os os ){
        return vdsOsToNMIOSMap().get( os );
    }


    /**
     * Returns the NMI OS object corresponding to the VDS Os
     * object
     *
     * @param os  os in the VDS format.
     *
     * @return NMI OS
     */
    public static OS vdsOsToNMIOS( String os ){
        return vdsOsToNMIOSMap().get( Os.fromValue(os) );
    }
    
}
