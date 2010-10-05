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

/**
 * A container class to keep system information associated with a Site entry in
 * the Site Catalog or a Transformation in the Transformation Catalog.
 *
 * The class follows the NMI conventions for specifying Architecture/ OS and OS release.
 *
 *
 * @author Karan Vahi
 * @version  $Revision$
 */
public class SysInfo implements Cloneable {

    /**
     * Enumerates the new OS types supported in Pegasus.
     */
    public enum OS {
        LINUX, SUNOS, AIX, MACOSX, WINDOWS
    }

    /**
     * Enumerates the new architecture types supported in Pegasus.
     */
    public  enum Architecture {
        x86, x86_64, ppc, ppc_64, ia64,  sparcv7, sparcv9, amd64
    }

    
    /**
     * The default OS the entry is associated with if none is specified
     */
    public static final OS DEFAULT_OS = OS.LINUX;

    /**
     * The default Architecture the entry is associated with if none is specified
     */
    public static final Architecture DEFAULT_ARCHITECTURE = Architecture.x86;

    /**
     * The architecture.
     */
    protected Architecture mArchitecture;

    /**
     * The Operating System.
     */
    protected OS mOS;

    /**
     * The Operating System Release. Optional.
     */
    protected String mOSRelease;

    /**
     * The Operating System Version. Optional.
     */
    protected String mOSVersion;

    /**
     * The Glibc version. Optional.
     */
    protected String mGlibc;



    /**
     * The default constructor.
     */
    public SysInfo(){
        mArchitecture = SysInfo.DEFAULT_ARCHITECTURE;
        mOS           = SysInfo.DEFAULT_OS;
        mOSRelease    = "";
        mOSVersion    = "";
        mGlibc        = "";
    }

    /**
     * Sets the architecture of the site.
     *
     * @param arch  the architecture.
     */
    public void setArchitecture( Architecture arch ){
        mArchitecture = arch;
    }


    /**
     * Returns the architecture of the site.
     *
     * @return  the architecture.
     */
    public Architecture getArchitecture( ){
        return mArchitecture;
    }


    /**
     * Sets the OS of the site.
     *
     * @param os the os of the site.
     */
    public void setOS( OS os ){
        mOS = os;
    }


    /**
     * Returns the OS of the site.
     *
     * @return  the OS
     */
    public OS getOS( ){
        return mOS;
    }

    /**
     * Sets the OS release of the site.
     *
     * @param release the os releaseof the site.
     */
    public void setOSRelease( String release ){
        mOSRelease = release;
    }


    /**
     * Returns the OS release of the site.
     *
     * @return  the OS
     */
    public String getOSRelease( ){
        return mOSRelease;
    }

    /**
     * Sets the OS version of the site.
     *
     * @param version  the os versionof the site.
     */
    public void setOSVersion( String version ){
        mOSVersion = version;
    }


    /**
     * Returns the OS version of the site.
     *
     * @return  the OS
     */
    public String getOSVersion( ){
        return mOSVersion;
    }

    /**
     * Sets the glibc version on the site.
     *
     * @param version  the glibc version of the site.
     */
    public void setGlibc( String version ){
        mGlibc = version;
    }


    /**
     * Returns the glibc version of the site.
     *
     * @return  the OS
     */
    public String getGlibc( ){
        return mGlibc;
    }

    /**
     * Check if the system information matches.
     *
     * @param obj to be compared.
     *
     * @return boolean
     */
    public boolean equals(Object obj) {
        boolean result = false;
        if( obj instanceof SysInfo ){
            SysInfo sysinfo = (SysInfo)obj;

            result = this.getArchitecture().equals( sysinfo.getArchitecture() ) &&
                     this.getOS().equals( sysinfo.getOS() ) &&
                     this.getOSRelease().equals( sysinfo.getOSRelease() ) &&
                     this.getOSVersion().equals( sysinfo.getOSVersion() ) &&
                     this.getGlibc().equals( sysinfo.getGlibc() );
        }
        return result;
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone(){
        SysInfo obj = null;
        try{
            obj = ( SysInfo ) super.clone();
            obj.setArchitecture( this.getArchitecture() );
            obj.setOS( this.getOS() );

            obj.setOSRelease( this.getOSRelease() );
            obj.setOSVersion( this.getOSVersion() );
            obj.setGlibc( this.getGlibc() );
        }
        catch( CloneNotSupportedException e ){
            //somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException("Clone not implemented in the base class of " + this.getClass().getName(),
                                       e );
        }
        return obj;

    }

    /**
     * Returns the output of the data class as string.
     * @return String
     */
    public String toString() {
        StringBuffer s = new StringBuffer();
        s.append( "{" );
        s.append( "arch=" + this.getArchitecture() );
        s.append( " os=" + this.getOS() );

        String release = this.getOSRelease();
        if ( release  != null && release.length() > 0 ) {
            s.append( " osrelease=" + release );
        }

        String version = this.getOSVersion();
        if ( version  != null && version.length() > 0 ) {
            s.append( " osversion=" + version );
        }
        s.append( "}" );
        return s.toString();
    }

}
