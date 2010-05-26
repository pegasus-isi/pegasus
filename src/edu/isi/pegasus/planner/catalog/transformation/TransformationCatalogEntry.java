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


package edu.isi.pegasus.planner.catalog.transformation;

/**
 * An object of this class corresponds to a
 * tuple in the Transformation Catalog.
 * @author Gaurang Mehta
 * @$Revision$
 *
 * @see org.griphyn.common.classes.VDSSysInfo
 * @see org.griphyn.common.classes.TCType
 */

import edu.isi.pegasus.planner.catalog.classes.CatalogEntry;
import edu.isi.pegasus.planner.catalog.classes.VDSSysInfo2NMI;

import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.NMI2VDSSysInfo;

import edu.isi.pegasus.common.util.ProfileParser;
import edu.isi.pegasus.common.util.Separator;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import org.griphyn.cPlanner.classes.Profile;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TransformationCatalogEntry
    implements CatalogEntry {

    
    /**
     * The logical namespace of the transformation
     */

    private String namespace;

    /**
     * The version of the transformation.
     */
    private String version;

    /**
     *  The logical name of the transformation.
     */
    private String name;

    /**
     *  The Id of the resource on which the transformation
     *  is installed.
     */
    private String resourceid;

    /**
     * The physical path on the resource for a particual arch, os and type.
     */
    private String physicalname;

    /**
     * The profiles associated with the transformation;
     */
    private List profiles;

    /**
     * The System Info for the transformation.
     */
    private SysInfo mSysInfo;

    
    /**
     * The type of transformation. Takes one of the predefined enumerated type TCType.
     */
    private TCType type = TCType.INSTALLED;

    /**
     * The basic constructor
     */
    public TransformationCatalogEntry() {
        namespace = null;
        name = null;
        version = null;
        resourceid = null;
        physicalname = null;
        profiles = null;
//        sysinfo = null;
        mSysInfo = null;
    }

    /**
     * Optimized Constructor
     *
     * @param namespace String
     * @param name String
     * @param version String
     */
    public TransformationCatalogEntry( String namespace,
                                       String name,
                                       String version){
        this.namespace = namespace;
        this.version = version;
        this.name = name;
    }
    /**
     *  Optimized Constructor
     * @param namespace String
     * @param name String
     * @param version String
     * @param resourceid String
     * @param physicalname String
     * @param type TCType
     * @param profiles List
     * @param sysinfo VDSSysInfo
     */
    public TransformationCatalogEntry( String namespace, String name,
        String version,
        String resourceid, String physicalname, TCType type,
        List profiles,
        VDSSysInfo sysinfo ) {
        this.namespace = namespace;
        this.version = version;
        this.name = name;
        this.resourceid = resourceid;
        this.physicalname = physicalname;
        this.profiles = profiles;

        //       this.sysinfo = sysinfo;
        mSysInfo  = VDSSysInfo2NMI.vdsSysInfo2NMI( sysinfo );

        this.type = type;

    }

    /**
     * creates a new instance of this object and returns
     * you it. A shallow clone.
     * TO DO : Gaurang correct the clone method.
     *
     * @return Object
     */
    public Object clone() {
        return new TransformationCatalogEntry( namespace, name, version,
            resourceid, physicalname,
            type, profiles, this.getVDSSysInfo() );
    }

    /**
     * gets the String version of the
     * data class
     * @return String
     */
    public String toString() {
        String st = "\n " +
            "\n Logical Namespace : " + this.namespace +
            "\n Logical Name      : " + this.name +
            "\n Version           : " + this.version +
            "\n Resource Id       : " + this.resourceid +
            "\n Physical Name     : " + this.physicalname +
//            "\n VDSSysInfo           : " + ((this.sysinfo == null) ? "" : this.sysinfo.toString()) +
            "\n SysInfo           : " + this.getVDSSysInfo() +

            "\n TYPE              : " + ((this.type == null) ? "" : type.toString());
        if(profiles != null){
            for (Iterator i = profiles.listIterator(); i.hasNext(); ) {
                st = st +
                    "\n Profile           : " + ( (Profile) i.next()).toString();
            }
        }
        return st;

    }

    /**
     * Prints out a TC file format String.
     * @return String
     */
    public String toTCString() {
        String st = this.getResourceId() + "\t" +
            this.getLogicalTransformation() + "\t" +
            this.getPhysicalTransformation() + "\t" +
            this.getType() + "\t" +
            this.getVDSSysInfo() + "\t";
        if ( profiles != null ) {
            st += ProfileParser.combine( profiles );
        } else {
            st += "NULL";
        }
        return st;
    }

    /**
     * Returns an xml output of the contents of the data class.
     * @return String
     */
    public String toXML() {
        String xml = "\t\t<pfn physicalName=\"" +
            this.getPhysicalTransformation() + "\""
            + " siteid=\"" + this.getResourceId() + "\""
            + " type=\"" + this.getType() + "\""
            + " sysinfo=\"" + this.getVDSSysInfo() + "\"";
        if ( this.profiles != null ) {
            xml += " >\n";
            for ( Iterator iter = this.profiles.iterator(); iter.hasNext(); ) {
                Profile profile = ( Profile ) iter.next();
                xml += "\t\t\t" + profile.toXML() + "\n";
            }
            xml += "\t\t</pfn>\n";
        } else {
            xml += " />\n";
        }

        return xml;
    }

    /**
     * Set the logical transformation with a fully qualified tranformation String of the format NS::NAME:Ver
     * @param logicaltransformation String
     */
    public void setLogicalTransformation( String logicaltransformation ) {
        String[] ltr;
        ltr = splitLFN( logicaltransformation );
        this.namespace = ltr[ 0 ];
        this.name = ltr[ 1 ];
        this.version = ltr[ 2 ];
    }

    /**
     * Set the logical transformation by providing the namespace, name and version as seperate strings.
     * @param namespace String
     * @param name String
     * @param version String
     */
    public void setLogicalTransformation( String namespace, String name,
        String version ) {
        this.namespace = namespace;
        this.name = name;
        this.version = version;
    }

    /**
     * Set the logical namespace of the transformation.
     * @param namespace String
     */
    public void setLogicalNamespace( String namespace ) {
        this.namespace = namespace;
    }

    /**
     * Set the logical name of the transformation.
     * @param name String
     */
    public void setLogicalName( String name ) {
        this.name = name;
    }

    /**
     * Set the logical version of the transformation.
     * @param version String
     */
    public void setLogicalVersion( String version ) {
        this.version = version;
    }

    /**
     *  Set the resourceid where the transformation is available.
     * @param resourceid String
     */
    public void setResourceId( String resourceid ) {
        this.resourceid = resourceid;
    }

    /**
     * Set the type of the transformation.
     * @param type TCType
     */
    public void setType( TCType type ) {
        this.type = ( type == null ) ? TCType.INSTALLED : type;
    }

    /**
     * Set the physical location of the transformation.
     * @param physicalname String
     */
    public void setPhysicalTransformation( String physicalname ) {
        this.physicalname = physicalname;
    }

    /**
     * Set the System Information associated with the transformation.
     * @param sysinfo VDSSysInfo
     */
    public void setVDSSysInfo( VDSSysInfo sysinfo ) {
        this.mSysInfo = ( sysinfo == null ) ? new SysInfo() : VDSSysInfo2NMI.vdsSysInfo2NMI(sysinfo);
    }



    /**
     * Allows you to add one profile at a time to the transformation.
     * @param profile Profile  A single profile consisting of namespace, key and value
     */
    public void setProfile( Profile profile ) {
        if ( profile != null ) {
            if ( this.profiles == null ) {
                this.profiles = new ArrayList( 5 );
            }
            this.profiles.add( profile );
        }
    }

    /**
     * Allows you to add multiple profiles to the transformation.
     * @param profiles List of Profile objects containing the profile information.
     */
    public void setProfiles( List profiles ) {
        if ( profiles != null ) {
            if ( this.profiles == null ) {
                this.profiles = new ArrayList( profiles.size() );
            }
            this.profiles.addAll( profiles );
        }
    }

    /**
     * Gets the Fully Qualified Transformation name in the format NS::Name:Ver.
     * @return String
     */
    public String getLogicalTransformation() {
        return joinLFN( namespace, name, version );
    }

    /**
     * Returns the Namespace associated with the logical transformation.
     * @return String Returns null if no namespace associated with the transformation.
     */
    public String getLogicalNamespace() {
        return this.namespace;
    }

    /**
     * Returns the Name of the logical transformation.
     * @return String
     */
    public String getLogicalName() {
        return this.name;
    }

    /**
     * Returns the version of the logical transformation.
     * @return String Returns null if no version assocaited with the transformation.
     */
    public String getLogicalVersion() {
        return this.version;
    }

    /**
     * Returns the resource where the transformation is located.
     * @return String
     */
    public String getResourceId() {
        return this.resourceid;
    }

    /**
     * Returns the type of the transformation.
     * @return TCType
     */
    public TCType getType() {
        return this.type;
    }

    /**
     * Returns the physical location of the transformation.
     * @return String
     */
    public String getPhysicalTransformation() {
        return this.physicalname;
    }

    /**
     * Returns the System Information associated with the transformation.
     *
     *
     * @return SysInfo
     */
    public SysInfo getSysInfo(  ) {
        return   mSysInfo;
    }

    /**
     * Returns the System Information in the old VDS format associated with the
     * transformation.
     *
     *
     * @return VDSSysInfo
     */
    public VDSSysInfo getVDSSysInfo(  ) {
        return NMI2VDSSysInfo.nmiToVDSSysInfo( mSysInfo );
    }

    /**
     * Returns the list of profiles associated with the transformation.
     * @return List Returns null if no profiles associated.
     */
    public List getProfiles() {
        return this.profiles;
    }

    /**
     * Returns the profiles for a particular Namespace.
     * @param namespace String The namespace of the profile
     * @return List   List of Profile objects. returns null if none are found.
     */
    public List getProfiles( String namespace ) {
        List results = null;
        if ( profiles != null ) {
            for ( Iterator i = profiles.iterator(); i.hasNext(); ) {
                Profile p = ( Profile ) i.next();
                if ( p.getProfileNamespace().equalsIgnoreCase( namespace ) ) {
                    if ( results == null ) {
                        results = new ArrayList();
                    }
                    results.add( p );
                }
            }
            return results;
        } else {
            return results;
        }
    }

    /**
     * Joins the 3 components into a fully qualified logical name of the format NS::NAME:VER
     * @param namespace String
     * @param name String
     * @param version String
     * @return String
     */
    private static String joinLFN( String namespace, String name,
        String version ) {
        return Separator.combine( namespace, name, version );
    }

    /**
     * Splits the full qualified logical transformation into its components.
     * @param logicaltransformation String
     * @return String[]
     */
    private static String[] splitLFN( String logicaltransformation ) {
        return Separator.split( logicaltransformation );
    }

    /**
     * Converts the file profile string to a list of Profiles.
     * @param profiles String The profile string.
     * @return List Returns a list of profile objects
     */
    /* public List stringToProfiles( String profiles ) {
         if ( profiles == null ) {
             return null;
         }
         List resultprofiles = new ArrayList();
         String[] namespaces = profiles.split( ";" );
         for ( int i = 0; i < namespaces.length; i++ ) {
             String[] nsprofiles = namespaces[ i ].split( "::", 2 );
             if ( nsprofiles.length == 2 ) {
                 String ns = nsprofiles[ 0 ].trim();
                 String[] keyvalues = nsprofiles[ 1 ].trim().split( "," );
                 for ( int j = 0; j < keyvalues.length; j++ ) {
                     String[] keyvalue = keyvalues[ j ].trim().split( "=", 2 );
                     String key = null;
                     String value = null;
                     if ( keyvalue.length == 2 ) {
                         key = keyvalue[ 0 ].trim();
                         value = keyvalue[ 1 ].trim();
                     }
                     if ( key != null && !key.equals( "" ) && value != null ) {
                         Profile p = new Profile( ns, key, value );
                         resultprofiles.add( p );
                     }
                 }
             }
         }
         return resultprofiles;
     }
     */

    /*
     * Generates a file type profiles String.
     * @param listprofiles List
     * @return String
     */
    /*    public String profilesToString( List listprofiles ) {
            String lprofiles = null;
//        String temp = null;
            if ( listprofiles != null ) {
                lprofiles=ProfileParser.combine( listprofiles );
            }
                String currentns = "";
                for ( Iterator i = listprofiles.iterator(); i.hasNext(); ) {
                    Profile p = ( Profile ) i.next();
     if ( !currentns.equalsIgnoreCase( p.getProfileNamespace() ) ) {
                        currentns = p.getProfileNamespace();
                        if ( lprofiles != null ) {
     lprofiles = lprofiles + temp + ";" + currentns + "::";
                            temp = null;
                        } else {
                            lprofiles = currentns + "::";
                        }
                    }
                    if ( temp != null ) {
                        temp = temp + "," + p.getProfileKey() + "=" +
                            p.getProfileValue();
                    } else {
                        temp = p.getProfileKey() + "=" + p.getProfileValue();
                    }
                }
                lprofiles += temp;
            }
            return lprofiles;
        }
     */

    /**
     * Compares two catalog entries for equality.
     *
     * @param entry is the entry to compare with
     * @return true if the entries match, false otherwise
     */
    public boolean equals( TransformationCatalogEntry entry ) {
        return this.toTCString().equalsIgnoreCase( entry.toTCString() );
    }
}
