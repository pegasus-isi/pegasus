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

package edu.isi.pegasus.planner.catalog.site.classes;

import edu.isi.pegasus.planner.catalog.classes.Architecture;
import edu.isi.pegasus.planner.catalog.classes.OS;
import edu.isi.pegasus.planner.catalog.classes.Profiles;

import org.griphyn.cPlanner.classes.Profile;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import java.io.Writer;
import java.io.IOException;
        
/**
 * This data class describes a site in the site catalog.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Site extends AbstractSiteData{

    
    /**
     * The site identifier. 
     */
    private String mID;
    
    /**
     * The OS of the site. 
     */
    private OS mOS;
    
    /**
     * The architecture of the site.
     */
    private Architecture mArch;
    
    /**
     * Optional information about the os release.
     */
    private String mOSRelease;
    
    /**
     * Optional information about the version.
     */
    private String mOSVersion;
    
    /**
     * Optional information about the glibc.
     */
    private String mGlibc;
    
    /**
     * The profiles asscociated with the site.
     */
    private Profiles mProfiles;
    
    /**
     * The handle to the head node filesystem.
     */
    private HeadNodeFS mHeadFS;
    
    /**
     * The handle to the worker node filesystem.
     */
    private WorkerNodeFS mWorkerFS;
    
    
    /**
     * Map of grid gateways at the site for submitting different job types.
     */
    private Map<GridGateway.JOB_TYPE, GridGateway> mGridGateways;
    
    /**
     * The list of replica catalog associated with the site.
     */
    private List<ReplicaCatalog> mReplicaCatalogs;
    
    /**
     * The overloaded constructor.
     * 
     * @param id   the site identifier.
     */
    public Site( String id ) {
        mID       = id;
        mArch     = Architecture.x86;
        mOS       = OS.LINUX;      
        mProfiles        = new Profiles();
        mGridGateways    = new HashMap();
        mReplicaCatalogs = new LinkedList();
    }
    
    /**
     * Sets the site handle for the site
     * 
     * @param id  the site identifier.
     */
    public void setSiteHandle( String id ){
        mID = id;
    }
    
    
    /**
     * Returns the site handle for the site
     * 
     * @return  the site identifier.
     */
    public String getSiteHandle( ){
        return mID;
    }
    
    /**
     * Sets the architecture of the site.
     * 
     * @param arch  the architecture.
     */
    public void setArchitecture( Architecture arch ){
        mArch = arch;
    }
    
    
    /**
     * Returns the architecture of the site.
     * 
     * @return  the architecture.
     */
    public Architecture getArchitecture( ){
        return mArch;
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
     * Sets the headnode filesystem.
     * 
     * @param system   the head node filesystem.
     */
    public void setHeadNodeFS( HeadNodeFS system ){
        mHeadFS = system;
    }
    
    
    /**
     * Returns the headnode filesystem.
     * 
     * @returm   the head node filesystem.
     */
    public HeadNodeFS getHeadNodeFS(  ){
        return mHeadFS;
    }
    
    /**
     * Sets the worker node filesystem.
     * 
     * @param system   the head node filesystem.
     */
    public void setWorkerNodeFS( WorkerNodeFS system ){
        mWorkerFS = system;
    }
    
    
    /**
     * Returns the worker node filesystem.
     * 
     * @returm   the worker node filesystem.
     */
    public WorkerNodeFS getWorkerNodeFS(  ){
        return mWorkerFS;
    }
    
    /**
     * Adds a profile.
     * 
     * @param profile  the profile to be added
     */
    public void addProfile( Profile p ){
        //retrieve the appropriate namespace and then add
       mProfiles.addProfile(  p );
    }
    
    /**
     * Returns the profiles associated with the site.
     * 
     * @return profiles.
     */
    public Profiles getProfiles( ){
        return mProfiles;
    }
    
    /**
     * Returns a grid gateway object corresponding to a job type.
     * 
     * @return GridGateway
     */
    public GridGateway getGridGateway( GridGateway.JOB_TYPE type ){
        return mGridGateways.get( type );
    }
    
    /**
     * Return an iterator to value set of the Map.
     * 
     * @return Iterator<GridGateway>
     */
    public Iterator<GridGateway> getGridGatewayIterator(){        
        return mGridGateways.values().iterator();
    }
    
    /**
     * Add a GridGateway to the site.
     * 
     * @param gateway   the grid gateway to be added.
     */
    public void addGridGateway( GridGateway g ){
        mGridGateways.put( g.getJobType(), g );
    }
    
    
    /**
     * Return an iterator to the replica catalog associated with the site.
     * 
     * @return Iterator<ReplicaCatalog>
     */
    public Iterator<ReplicaCatalog> getReplicaCatalogIterator(){        
        return mReplicaCatalogs.iterator();
    }
    
    /**
     * Add a Replica Catalog to the site.
     * 
     * @param gateway   the grid gateway to be added.
     */
    public void addReplicaCatalog( ReplicaCatalog catalog ){
        mReplicaCatalogs.add( catalog );
    }
    
    /**
     * Writes out the xml description of the object. 
     *
     * @param writer is a Writer opened and ready for writing. This can also
     *               be a StringWriter for efficient output.
     * @param indent the indent to be used.
     *
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML( Writer writer, String indent ) throws IOException {
        String newLine = System.getProperty( "line.separator", "\r\n" );
        String newIndent = indent + "\t";
        
        //write out the  xml element
        writer.write( indent );
        writer.write( "<site " );        
        writeAttribute( writer, "handle", getSiteHandle() );
        writeAttribute( writer, "arch", getArchitecture().toString() );        
        writeAttribute( writer, "os", getOS().toString() );
       
        String val = null;
        if ( ( val = this.getOSRelease() ) != null ){
            writeAttribute( writer, "osrelease", val );
        }
        
        if ( ( val = this.getOSVersion() ) != null ){
            writeAttribute( writer, "osversion", val );
        }
         
        if ( ( val = this.getGlibc() ) != null ){
            writeAttribute( writer, "glibc", val );
        }
        
        writer.write( ">");
        writer.write( newLine );
        
        //list all the gridgateways
        for( Iterator<GridGateway> it = this.getGridGatewayIterator(); it.hasNext(); ){
            it.next().toXML( writer, newIndent );
        }
        
        HeadNodeFS fs = null;
        if( (fs = this.getHeadNodeFS()) != null ){
            fs.toXML( writer, newIndent );
        }
        
        
        WorkerNodeFS wfs = null;
        if( ( wfs = this.getWorkerNodeFS() ) != null ){
            wfs.toXML( writer, newIndent );
        }
        
        //list all the replica catalogs associate
        for( Iterator<ReplicaCatalog> it = this.getReplicaCatalogIterator(); it.hasNext(); ){
            it.next().toXML( writer, newIndent );
        }
        
        this.getProfiles().toXML( writer, newIndent );
        
        writer.write( indent );
        writer.write( "</site>" );
        writer.write( newLine );
    }

    
}