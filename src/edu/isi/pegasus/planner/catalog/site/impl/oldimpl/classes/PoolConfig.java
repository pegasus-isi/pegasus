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

package edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LogManager;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A data class to store information about the various remote sites.
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @version $Revision$
 *
 * @see SiteInfo
 */
public class PoolConfig {

    /**
     * The map indexed by the site handle. Each value is a SiteInfo object,
     * containing the information about a grid site.
     */
    private HashMap mSiteCatalog;

    /**
     * The handle to the logging object.
     */
    private LogManager mLogger;

    /**
     * The default constructor.
     */
    public PoolConfig() {
        mLogger     = LogManagerFactory.loadSingletonInstance();
        mSiteCatalog = new HashMap(20);
    }

    /**
     * Adds a SiteInfo object to the container. If an entry already exists with
     * the same SiteID, it is overwritten.
     *
     * @param id    the id of the site, usually the name of the site.
     * @param site  the <code>SiteInfo</code> object containing the information
     *              about the site.
     */
    public void add(String id, SiteInfo site){
        mSiteCatalog.put(id,site);
    }

    /**
     * Adds all the sites in a controlled fashion, to the existing map containing
     * information about the sites. If an information about a site already
     * exists, it is overwritten.
     *
     * @param sites      a map indexed by siteid. Each value is a SiteInfo object.
     */
    public void add( PoolConfig sites){
        add(sites,true);
    }


    /**
     * Adds all the sites in a controlled fashion, to the existing map containing
     * information about the sites.
     *
     * @param sites      a map indexed by siteid. Each value is a SiteInfo object.
     * @param overwrite  resolves intersections, in case of a site already exists.
     *                   If true, the orginal site information is overwritten with
     *                   the new one. If false original site information remains.
     *
     */
    public void add( PoolConfig sites, boolean overwrite ){
        String id;
        SiteInfo site;
        boolean contains = false;
        for(Iterator it = sites.getSites().entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry)it.next();
            id   = (String)entry.getKey();
            site = (SiteInfo)entry.getValue();
            contains = contains(id);
            if( overwrite || !contains) {
                add(id,site);
            }
        }
    }

    /**
     * Returns a boolean indicating if an entry for a Site with a particular id
     * exists or not.
     *
     * @param id  the id of the site, usually the name of the site.
     *
     * @return true if entry for the site exists, else false.
     */
    public boolean contains(String id){
        return mSiteCatalog.containsKey(id);
    }

    /**
     * Retrives the information about a site.
     *
     * @param siteID the id of the site, usually the name of the site.
     * @return <code>SiteInfo</code> containing the site layout,
     *         else null in case of site not existing.
     */
    public SiteInfo get(String siteID){
        if(mSiteCatalog.containsKey(siteID)) {
            return ( SiteInfo ) mSiteCatalog.get( siteID );
        } else {
            mLogger.log("Site '" + siteID + "' does not exist in the Site Catalog.",
                        LogManager.ERROR_MESSAGE_LEVEL);
            return null;
        }
    }


    /**
     * Returns information about all the sites.
     *
     * @return a Map indexed by the site id (name of the site). Each value is a
     *         <code>SiteInfo</code> object.
     */
    public Map getSites(){
        return mSiteCatalog;
    }


    /**
     * Returns the textual description of the  contents of <code>PoolConfig</code>
     * object in the multiline format.
     *
     * @return the textual description in multiline format.
     */
    public String toMultiLine() {
        String output="";
        for (Iterator i = mSiteCatalog.keySet().iterator();i.hasNext();){
            String poolid = (String)i.next();
            //Karan Oct 13,2005
            //This is moved to SiteInfo.toMultiLine()
            //output += "pool " + poolid +
              output += ((SiteInfo)mSiteCatalog.get(poolid)).toMultiLine()+"\n";
        }
        return output;
    }

    /**
     * Returns the XML description of the  contents of <code>PoolConfig</code>
     * object.
     *
     * @return the xml description.
     */
    public String toXML(){
        String output="";
        for (Iterator i = mSiteCatalog.keySet().iterator();i.hasNext();){
            String poolid=(String)i.next();
            output+="  <site handle=\""+poolid+"\"" +
                ((SiteInfo)mSiteCatalog.get(poolid)).toXML();
        }
        // System.out.println(output);
        return output;
    }
}
