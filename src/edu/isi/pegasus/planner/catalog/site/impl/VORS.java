/*
 * 
 *   Copyright 2007-2008 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */
package edu.isi.pegasus.planner.catalog.site.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.griphyn.cPlanner.common.LogManager;

import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.VORSSiteCatalogUtil;
import edu.isi.pegasus.planner.catalog.site.classes.VORSSiteInfo;
import edu.isi.pegasus.planner.catalog.site.classes.VORSVOInfo;

/**
 * @author akumar
 *
 */
public class VORS implements SiteCatalog {  
    public static final String DEFAULT_VORS_HOST = "vors.grid.iu.edu";
    public static final String DEFAULT_VORS_PORT = "80";
    //private
    public SiteStore mSiteStore;
    private Map<String, VORSVOInfo> voInfo = null;
    private String host;
    private String port;
    private String vo;
    private String grid;
    
    /**
     * The handle to the log manager.
     */
    private LogManager mLogger;
	

	public VORS() {		
            mLogger = LogManager.getInstance();	       
	    mSiteStore = new SiteStore();		
	}

	/* (non-Javadoc)
	 * @see edu.isi.pegasus.planner.catalog.SiteCatalog#insert(edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry)
	 */
	public int insert(SiteCatalogEntry entry) throws SiteCatalogException {
		mSiteStore.addEntry(entry);
		return 1;
	}

	/* (non-Javadoc)
	 * @see edu.isi.pegasus.planner.catalog.SiteCatalog#list()
	 */
	public Set<String> list() throws SiteCatalogException {		
		return mSiteStore.list();
	}

	/* (non-Javadoc)
	 * @see edu.isi.pegasus.planner.catalog.SiteCatalog#load(java.util.List)
	 */
	public int load(List<String> sites) throws SiteCatalogException {
		int ret = 0;
		Iterator<String> siteItr;
		if(sites.get(0).trim().equals("*")){
			siteItr = voInfo.keySet().iterator();
		}
		else{
			siteItr = sites.iterator();
		}
    	while(siteItr.hasNext()){
    		String sitename = siteItr.next();
    		VORSVOInfo temp = voInfo.get(sitename);
    		if(temp == null){
    			System.out.println(sitename + " site not found.");
    			continue;
    		}
    		if(temp.getStatus().equals("PASS")){
    			VORSSiteInfo siteInfo = VORSSiteCatalogUtil.get_sites_info(host, port, vo, grid, temp.getID());
    			siteInfo.setVoInfo(temp);
    			if(siteInfo.getOsg_grid() == null || siteInfo.getTmp_loc() == null){
    				mLogger.log("Paths are null."+ sitename + " is invalid.",
   	                     LogManager.CONFIG_MESSAGE_LEVEL);        			
    			}  
    			else{
                            mLogger.log( "Site " + sitename + " is ACCESSIBLE",
  	                                  LogManager.INFO_MESSAGE_LEVEL);     
                            mSiteStore.addEntry( VORSSiteCatalogUtil.createSiteCatalogEntry(siteInfo ));
    				ret++;
    			}    			    			    
    		}
    		else{
    			mLogger.log("Site " + sitename + " is INACCESSIBLE",
  	                     LogManager.INFO_MESSAGE_LEVEL);        			
    		}    		    	           
    	}
                /*////////////////////////FOR TESTING/////////////
        try {
            System.out.println(mSiteStore.toXML());
        } catch (IOException ex) {
            Logger.getLogger(VORS.class.getName()).log(Level.SEVERE, null, ex);
        }
                //////////////////////////////////////*/
		return ret;
	}

	/* (non-Javadoc)
	 * @see edu.isi.pegasus.planner.catalog.SiteCatalog#lookup(java.lang.String)
	 */
	public SiteCatalogEntry lookup(String handle) throws SiteCatalogException {		
		return mSiteStore.lookup(handle);
	}

	/* (non-Javadoc)
	 * @see edu.isi.pegasus.planner.catalog.SiteCatalog#remove(java.lang.String)
	 */
	public int remove(String handle) throws SiteCatalogException {
		throw new UnsupportedOperationException( "Method remove( String , String ) not yet implmeneted" );			
	}

	/* (non-Javadoc)
	 * @see org.griphyn.common.catalog.Catalog#close()
	 */
	public void close() {
		if(voInfo != null){
			voInfo.clear();
			voInfo = null;
		}		
	}

	/* (non-Javadoc)
	 * @see org.griphyn.common.catalog.Catalog#connect(java.util.Properties)
	 */
	public boolean connect(Properties props) {
		host = props.getProperty( "vors.host", DEFAULT_VORS_HOST );
		port = props.getProperty("vors.port", DEFAULT_VORS_PORT );
		vo = props.getProperty("vors.vo", "all");
		grid = props.getProperty("vors.grid", "all");                
		
		Iterator<VORSVOInfo> itr = VORSSiteCatalogUtil.get_sites_in_grid(host, port, vo, grid).iterator();		
		while(itr.hasNext()){
			if(voInfo == null){
				voInfo = new HashMap<String, VORSVOInfo>();
			}
			VORSVOInfo temp = itr.next();
			voInfo.put(temp.getName(), temp); 			
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see org.griphyn.common.catalog.Catalog#isClosed()
	 */
	public boolean isClosed() {		
		return voInfo == null;
	}

}
