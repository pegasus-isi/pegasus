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

import edu.isi.pegasus.common.logging.LogManagerFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.VORSSiteCatalogUtil;
import edu.isi.pegasus.planner.catalog.site.classes.VORSSiteInfo;
import edu.isi.pegasus.planner.catalog.site.classes.VORSVOInfo;

/**
 * The VORS implementation of the Site Catalog interface.
 *
 * @author Atul Kumara
 * @author Karan Vahi
 */
public class VORS implements SiteCatalog {  
    
    /**
     * The default VORS mVORSHost.
     */
    public static final String DEFAULT_VORS_HOST = "vors.grid.iu.edu";
    
    /**
     * The default VORS mVORSPort.
     */
    public static final String DEFAULT_VORS_PORT = "80";
    
    
    /**
     * The SiteStore object where information about the sites is stored.
     */
    private SiteStore mSiteStore;
    
    
    private Map<String, VORSVOInfo> mVOInfo = null;
    
    /**
     * The mVORSHost where VORS is running.
     */
    private String mVORSHost;
    
    /**
     * The VORS mVORSPort.
     */
    private String mVORSPort;
    
    /**
     * The VO to which the user belongs to.
     */
    private String mVO;
    
    /**
     * The Grid for which information is required.
     */
    private String mGRID;
    
    /**
     * The handle to the log manager.
     */
    private LogManager mLogger;
	

    /**
     * The default constructor.
     */
	public VORS() {		
            mLogger = LogManagerFactory.loadSingletonInstance();       
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
            if (sites.get(0).trim().equals("*")) {
                siteItr = mVOInfo.keySet().iterator();
            } else {
                siteItr = sites.iterator();
            }
            while (siteItr.hasNext()) {
                String sitename = siteItr.next();
                VORSVOInfo temp = mVOInfo.get(sitename);
                if (temp == null) {
                    mLogger.log( sitename + " site not found.",
                                 LogManager.ERROR_MESSAGE_LEVEL );
                    continue;
                }
                if (temp.getStatus().equals("PASS")) {
                    VORSSiteInfo siteInfo = VORSSiteCatalogUtil.get_sites_info(mVORSHost, mVORSPort, mVO, mGRID, temp.getID());
                    siteInfo.setVoInfo(temp);
                    if (siteInfo.getOsg_grid() == null || siteInfo.getTmp_loc() == null) {
                        mLogger.log("Paths are null." + sitename + " is invalid.",
                                LogManager.CONFIG_MESSAGE_LEVEL);
                    } else {
                        mLogger.log("Site " + sitename + " is ACCESSIBLE",
                                LogManager.INFO_MESSAGE_LEVEL);
                        mSiteStore.addEntry(VORSSiteCatalogUtil.createSiteCatalogEntry(siteInfo));
                        ret++;
                    }
                } else {
                    mLogger.log("Site " + sitename + " is INACCESSIBLE",
                            LogManager.INFO_MESSAGE_LEVEL);
                }
            }
            
            //always add local site.
            VORSSiteInfo siteInfo = VORSSiteCatalogUtil.getLocalSiteInfo( mVO );
            VORSVOInfo local = new VORSVOInfo();
            local.setGrid( mGRID );
            siteInfo.setVoInfo( local );
            mLogger.log( "Site LOCAL . Creating default entry" , LogManager.INFO_MESSAGE_LEVEL );
            mSiteStore.addEntry(VORSSiteCatalogUtil.createSiteCatalogEntry(siteInfo));
            ret++;
            
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
		if(mVOInfo != null){
			mVOInfo.clear();
			mVOInfo = null;
		}		
	}

	/* (non-Javadoc)
	 * @see org.griphyn.common.catalog.Catalog#connect(java.util.Properties)
	 */
	public boolean connect(Properties props) {
		mVORSHost = props.getProperty( "vors.host", DEFAULT_VORS_HOST );
		mVORSPort = props.getProperty("vors.port", DEFAULT_VORS_PORT );
		mVO = props.getProperty("vors.vo", "all");
		mGRID = props.getProperty("vors.grid", "all");                
		
		Iterator<VORSVOInfo> itr = VORSSiteCatalogUtil.get_sites_in_grid(mVORSHost, mVORSPort, mVO, mGRID).iterator();		
		while(itr.hasNext()){
			if(mVOInfo == null){
				mVOInfo = new HashMap<String, VORSVOInfo>();
			}
			VORSVOInfo temp = itr.next();
			mVOInfo.put(temp.getName(), temp); 			
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see org.griphyn.common.catalog.Catalog#isClosed()
	 */
	public boolean isClosed() {		
		return mVOInfo == null;
	}

}
