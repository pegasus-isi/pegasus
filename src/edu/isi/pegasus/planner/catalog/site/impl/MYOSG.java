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


import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LoggingKeys;

import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.MYOSGSiteInfo;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.MYOSGSiteInfoFacade;

import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.URLParamConstants;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGSiteCatalogParser;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGSiteCatalogUtil;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGSiteConstants;


import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * This class implements the SiteCatalog interface
 * 
 * 
 * @author Prasanth Thomas
 */
public class MYOSG implements SiteCatalog {

    private Map<String, MYOSGSiteInfo> mMYOSGInfo = null;
	
    /**
     * The Logging instance.
     */
    private LogManager mLogger;
    
    /**
     * The Site Catalog file to be parser.
     */
    private String mFilename;
    
    /**
     * The SiteStore object where information about the sites is stored.
     */
    private SiteStore mSiteStore;

    public MYOSG() {
        mLogger = LogManagerFactory.loadSingletonInstance();
        mSiteStore = new SiteStore();
    }


    /**
     * Not implemented as yet.
     * 
     * @param entry
     *
     * @return
     * @throws edu.isi.pegasus.planner.catalog.site.SiteCatalogException
     */
    public int insert(SiteCatalogEntry entry) throws SiteCatalogException {
        mSiteStore.addEntry(entry);
	return 1;
    }

    /**
     * Lists the site handles for all the sites in the Site Catalog.
     * 
     * @return A set of site handles.
     * 
     * @throws SiteCatalogException
     *             in case of error.
     */
    public Set<String> list() throws SiteCatalogException {
	return mSiteStore.list();
    }

    /**
     * Loads up the Site Catalog implementation with the sites whose site
     * handles are specified. This is a convenience method, that can allow the
     * backend implementations to maintain soft state if required.
     * 
     * If the implementation chooses not to implement this, just do an empty
     * implementation.
     * 
     * The site handle * is a special handle designating all sites are to be
     * loaded.
     * 
     * @param sites             the list of sites to be loaded.
     * 
     * @return the number of sites loaded.
     * 
     * @throws SiteCatalogException
     *             in case of error.
     */
    public int load(List<String> sites) throws SiteCatalogException {
        if (this.isClosed()) {
            throw new SiteCatalogException(
					"Need to connect to site catalog before loading");
	}
	mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_PARSE_SITE_CATALOG,
				"site-catalog.id", mFilename, LogManager.DEBUG_MESSAGE_LEVEL);
		
        int ret = 0;
		 
        Iterator<String> siteItr;
        if (sites.get(0).trim().equals("*")) {
             siteItr = mMYOSGInfo.keySet().iterator();
        } else {
             siteItr = sites.iterator();
        }
		
	while ( siteItr.hasNext() ) {
            String sitename = siteItr.next();
            MYOSGSiteInfo temp = mMYOSGInfo.get(sitename);
            if (temp == null) {
                mLogger.log( sitename + " site not found.",
                             LogManager.ERROR_MESSAGE_LEVEL);
		continue;
            }
	
            MYOSGSiteInfoFacade myOSiteInfoFacade = new MYOSGSiteInfoFacade(temp);
            if (myOSiteInfoFacade.isValidSite()) {
                mSiteStore.addEntry(MYOSGSiteCatalogUtil
						.createSiteCatalogEntry(new MYOSGSiteInfoFacade(temp)));
		ret++;
            }else{
                mLogger.log(sitename + " site information is incomplete.",
                            LogManager.INFO_MESSAGE_LEVEL);
				
            }
        }
	
        mLogger.logEventCompletion(LogManager.DEBUG_MESSAGE_LEVEL);
	return ret;
    }

    /**
     * Retrieves the <code>SiteCatalogEntry</code> for a site.
     * 
     * @param handle
     *            the site handle / identifier.
     * 
     * @return SiteCatalogEntry in case an entry is found , or <code>null</code>
     *         if no match is found.
     * 
     * 
     * @throws SiteCatalogException
     *             in case of error.
     */
    public SiteCatalogEntry lookup(String handle) throws SiteCatalogException {
	return mSiteStore.lookup(handle);
    }

    /**
     * Not yet implemented as yet.
     * 
     * @param handle
     *
     * @return
     * 
     * @throws edu.isi.pegasus.planner.catalog.site.SiteCatalogException
     */
    public int remove(String handle) throws SiteCatalogException {
	throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Close the connection to back end file.
     */
    public void close() {
	mFilename = null;
    }

    /**
     * Establishes a connection to the file from the properties. You will need
     * to specify a "file" property to point to the location of the on-disk
     * instance.
     * 
     * @param props
     *            is the property table with sufficient settings to to connect
     *            to the implementation.
     * 
     * @return true if connected, false if failed to connect.
     * 
     * @throws SiteCatalogException
     */
    public boolean connect(Properties props) throws SiteCatalogException {
        
        if (!props.containsKey("file")){
            return false; 
	}
	connect(props.getProperty("file"));
        
        
	MYOSGSiteCatalogParser myOSGCatalogCreator = new MYOSGSiteCatalogParser();
	mLogger.logEventStart(LoggingKeys.EVENT_PEGASUS_PARSE_SITE_CATALOG,
				"site-catalog.id", mFilename, LogManager.DEBUG_MESSAGE_LEVEL);
	myOSGCatalogCreator.startParser(mFilename);
	mLogger.logEventCompletion(LogManager.DEBUG_MESSAGE_LEVEL);
	for( Iterator <MYOSGSiteInfo> itr =  myOSGCatalogCreator.getSites().iterator();	itr.hasNext();){
            if(mMYOSGInfo == null){
                mMYOSGInfo = new HashMap<String, MYOSGSiteInfo>();
            }
            MYOSGSiteInfo myOSGSiteInfo = itr.next();
            mMYOSGInfo.put((String)myOSGSiteInfo.getProperty(MYOSGSiteConstants.SITE_NAME_ID), myOSGSiteInfo); 			
	}
	
        return true;

    }
    
    
    
    /**
     * Initializes the Site Catalog Parser instance for the file.
     * 
     * @param filename
     *            is the name of the file to read.
     * 
     * @return true,
     */ 
    private boolean connect(String filename) {
        mFilename = filename;
	File f = new File(filename);
	if (f.exists() && f.canRead()) {
            return true;
	} else {
            throw new RuntimeException("Cannot read or access file " + filename);
	}
    }


    /**
     * Returns if the connection is closed or not.
     * 
     * @return
     */
    public boolean isClosed() {
        // TODO Auto-generated method stub
	return (mFilename == null);
    }

}
