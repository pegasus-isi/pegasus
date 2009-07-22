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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.griphyn.common.util.Boolean;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.MYOSGSiteInfo;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.MYOSGSiteInfoFacade;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.DateUtils;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGSiteCatalogParser;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGSiteCatalogUtil;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGSiteConstants;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGURLGenerator;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.SiteScrapper;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.URLParamConstants;

/**
 * This class implements the SiteCatalog interface
 * 
 * 
 * @author Prasanth Thomas
 * @author Karan Vahi
 * 
 * @version $Revision$
 */
public class MYOSG implements SiteCatalog {

	/**
	 * The date format to be used while passing dates to the URL construction.
	 */
	public static final String DATE_FORMAT = "MM/dd/yyyy";

	/**
	 * The default VO to use if none is specified
	 */
	public static final String DEFAULT_VO = "LIGO";

	/**
	 * The default VO to use if none is specified
	 */
	public static final String DEFAULT_GRID = "OSG";
	
	/**
	 * The name of the key that determines whether we keep the tmp xml file
	 * around or not.
	 */
	public static final String KEEP_TMP_FILE_KEY = "myosg.keep.tmp.file";

	/**
	 * The name of the key that determines what VO to query for.
	 */
	public static final String VO_KEY = "myosg.vo";
	
	/**
	 * The name of the key that determines whether we keep the tmp xml file
	 * around or not.
	 */
	public static final String GRID_KEY = "myosg.grid";
	
	private Map<String, MYOSGSiteInfo> mMYOSGInfo = null;

	/**
	 * The Logging instance.
	 */
	private final LogManager mLogger;

	/**
	 * A boolean variable tracking whether catalog is connected or not
	 */
	private boolean mConnected;

	/**
	 * A boolean variable that tracks whether to keep the temp xml file or not.
	 */
	private boolean mKeepTmpFile;

	/**
	 * The Site Catalog file to be parser.
	 */
	// private String mFilename;
	/**
	 * The SiteStore object where information about the sites is stored.
	 */
	private final SiteStore mSiteStore;

	/**
	 * The vo for which to query MYOSG
	 */
	private String mVO;
	
	/**
	 * The grid for which to query MYOSG
	 */
	private String mGrid;
	
	public MYOSG() {
		mLogger = LogManagerFactory.loadSingletonInstance();
		mSiteStore = new SiteStore();
		mConnected = false;
		mKeepTmpFile = false;
		mVO = MYOSG.DEFAULT_VO;
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
	 * @param sites
	 *            the list of sites to be loaded.
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

		int ret = 0;

		Iterator<String> siteItr;
		if (sites.get(0).trim().equals("*")) {
			siteItr = mMYOSGInfo.keySet().iterator();
		} else {
			siteItr = sites.iterator();
		}

		while (siteItr.hasNext()) {
			String sitename = siteItr.next();
			MYOSGSiteInfo temp = mMYOSGInfo.get(sitename);
			if (temp == null) {
				mLogger.log(sitename + " site not found.",
						LogManager.ERROR_MESSAGE_LEVEL);
				continue;
			}

			MYOSGSiteInfoFacade myOSiteInfoFacade = new MYOSGSiteInfoFacade(
					temp);
			if (myOSiteInfoFacade.isValidSite()) {
				mSiteStore.addEntry(MYOSGSiteCatalogUtil
						.createSiteCatalogEntry(new MYOSGSiteInfoFacade(temp)));
				ret++;
			} else {
				mLogger.log( "Not constructing entry for site " + sitename + " "+  
							  myOSiteInfoFacade.getSitesMissingInformation(sitename),
						LogManager.INFO_MESSAGE_LEVEL);

			}
		}

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
		mConnected = false;
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
		/*
		 * for( Iterator it = props.keySet().iterator(); it.hasNext(); ){ String
		 * key = (String)it.next(); System.out.println( key + " -> " +
		 * props.getProperty( key )); }
		 */

		if (props.containsKey(MYOSG.KEEP_TMP_FILE_KEY)) {
			String value = props.getProperty(MYOSG.KEEP_TMP_FILE_KEY);
			mKeepTmpFile = Boolean.parse(value, false);
		}
		
		/* determine the VO and grid if specified */
		if (props.containsKey( MYOSG.VO_KEY )) {
			mVO = props.getProperty( MYOSG.VO_KEY );
		}
		if (props.containsKey( MYOSG.GRID_KEY )) {
			mGrid = props.getProperty( MYOSG.GRID_KEY );
		}
		
		mLogger.log( "MYOSG queried for VO " + mVO + " for grid " + mGrid ,
					 LogManager.DEBUG_MESSAGE_LEVEL );
		
		/* generate the HTTP URL to the MYOSG website. */
		String urlString = new MYOSGURLGenerator().getURL(this
				.createConnectionURLProperties());
		mLogger.log("HTTP URL constructed to the MYOSG website " + urlString,
				LogManager.DEBUG_MESSAGE_LEVEL);

		/* grab the XML on the web url and populate to a temp file */
		File temp;
		try {
			temp = File.createTempFile("myosg-", ".xml");
		} catch (IOException ioe) {
			throw new SiteCatalogException("Unable to create a temp file ", ioe);
		}

		String tempPath = temp.getAbsolutePath();
		SiteScrapper.scrapeSite(urlString, tempPath);
		mLogger.log("Webpage retrieved to " + tempPath,
				LogManager.DEBUG_MESSAGE_LEVEL);

		MYOSGSiteCatalogParser myOSGCatalogCreator = new MYOSGSiteCatalogParser();
		mLogger.logEventStart(LoggingKeys.EVENT_PEGASUS_PARSE_SITE_CATALOG,
				"site-catalog.id", tempPath, LogManager.DEBUG_MESSAGE_LEVEL);
		myOSGCatalogCreator.startParser(tempPath);
		mLogger.logEventCompletion(LogManager.DEBUG_MESSAGE_LEVEL);
		for (Iterator<MYOSGSiteInfo> itr = myOSGCatalogCreator.getSites()
				.iterator(); itr.hasNext();) {
			if (mMYOSGInfo == null) {
				mMYOSGInfo = new HashMap<String, MYOSGSiteInfo>();
			}
			MYOSGSiteInfo myOSGSiteInfo = itr.next();
			mMYOSGInfo.put((String) myOSGSiteInfo
					.getProperty(MYOSGSiteConstants.SITE_NAME_ID),
					myOSGSiteInfo);
		}

		/* delete the temp file if required */
		if (!mKeepTmpFile){
                    mLogger.log( "Deleting temp file " + tempPath, 
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                    temp.delete();
		}

		mConnected = true;
		return true;

	}

	/**
	 * Creates the properties that are required to compose the HTTP URL to the
	 * MYOSG website.
	 * 
	 * @return
	 */
	private Properties createConnectionURLProperties() {
		Properties properties = new Properties();

		properties.setProperty(""
				+ URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWSERVICE, "on");
		properties.setProperty(""
				+ URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWRSVSTATUS, "on");
		properties.setProperty(""
				+ URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWFQDN, "on");
		properties.setProperty(""
				+ URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWVOMEMBERSHIP, "on");
		properties.setProperty(""
				+ URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWVOOWNERSHIP, "on");
		properties.setProperty(""
				+ URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWENVIRONMNENT, "on");
		properties.setProperty(""
				+ URLParamConstants.PARAM_GIP_STATUS_ATTRS_SHOWTESTRESULTS,
				"on");
		properties.setProperty(""
				+ URLParamConstants.PARAM_GIP_STATUS_ATTRS_SHOWFQDN, "on");
		properties.setProperty("" + URLParamConstants.PARAM_ACCOUNT_TYPE,
				"cumulative_hours");
		properties.setProperty("" + URLParamConstants.PARAM_CE_ACCOUNT_TYPE,
				"gip_vo");
		properties.setProperty("" + URLParamConstants.PARAM_SE_ACCOUNT_TYPE,
				"vo_transfer_volume");
		properties.setProperty("" + URLParamConstants.PARAM_START_TYPE,
				"7daysago");
		properties.setProperty("" + URLParamConstants.PARAM_START_DATE,
				getStartDate());
		properties.setProperty("" + URLParamConstants.PARAM_END_TYPE, "now");
		properties.setProperty("" + URLParamConstants.PARAM_END_DATE,
				getDateAfter(7));
		properties.setProperty(""
				+ URLParamConstants.PARAM_RESOURCE_TO_DISPLAY_ALL_RESOURCES,
				"on");
		properties.setProperty("" + URLParamConstants.PARAM_FILTER_GRID_TYPE,
				"on");
		
		int gridTypeID = URLParamConstants.getGridTypeID(mGrid);
		if( gridTypeID == -1 ){
			throw new SiteCatalogException("Unable to determine integer ID for grid " + mGrid);
		}
		properties.setProperty(""
				+ URLParamConstants.PARAM_FILTER_GRID_TYPE_OPTION, ""+ gridTypeID);
		properties.setProperty(""
				+ URLParamConstants.PARAM_FILTER_CURRENT_RSV_STATUS, "on");
		properties
				.setProperty(
						""
								+ URLParamConstants.PARAM_FILTER_CURRENT_RSV_STATUS_OPTION,
						"1");
		properties.setProperty("" + URLParamConstants.PARAM_FILTER_VO_SUPPORT,
				"on");
	
		int voID = URLParamConstants.getVOID( mVO );
		if( voID == -1 ){
			throw new SiteCatalogException("Unable to determine integer ID for VO " + mVO);
		}
		
		properties.setProperty(""+URLParamConstants.PARAM_FILTER_VO_SUPPORT_OPTION,"" + voID );
		properties.setProperty(""
				+ URLParamConstants.PARAM_FILTER_ACTIVE_STATUS_OPTION, "1");
		properties.setProperty(""
				+ URLParamConstants.PARAM_FILTER_DISABLE_STATUS_OPTION, "1");

		return properties;
	}

	/**
	 * Returns the start date formatted as MM/dd/yyyy.
	 * 
	 * @return
	 */
	private String getStartDate() {
		String now = null;
		try {
			now = URLEncoder.encode(DateUtils.now(DATE_FORMAT), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new SiteCatalogException("Problem encoding the date after", e);
		}
		return now;
	}

	/**
	 * Returns the date after n days formatted as MM/dd/yyyy.
	 * 
	 * @param day
	 *            the days after.
	 * @return
	 */
	private static String getDateAfter(int days) {
		String now = null;
		try {
			now = URLEncoder
					.encode(DateUtils.after(days, DATE_FORMAT), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new SiteCatalogException("Problem encoding the date after", e);
		}
		return now;
	}

	/**
	 * Returns if the connection is closed or not.
	 * 
	 * @return
	 */
	public boolean isClosed() {
		// TODO Auto-generated method stub
		return !mConnected;
	}

}
