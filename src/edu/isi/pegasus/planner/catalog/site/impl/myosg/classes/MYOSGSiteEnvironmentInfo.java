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
package edu.isi.pegasus.planner.catalog.site.impl.myosg.classes;

import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGSiteConstants;

/**
 * This class extends AbstractSiteCatalogResource and stores MYOSG Site
 * Environment Information
 * 
 * @author prasanth
 * 
 */
public class MYOSGSiteEnvironmentInfo extends AbstractSiteCatalogResource {

	private String envGlobusLocation = "";
	private String envApp = "";
	private String envData = "";
	private String envDefaultSe = "";
	private String envGlexecLocation = "";
	private String envGrid = "";
	private String envHostName = "";
	private String envJobContact = "";
	private String envLocation = "";
	private String envSiteName = "";
	private String envSiteRead = "";
	private String envSiteWrite = "";
	private String envSquidLocation = "";
	private String envStorageElement = "";
	private String envWnTmp = "";

	private static final String ENV_GLOBUS_LOCATION_TAG = "GLOBUS_LOCATION";
	private static final String ENV_APP_TAG = "OSG_APP";
	private static final String ENV_DATA_TAG = "OSG_DATA";
	private static final String ENV_DEFAULT_SE_TAG = "OSG_DEFAULT_SE";
	private static final String ENV_GLEXEC_LOCATION_TAG = "OSG_GLEXEC_LOCATION";
	private static final String ENV_GRID_TAG = "OSG_GRID";
	private static final String ENV_HOSTNAME_TAG = "OSG_HOSTNAME";
	private static final String ENV_JOB_CONTACT_TAG = "OSG_JOB_CONTACT";
	private static final String ENV_LOCATION_TAG = "OSG_LOCATION";
	private static final String ENV_SITE_NAME_TAG = "OSG_SITE_NAME";
	private static final String ENV_SITE_READ_TAG = "OSG_SITE_READ";
	private static final String ENV_SITE_WRITE_TAG = "OSG_SITE_WRITE";
	private static final String ENV_SQUID_LOCATION_TAG = "OSG_SQUID_LOCATION";
	private static final String ENV_STORAGE_ELEMENT_TAG = "OSG_STORAGE_ELEMENT";
	private static final String ENV_WN_TMP_TAG = "OSG_WN_TMP";

	public MYOSGSiteEnvironmentInfo(int depth) {
		setDepth(depth);
	}

	/**
	 * Returns the property value
	 * 
	 * @param ID
	 * @return propety value
	 */
	public Object getProperty(int ID) {
		switch (ID) {
		case MYOSGSiteConstants.ENV_GLOBUS_LOCATION_ID:
			return envGlobusLocation;

		case MYOSGSiteConstants.ENV_APP_ID:
			return envApp;

		case MYOSGSiteConstants.ENV_DATA_ID:
			return envData;

		case MYOSGSiteConstants.ENV_DEFAULT_SE_ID:
			return envDefaultSe;

		case MYOSGSiteConstants.ENV_GLEXEC_LOCATION_ID:
			return envGlexecLocation;

		case MYOSGSiteConstants.ENV_GRID_ID:
			return envGrid;

		case MYOSGSiteConstants.ENV_HOSTNAME_ID:
			return envHostName;

		case MYOSGSiteConstants.ENV_JOB_CONTACT_ID:
			return envJobContact;

		case MYOSGSiteConstants.ENV_LOCATION_ID:
			return envLocation;

		case MYOSGSiteConstants.ENV_SITE_NAME_ID:
			return envSiteName;

		case MYOSGSiteConstants.ENV_SITE_READ_ID:
			return envSiteRead;

		case MYOSGSiteConstants.ENV_SITE_WRITE_ID:
			return envSiteWrite;

		case MYOSGSiteConstants.ENV_SQUID_LOCATION_ID:
			return envSquidLocation;

		case MYOSGSiteConstants.ENV_STORAGE_ELEMENT_ID:
			return envStorageElement;

		case MYOSGSiteConstants.ENV_WN_TMP_ID:
			return envWnTmp;

		}

		return super.getProperty(ID);
	}

	/**
	 * Sets the property of Site Catalog resource
	 * 
	 * @param ID
	 *            property ID
	 * @param value
	 *            property value
	 */
	public void setProperty(String ID, Object value) {
		if (ID.equals(ENV_GLOBUS_LOCATION_TAG)) {
			envGlobusLocation = (String) value;
		} else if (ID.equals(ENV_APP_TAG)) {
			envApp = (String) value;
		} else if (ID.equals(ENV_DATA_TAG)) {
			envData = (String) value;
		} else if (ID.equals(ENV_DEFAULT_SE_TAG)) {
			envDefaultSe = (String) value;
		} else if (ID.equals(ENV_GLEXEC_LOCATION_TAG)) {
			envGlexecLocation = (String) value;
		} else if (ID.equals(ENV_GRID_TAG)) {
			envGrid = (String) value;
		} else if (ID.equals(ENV_HOSTNAME_TAG)) {
			envHostName = (String) value;
		} else if (ID.equals(ENV_JOB_CONTACT_TAG)) {
			envJobContact = (String) value;
		} else if (ID.equals(ENV_LOCATION_TAG)) {
			envLocation = (String) value;
		} else if (ID.equals(ENV_SITE_NAME_TAG)) {
			envSiteName = (String) value;
		} else if (ID.equals(ENV_SITE_READ_TAG)) {
			envSiteRead = (String) value;
		} else if (ID.equals(ENV_SITE_WRITE_TAG)) {
			envSiteWrite = (String) value;
		} else if (ID.equals(ENV_SQUID_LOCATION_TAG)) {
			envSquidLocation = (String) value;
		} else if (ID.equals(ENV_STORAGE_ELEMENT_TAG)) {
			envStorageElement = (String) value;
		} else if (ID.equals(ENV_WN_TMP_TAG)) {
			envWnTmp = (String) value;
		}

	}

	/**
	 * Add child resources to a site catalog resource
	 * 
	 * @param ID  child resource
	 */
	public void addChildResource(AbstractSiteCatalogResource ID) {

	}

	public String toString() {
		return "Environment Info :-  " + envApp;
	}
}
