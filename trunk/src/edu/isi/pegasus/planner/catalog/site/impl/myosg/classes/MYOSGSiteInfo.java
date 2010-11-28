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

import java.util.ArrayList;
import java.util.List;

import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGSiteConstants;
/**
 * This class extends AbstractSiteCatalogResource and stores MYOSG Site Information
 * @author prasanth
 *
 */
public class MYOSGSiteInfo extends AbstractSiteCatalogResource {
	
	private  String siteName ="";
	private List<MYOSGSiteResourceInfo> resourceList;
	
	public MYOSGSiteInfo(int depth) {
		resourceList = new ArrayList<MYOSGSiteResourceInfo>();
		setDepth(depth);
	}
	
	private static final String SITE_NAME_TAG ="GroupName";
	
	
	/**
	 * Returns the property value
	 * @param ID
	 * @return property value
	 */
	public Object getProperty(int ID) {
		switch (ID) {
		case MYOSGSiteConstants.SITE_NAME_ID:
			return siteName;
		case MYOSGSiteConstants.RESOURCE_LIST_ID:
			return resourceList;
		}
		return super.getProperty(ID);
	}

	/**
	 * Sets the property of Site Catalog resource
	 * @param ID property ID
	 * @param value property value
	 */
	public void setProperty(String ID, Object value) {
		if(ID.equals(SITE_NAME_TAG)){
			siteName = (String)value;
		}
	}

	/**
	 * Add child resources to a site catalog resource
	 * @param childResource child resource
	 */
	public void addChildResource(AbstractSiteCatalogResource childResource) {
		if(childResource instanceof MYOSGSiteResourceInfo)
			resourceList.add((MYOSGSiteResourceInfo)childResource);
		
	}
	
	public String toString(){
		
		String info = "Site :- " + siteName;
		for(int i =0 ;i < resourceList.size();i++){
			info += "\n"+resourceList.get(i);
		}
		return info ;
	}

}
