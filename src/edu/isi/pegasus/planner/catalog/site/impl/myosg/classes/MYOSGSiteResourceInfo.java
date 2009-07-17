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
 * This class extends AbstractSiteCatalogResource and stores MYOSG Site resource Information
 * @author prasanth
 *
 */
public class MYOSGSiteResourceInfo  extends AbstractSiteCatalogResource{

	private static final String RESOURCE_ID_TAG ="ID";
	private static final String RESOURCE_NAME_TAG ="Name";
	private static final String RESOURCE_DESCRIPTION_TAG ="Description";
	
	private String resourceID ="";
	private String resourceName ="";
	private String resourceDescription="";
	private List<MYOSGSiteServiceInfo> serviceList;
	private MYOSGSiteEnvironmentInfo environmentInfo;
	private MYOSGSiteVOOwnershipInfo VOOwnershipInfo;
	
	public MYOSGSiteResourceInfo(int depth){
		serviceList = new ArrayList<MYOSGSiteServiceInfo>();
		setDepth(depth);
	}
	
	
	
	/**
	 * Returns the property value
	 * @param ID
	 * @return property value
	 */
	public Object getProperty(int ID) {
		switch(ID){
		case MYOSGSiteConstants.RESOURCE_ID_ID:
			return resourceID ;
		case MYOSGSiteConstants.RESOURCE_NAME_ID:
			return resourceName;
		case MYOSGSiteConstants.RESOURCE_DESCRIPTION_ID:
			return resourceDescription ;
			
		case MYOSGSiteConstants.SERVICE_LIST_ID:
			return serviceList ;
		case MYOSGSiteConstants.ENVIRONMENT_INFO_ID:
			return environmentInfo ;
		case MYOSGSiteConstants.VO_OWNERSHIP_INFO_ID:
			return VOOwnershipInfo ;
		}
		return super.getProperty(ID);
	}

	
	/**
	 * Sets the property of Site Catalog resource
	 * @param ID property ID
	 * @param value property value
	 */
	public void setProperty(String ID, Object value) {
		if(ID.equals(RESOURCE_ID_TAG)){
			resourceID = (String)value;
		}else if(ID.equals(RESOURCE_NAME_TAG)){
			resourceName = (String)value;
			
		}else if(ID.equals(RESOURCE_DESCRIPTION_TAG)){
			resourceDescription = (String)value;
		}		
		
	}

	/**
	 * Add child resources to a site catalog resource
	 * @param childResource child resource
	 */
	public void addChildResource(AbstractSiteCatalogResource childResource) {
		if(childResource instanceof MYOSGSiteServiceInfo){
			serviceList.add((MYOSGSiteServiceInfo)childResource);
		}
		else if(childResource instanceof MYOSGSiteEnvironmentInfo){
			environmentInfo = (MYOSGSiteEnvironmentInfo)childResource;
		}
		else if(childResource instanceof MYOSGSiteVOOwnershipInfo){
			VOOwnershipInfo = (MYOSGSiteVOOwnershipInfo)childResource;
		}
	}
	
	public String toString(){
		String info ="Resource :- "+ resourceID +" , "+ resourceName +" , " + resourceDescription;
		for(int i =0 ;i < serviceList.size();i++){
			info += "\n"+serviceList.get(i);
		}
		info +="\n"+environmentInfo;
		info +="\n"+VOOwnershipInfo;
		return info;
	}

}
