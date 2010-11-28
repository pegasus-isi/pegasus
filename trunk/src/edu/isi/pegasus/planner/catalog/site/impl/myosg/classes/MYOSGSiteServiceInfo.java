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
 * This class extends AbstractSiteCatalogResource and stores MYOSG Site service Information 
 * @author prasanth
 *
 */
public class MYOSGSiteServiceInfo extends AbstractSiteCatalogResource{
	
	
	
	private static final String SERVICE_ID_TAG ="ID";
	private static final String SERVICE_NAME_TAG ="Name";
	private static final String SERVICE_DESCRIPTION_TAG ="Description";
	private static final String SERVICE_URI_TAG ="ServiceUri";
	
	private String serviceID ="";
	private String serviceName="";
	private String serviceDescription="";
	private String serviceURI="";
	
	public MYOSGSiteServiceInfo(int depth){
		setDepth(depth);
	}
	
	/**
	 * Returns the property value
	 * @param ID
	 * @return property value
	 */
	public Object getProperty(int ID) {
		switch(ID){
		case MYOSGSiteConstants.SERVICE_ID_ID:
			return serviceID ;
		case MYOSGSiteConstants.SERVICE_NAME_ID:
			return serviceName;
			
		case MYOSGSiteConstants.SERVICE_DESCRIPTION_ID:
			return serviceDescription;
			
		case MYOSGSiteConstants.SERVICE_URI_ID:
			return serviceURI;
		}
		return super.getProperty(ID);
	}
	/**
	 * Sets the property of Site Catalog resource
	 * @param ID property ID
	 * @param value property value
	 */
	public void setProperty(String ID, Object value) {
		if(ID.equals(SERVICE_ID_TAG)){
			serviceID = (String)value;
		}else if(ID.equals(SERVICE_NAME_TAG)){
			serviceName = (String)value;
			
		}else if(ID.equals(SERVICE_DESCRIPTION_TAG)){
			serviceDescription = (String)value;
			
		}else if(ID.equals(SERVICE_URI_TAG)){
			serviceURI = (String)value;
			
		}
		
		
	}

	/**
	 * Add child resources to a site catalog resource
	 * @param childResource child resource
	 */
	public void addChildResource(AbstractSiteCatalogResource childResource) {
		
	}
	
	public String toString(){
		String info ="Service :- "+ serviceID +" , "+ serviceName +" , " + serviceDescription +" , " + serviceURI;
		return info;
	}

}
