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
public class MYOSGSiteVOOwnershipInfo extends AbstractSiteCatalogResource{
	
	
	
	private static final String VO_OWN_PERCENT_TAG ="Percent";
	private static final String VO_OWN_VO_TAG ="VO";
	
	private String percent ="";
	private String vo="";
	
	public MYOSGSiteVOOwnershipInfo(int depth){
		setDepth(depth);
	}
	
	/**
	 * Returns the property value
	 * @param ID
	 * @return property value
	 */
	public Object getProperty(int ID) {
		switch(ID){
		case MYOSGSiteConstants.VO_OWN_PERCENT_ID:
			return percent ;
		case MYOSGSiteConstants.VO_OWN_VO_ID:
			return vo;
			
	
		}
		return super.getProperty(ID);
	}
	/**
	 * Sets the property of Site Catalog resource
	 * @param ID property ID
	 * @param value property value
	 */
	public void setProperty(String ID, Object value) {
		if(ID.equals(VO_OWN_PERCENT_TAG)){
			percent = (String)value;
		}else if(ID.equals(VO_OWN_VO_TAG)){
			vo = (String)value;
			
		}
		
		
	}

	/**
	 * Add child resources to a site catalog resource
	 * @param childResource child resource
	 */
	public void addChildResource(AbstractSiteCatalogResource childResource) {
		
	}
	
	public String toString(){
		String info ="VO OWNERSHIP :- "+ percent +" , "+ vo ;
		return info;
	}

}
