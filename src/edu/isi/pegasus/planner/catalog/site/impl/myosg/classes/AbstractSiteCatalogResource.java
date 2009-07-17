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

/**
 * This class defines a AbstractSiteCatalogResoure type.
 * @author prasanth
 *
 */
public abstract class AbstractSiteCatalogResource {
	protected int mDepth;
	
	/**
	 * Add child resources to a site catalog resource
	 * @param childResource child resource
	 */
	abstract public void addChildResource(AbstractSiteCatalogResource childResource);
	public void setDepth(int depth){
		mDepth = depth;
	}
	public int getDepth(){
		return mDepth;
	}
	
	/**
	 * Sets the property of Site Catalog resource
	 * @param ID property ID
	 * @param value property value
	 */
	public void setProperty(String ID , Object value){
		
	}
	/**
	 * Returns the property value
	 * @param ID
	 * @return
	 */
	public Object getProperty(int ID){
		return null;
	}
	
}
