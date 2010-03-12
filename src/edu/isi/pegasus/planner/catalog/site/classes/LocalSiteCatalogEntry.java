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
package edu.isi.pegasus.planner.catalog.site.classes;



/**
 * A convenience class that creates a default local site catalog entry
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class LocalSiteCatalogEntry{

    /**
     * Creates a default site catalog entry for site local with the VO 
     * set to pegasus.
     *
     * @return  SiteCatalogEntry for the local site.
     */
    public static SiteCatalogEntry create(){
        return create( "pegasus" , null );
    }
    
    
    /**
     * Creates a default site catalog entry for site local with the VO 
     * set to pegasus.
     *
     * @param vo    the VO to create the entry for.
     * @param grid  the grid to create entry for
     * 
     * @return SiteCatalogEntry for the local site.
     */
    public static SiteCatalogEntry create( String vo, String grid ){
        //always add local site.
        VORSSiteInfo siteInfo = VORSSiteCatalogUtil.getLocalSiteInfo( vo );
        VORSVOInfo local = new VORSVOInfo();
        local.setGrid( grid );
        siteInfo.setVoInfo( local );
        
        return VORSSiteCatalogUtil.createSiteCatalogEntry( siteInfo );
        
    }
}