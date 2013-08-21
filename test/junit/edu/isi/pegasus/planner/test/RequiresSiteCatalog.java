/**
 *  Copyright 2007-2013 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.isi.pegasus.planner.test;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.List;

/**
 * An interface that designates that a Test Requires a Site Catalog loaded.
 *
 * @author Karan 
 */
public interface RequiresSiteCatalog {

    
    /**
     * Loads up the SiteStore with the sites passed in list of sites.
     * 
     * @param props   the properties
     * @param logger  the logger
     * @param sites   the list of sites to load
     * 
     * @return the SiteStore
     */
    public SiteStore getSiteStore( PegasusProperties props , LogManager logger, List<String> sites );
    
}