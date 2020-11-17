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

package edu.isi.pegasus.planner.parser;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

/**
 * An empty interface for Site Catalog XML parsers
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface SiteCatalogXMLParser {

    /**
     * Returns the constructed site store object
     *
     * @return <code>SiteStore<code> if parsing completed
     */
    public SiteStore getSiteStore();
}
