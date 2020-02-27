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

package edu.isi.pegasus.planner.catalog;

import edu.isi.pegasus.planner.catalog.site.*;
import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * @author Karan Vahi
 * @version $Revision$
 */
public interface SiteCatalog extends Catalog {

    /** The version of the API */
    public static final String VERSION = "1.1";

    /** Prefix for the property subset to use with this catalog. */
    public static final String c_prefix = "pegasus.catalog.site";

    /** Key name of property to set variable expansion */
    public static final String VARIABLE_EXPANSION_KEY = "expand";

    /**
     * Loads up the Site Catalog implementation with the sites whose site handles are specified.
     * This is a convenience method, that can allow the backend implementations to maintain soft
     * state if required.
     *
     * <p>If the implementation chooses not to implement this, just do an empty implementation.
     *
     * <p>The site handle * is a special handle designating all sites are to be loaded.
     *
     * @param sites the list of sites to be loaded.
     * @return the number of sites loaded.
     * @throws SiteCatalogException in case of error.
     */
    public int load(List<String> sites) throws SiteCatalogException;

    /**
     * Inserts a new mapping into the Site catalog.
     *
     * @param entry the <code>SiteCatalogEntry</code> object that describes a site.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @throws SiteCatalogException in case of error.
     */
    public int insert(SiteCatalogEntry entry) throws SiteCatalogException;

    /**
     * Lists the site handles for all the sites in the Site Catalog.
     *
     * @return A set of site handles.
     * @throws SiteCatalogException in case of error.
     */
    public Set<String> list() throws SiteCatalogException;

    /**
     * Retrieves the <code>SiteCatalogEntry</code> for a site.
     *
     * @param handle the site handle / identifier.
     * @return SiteCatalogEntry in case an entry is found , or <code>null</code> if no match is
     *     found.
     * @throws SiteCatalogException in case of error.
     */
    public SiteCatalogEntry lookup(String handle) throws SiteCatalogException;

    /**
     * Removes a site catalog entry matching the the handle.
     *
     * @param handle the site handle / identifier.
     * @return the number of removed entries.
     * @throws SiteCatalogException in case of error.
     */
    public int remove(String handle) throws SiteCatalogException;

    /**
     * Returns the File Source for the Site Catalog
     *
     * @return path to the backend catalog file , else null
     */
    public File getFileSource();
}
