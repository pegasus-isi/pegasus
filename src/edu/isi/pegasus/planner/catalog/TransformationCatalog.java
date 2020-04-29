/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.catalog;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import java.io.File;
import java.util.List;

/**
 * This class is an interface to the various TxCatalog implementations that Pegasus will use. It
 * defines the basic functionality for interfacing with various transformation Catalogs It defines
 * api's for the querying, adding and deleting transformation and associated mappings from the
 * implementing Tx Catalog By implementing this inteface a user can easily use his own TX Catalog
 * with Pegasus.
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @version $Revision$
 */
public interface TransformationCatalog extends edu.isi.pegasus.planner.catalog.Catalog {

    /** The version of the API */
    public static final String VERSION = "1.4";

    /** Prefix for the property subset to use with this catalog. */
    public static final String c_prefix = "pegasus.catalog.transformation";

    /**
     * The key that if set, specifies the file to be picked up to connect to transformation catalog
     * backend.
     */
    public static final String FILE_KEY = "file";

    /** Key name of property to set variable expansion */
    public static final String VARIABLE_EXPANSION_KEY = "expand";

    /** Property specify whether to modify file url or not. */
    public static final String MODIFY_FOR_FILE_URLS_KEY =
            "pegasus.catalog.transformation.modify.file.urls";

    /**
     * Initialize the implementation, and return an instance of the implementation.
     *
     * @param bag the bag of Pegasus initialization objects.
     */
    public void initialize(PegasusBag bag);

    /**
     * Returns TC entries for a particular logical transformation and/or on a particular resource
     * and/or of a particular type.
     *
     * @param namespace String The namespace of the logical transformation.
     * @param name String the name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param resourceid String The resourceid where the transformation is located. If <B>NULL</B>
     *     it returns all resources.
     * @param type TCType The type of the transformation to search for. If <B>NULL</b> it returns
     *     all types.
     * @return List Returns a list of TransformationCatalogEntry objects containing the
     *     corresponding entries from the TC. Returns null if no entry found.
     * @throws Exception
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public List<TransformationCatalogEntry> lookup(
            String namespace, String name, String version, String resourceid, TCType type)
            throws Exception;

    /**
     * Returns TC entries for a particular logical transformation and/or on a number of resources
     * and/or of a particular type.
     *
     * @param namespace String The namespace of the logical transformation.
     * @param name String the name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param resourceids List The List resourceid where the transformation is located. If
     *     <b>NULL</b> it returns all resources.
     * @param type TCType The type of the transformation to search for. If <b>NULL</b> it returns
     *     all types.
     * @return List Returns a list of TransformationCatalogEntry objects containing the
     *     corresponding entries from the TC. Returns null if no entry found.
     * @throws Exception
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public List<TransformationCatalogEntry> lookup(
            String namespace, String name, String version, List resourceids, TCType type)
            throws Exception;

    /**
     * Get the list of Resource ID's where a particular transformation may reside.
     *
     * @param namespace String The namespace of the transformation to search for.
     * @param name String The name of the transformation to search for.
     * @param version String The version of the transformation to search for.
     * @param type TCType The type of the transformation to search for.<br>
     *     (Enumerated type includes SOURCE, STATIC-BINARY, DYNAMIC-BINARY, PACMAN, INSTALLED,
     *     SCRIPT)<br>
     *     If <B>NULL</B> it returns all types.
     * @return List Returns a list of Resource Id's as strings. Returns <B>NULL</B> if no results
     *     found.
     * @throws Exception NotImplementedException if not implemented
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     */
    public List<String> lookupSites(String namespace, String name, String version, TCType type)
            throws Exception;

    /**
     * Get the list of PhysicalNames for a particular transformation on a site/sites for a
     * particular type/types;
     *
     * @param namespace String The namespace of the transformation to search for.
     * @param name String The name of the transformation to search for.
     * @param version String The version of the transformation to search for.
     * @param resourceid String The id of the resource on which you want to search. <br>
     *     If <B>NULL</B> then returns entries on all resources
     * @param type TCType The type of the transformation to search for. <br>
     *     (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<br>
     *     If <B>NULL</B> then returns entries of all types.
     * @return List Returns a List of <TransformationCatalongEntry> objects with the profiles not
     *     populated.
     * @throws Exception NotImplementedException if not implemented.
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     */
    public List<TransformationCatalogEntry> lookupNoProfiles(
            String namespace, String name, String version, String resourceid, TCType type)
            throws Exception;

    /**
     * Get the list of LogicalNames available on a particular resource.
     *
     * @param resourceid String The id of the resource on which you want to search
     * @param type TCType The type of the transformation to search for. <br>
     *     (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<br>
     *     If <B>NULL</B> then return logical name for all types.
     * @return List Returns a list of String Arrays. Each array contains the resourceid, logical
     *     transformation in the format namespace::name:version and type. Returns <B>NULL</B> if no
     *     results found.
     * @throws Exception NotImplementedException if not implemented.
     */
    public List<String[]> getTCLogicalNames(String resourceid, TCType type) throws Exception;

    /**
     * Get the list of Profiles associated with a particular logical transformation.
     *
     * @param namespace String The namespace of the transformation to search for.
     * @param name String The name of the transformation to search for.
     * @param version String The version of the transformation to search for.
     * @return List Returns a list of Profile Objects containing profiles assocaited with the
     *     transformation. Returns <B>NULL</B> if no profiles found.
     * @throws Exception NotImplementedException if not implemented.
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public List<Profile> lookupLFNProfiles(String namespace, String name, String version)
            throws Exception;

    /**
     * Get the list of Profiles associated with a particular physical transformation.
     *
     * @param pfn The physical file name to search the transformation by.
     * @param resourceid String The id of the resource on which you want to search.
     * @param type TCType The type of the transformation to search for. <br>
     *     (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<br>
     * @throws Exception NotImplementedException if not implemented.
     * @return List Returns a list of Profile Objects containing profiles assocaited with the
     *     transformation. Returns <B>NULL</B> if no profiless found.
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public List<Profile> lookupPFNProfiles(String pfn, String resourceid, TCType type)
            throws Exception;

    /**
     * List all the contents of the TC
     *
     * @return List Returns a List of TransformationCatalogEntry objects.
     * @throws Exception
     */
    public List<TransformationCatalogEntry> getContents() throws Exception;

    /** ADDITIONS */

    /**
     * Add multiple TCEntries to the Catalog.
     *
     * @param tcentry List Takes a list of TransformationCatalogEntry objects as input
     * @throws Exception
     * @return number of insertions On failure,throw an exception, don't use zero.
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public int insert(List<TransformationCatalogEntry> tcentry) throws Exception;

    /**
     * Add single TCEntry to the Catalog.
     *
     * @param tcentry Takes a single TransformationCatalogEntry object as input
     * @throws Exception
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public int insert(TransformationCatalogEntry tcentry) throws Exception;

    /**
     * Add single TCEntry object temporarily to the in memory Catalog. This is a hack to get around
     * for adding soft state entries to the TC
     *
     * @param tcentry Takes a single TransformationCatalogEntry object as input
     * @param write boolean enable write commits to backed catalog or not.
     * @throws Exception
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public int insert(TransformationCatalogEntry tcentry, boolean write) throws Exception;

    /**
     * Add an single entry into the transformation catalog.
     *
     * @param namespace String The namespace of the transformation to be added (Can be null)
     * @param name String The name of the transformation to be added.
     * @param version String The version of the transformation to be added. (Can be null)
     * @param physicalname String The physical name/location of the transformation to be added.
     * @param type TCType The type of the physical transformation.
     * @param resourceid String The resource location id where the transformation is located.
     * @param lfnprofiles List The List of Profile objects associated with a Logical Transformation.
     *     (can be null)
     * @param pfnprofiles List The List of Profile objects associated with a Physical
     *     Transformation. (can be null)
     * @param sysinfo SysInfo The System information associated with a physical transformation.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @throws Exception
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     * @see edu.isi.pegasus.planner.catalog.classes.SysInfo
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public int insert(
            String namespace,
            String name,
            String version,
            String physicalname,
            TCType type,
            String resourceid,
            List lfnprofiles,
            List pfnprofiles,
            SysInfo sysinfo)
            throws Exception;

    /**
     * Add additional profile to a logical transformation .
     *
     * @param namespace String The namespace of the transformation to be added. (can be null)
     * @param name String The name of the transformation to be added.
     * @param version String The version of the transformation to be added. (can be null)
     * @param profiles List The List of Profile objects that are to be added to the transformation.
     * @return number of insertions. On failure, throw an exception, don't use zero.
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public int addLFNProfile(String namespace, String name, String version, List profiles)
            throws Exception;

    /**
     * Add additional profile to a physical transformation.
     *
     * @param pfn String The physical name of the transformation
     * @param type TCType The type of transformation that the profile is associated with.
     * @param resourcename String The resource on which the physical transformation exists
     * @param profiles The List of Profile objects that are to be added to the transformation.
     * @return number of insertions. On failure, throw an exception, don't use zero.
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public int addPFNProfile(String pfn, TCType type, String resourcename, List profiles)
            throws Exception;

    /** DELETIONS */

    /**
     * Delete all entries in the transformation catalog for a give logical transformation and/or on
     * a resource and/or of a particular type
     *
     * @param namespace String The namespace of the transformation to be deleted. (can be null)
     * @param name String The name of the transformation to be deleted.
     * @param version String The version of the transformation to be deleted. ( can be null)
     * @param resourceid String The resource id for which the transformation is to be deleted. If
     *     <B>NULL</B> then transformation on all resource are deleted
     * @param type TCType The type of the transformation. If <B>NULL</B> then all types are deleted
     *     for the transformation.
     * @throws Exception
     * @return the number of removed entries.
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     */
    public int removeByLFN(
            String namespace, String name, String version, String resourceid, TCType type)
            throws Exception;

    /**
     * Delete all entries in the transformation catalog for pair of logical and physical
     * transformation.
     *
     * @param physicalname String The physical name of the transformation
     * @param namespace String The namespace associated in the logical name of the transformation.
     * @param name String The name of the logical transformation.
     * @param version String The version number of the logical transformation.
     * @param resourceid String The resource on which the transformation is to be deleted. If
     *     <B>NULL</B> then it searches all the resource id.
     * @param type TCType The type of transformation. If <B>NULL</B> then it search and deletes
     *     entries for all types.
     * @throws Exception
     * @return the number of removed entries.
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     */
    public int removeByPFN(
            String physicalname,
            String namespace,
            String name,
            String version,
            String resourceid,
            TCType type)
            throws Exception;

    /**
     * Delete a particular type of transformation, and/or on a particular resource
     *
     * @param type TCType The type of the transformation
     * @param resourceid String The resource on which the transformation exists. If <B>NULL</B> then
     *     that type of transformation is deleted from all the resources.
     * @throws Exception
     * @return the number of removed entries.
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     */
    public int removeByType(TCType type, String resourceid) throws Exception;

    /**
     * Delete all entries on a particular resource from the transformation catalog.
     *
     * @param resourceid String The resource which you want to remove.
     * @throws Exception
     * @return the number of removed entries.
     */
    public int removeBySiteID(String resourceid) throws Exception;

    /**
     * Deletes entries from the catalog which have a particular system information.
     *
     * @param sysinfo SysInfo The System Information by which you want to delete
     * @return the number of removed entries.
     * @see edu.isi.pegasus.planner.catalog.classes.SysInfo
     * @throws Exception
     */
    public int removeBySysInfo(SysInfo sysinfo) throws Exception;

    /**
     * Deletes the entire transformation catalog. CLEAN............. USE WITH CAUTION.
     *
     * @return the number of removed entries.
     * @throws Exception
     */
    public int clear() throws Exception;

    /**
     * Delete a list of profiles or all the profiles associated with a pfn on a resource and of a
     * type.
     *
     * @param physicalname String The physical name of the transformation.
     * @param type TCType The type of the transformation.
     * @param resourceid String The resource of the transformation.
     * @param profiles List The list of profiles to be deleted. If <B>NULL</B> then all profiles for
     *     that pfn+resource+type are deleted.
     * @return the number of removed entries.
     * @see org.griphyn.cPlanner.classes.Profile
     * @throws Exception
     */
    public int deletePFNProfiles(String physicalname, TCType type, String resourceid, List profiles)
            throws Exception;

    /**
     * Delete a list of profiles or all the profiles associated with a logical transformation.
     *
     * @param namespace String The namespace of the logical transformation.
     * @param name String The name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param profiles List The List of profiles to be deleted. If <B>NULL</B> then all profiles for
     *     the logical transformation are deleted.
     * @return the number of removed entries.
     * @see org.griphyn.cPlanner.classes.Profile
     * @throws Exception
     */
    public int deleteLFNProfiles(String namespace, String name, String version, List profiles)
            throws Exception;

    /**
     * Returns the TC implementation being used
     *
     * @return String
     */
    public String getDescription();

    /**
     * Returns the file source.
     *
     * @return the file source if it exists , else null
     */
    public File getFileSource();
}
