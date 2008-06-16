/**
 *  Copyright 2007-2008 University Of Southern California
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

package org.griphyn.common.catalog;

import org.griphyn.common.classes.SysInfo;
import org.griphyn.common.classes.TCType;

import java.util.List;

/**
 * This class is an interface to the various TxCatalog implementations that Pegasus will use.
 * It defines the basic functionality for interfacing with various transformation Catalogs
 * It defines api's for the querying, adding and deleting transformation and associated mappings from the implementing Tx Catalog
 * By implementing this inteface a user can easily use his own TX Catalog with Pegasus.
 *
 *
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @version $Revision$
 */
public interface TransformationCatalog
    extends org.griphyn.common.catalog.Catalog {

    /**
     * Returns TC entries for a particular logical transformation and/or on a particular resource and/or of a particular type.
     * @param namespace String The namespace of the logical transformation.
     * @param name String the name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param resourceid String The resourceid where the transformation is located. If <B>NULL</B> it returns all resources.
     * @param type TCType The type of the transformation to search for. If <B>NULL</b> it returns all types.
     * @return List Returns a list of TransformationCatalogEntry objects containing the corresponding entries from the TC. Returns null if no entry found.
     * @throws Exception
     * @see org.griphyn.common.classes.TCType
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    List getTCEntries( String namespace, String name, String version,
        String resourceid, TCType type ) throws Exception;

    /**
     * Returns TC entries for a particular logical transformation and/or on a number of resources and/or of a particular type.
     * @param namespace String The namespace of the logical transformation.
     * @param name String the name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param resourceids List The List resourceid where the transformation is located. If <b>NULL</b> it returns all resources.
     * @param type TCType The type of the transformation to search for. If <b>NULL</b> it returns all types.
     * @return List Returns a list of TransformationCatalogEntry objects containing the corresponding entries from the TC. Returns null if no entry found.
     * @throws Exception
     * @see org.griphyn.common.classes.TCType
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    List getTCEntries( String namespace, String name, String version,
        List resourceids, TCType type ) throws Exception;

    /**
     * Get the list of Resource ID's where a particular transformation may reside.
     * @param   namespace String The namespace of the transformation to search for.
     * @param   name      String The name of the transformation to search for.
     * @param   version   String The version of the transformation to search for.
     * @param   type      TCType The type of the transformation to search for.<BR>
     *                    (Enumerated type includes SOURCE, STATIC-BINARY, DYNAMIC-BINARY, PACMAN, INSTALLED, SCRIPT)<BR>
     *                     If <B>NULL</B> it returns all types.
     * @return  List      Returns a list of Resource Id's as strings. Returns <B>NULL</B> if no results found.
     * @throws  Exception NotImplementedException if not implemented
     * @see org.griphyn.common.classes.TCType
     */
    List getTCResourceIds( String namespace, String name, String version,
        TCType type ) throws Exception;

    /**
     * Get the list of PhysicalNames for a particular transformation on a site/sites for a particular type/types;
     * @param  namespace  String The namespace of the transformation to search for.
     * @param  name       String The name of the transformation to search for.
     * @param  version    String The version of the transformation to search for.
     * @param  resourceid String The id of the resource on which you want to search. <BR>
     *                    If <B>NULL</B> then returns entries on all resources
     * @param  type       TCType The type of the transformation to search for. <BR>
     *                        (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<BR>
     *                        If <B>NULL</B> then returns entries of all types.
     * @return List       Returns a list of String Arrays.
     *                    Each array contains the resourceid, the physical transformation, the type of the tr and the systeminfo.
     *                    The last entry in the List is a int array containing the column lengths for pretty print.
     *                    Returns <B>NULL</B> if no results found.
     * @throws Exception  NotImplementedException if not implemented.
     * @see org.griphyn.common.classes.TCType
     * @see org.griphyn.common.classes.SysInfo
     */
    List getTCPhysicalNames( String namespace, String name,
        String version,
        String resourceid, TCType type ) throws
        Exception;

    /**
     * Get the list of LogicalNames available on a particular resource.
     * @param resourceid String The id of the resource on which you want to search
     * @param type       TCType The type of the transformation to search for. <BR>
     *                  (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<BR>
     *                   If <B>NULL</B> then return logical name for all types.
     * @return List      Returns a list of String Arrays.
     *                   Each array contains the resourceid, logical transformation
     *                   in the format namespace::name:version and type.
     *                   The last entry in the list is an array of integers specifying the column length for pretty print.
     *                   Returns <B>NULL</B> if no results found.
     * @throws Exception  NotImplementedException if not implemented.
     */
    List getTCLogicalNames( String resourceid, TCType type ) throws
        Exception;

    /**
     * Get the list of Profiles associated with a particular logical transformation.
     * @param namespace  String The namespace of the transformation to search for.
     * @param name       String The name of the transformation to search for.
     * @param version    String The version of the transformation to search for.
     * @return List      Returns a list of Profile Objects containing profiles assocaited with the transformation.
     *                   Returns <B>NULL</B> if no profiles found.
     * @throws Exception NotImplementedException if not implemented.
     * @see org.griphyn.cPlanner.classes.Profile
     */
    List getTCLfnProfiles( String namespace, String name, String version ) throws
        Exception;

    /**
     * Get the list of Profiles associated with a particular physical transformation.
     * @param pfn        The physical file name to search the transformation by.
     * @param resourceid String The id of the resource on which you want to search.
     * @param type       TCType The type of the transformation to search for. <br>
     *                   (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<br>
     * @throws Exception NotImplementedException if not implemented.
     * @return List      Returns a list of Profile Objects containing profiles assocaited with the transformation.
     *                   Returns <B>NULL</B> if no profiless found.
     * @see org.griphyn.cPlanner.classes.Profile
     */
    List getTCPfnProfiles( String pfn, String resourceid, TCType type ) throws
        Exception;

    /**
     * List all the contents of the TC
     *
     * @return List Returns a List of TransformationCatalogEntry objects.
     * @throws Exception
     */
    List getTC() throws Exception;

    /**
     *  ADDITIONS
     */

    /**
     * Add multiple TCEntries to the Catalog.
     * @param tcentry List Takes a list of TransformationCatalogEntry objects as input
     * @throws Exception
     * @return boolean Return true if succesful, false if error. Exception is thrown when error occurs.
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    boolean addTCEntry( List tcentry ) throws Exception;


    /**
     * Add single TCEntry to the Catalog.
     * @param tcentry Takes a single TransformationCatalogEntry object as input
     * @throws Exception
     * @return boolean Return true if succesful, false if error. Exception is thrown when error occurs.
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    boolean addTCEntry( TransformationCatalogEntry tcentry ) throws Exception;


    /**
     * Add single TCEntry object temporarily to the in memory Catalog.
     * This is a hack to get around for adding soft state entries to the TC
     * @param tcentry Takes a single TransformationCatalogEntry object as input
     * @param write boolean enable write commits to backed catalog or not.
     * @throws Exception
     * @return boolean Return true if succesful, false if error. Exception is thrown when error occurs.
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    boolean addTCEntry( TransformationCatalogEntry tcentry,boolean write) throws Exception;

    /**
     * Add an single entry into the transformation catalog.
     *
     * @param namespace    String The namespace of the transformation to be added (Can be null)
     * @param name         String The name of the transformation to be added.
     * @param version      String The version of the transformation to be added. (Can be null)
     * @param physicalname String The physical name/location of the transformation to be added.
     *  @param type        TCType  The type of the physical transformation.
     * @param resourceid   String The resource location id where the transformation is located.
     * @param lfnprofiles     List   The List of Profile objects associated with a Logical Transformation. (can be null)
     * @param pfnprofiles     List   The List of Profile objects associated with a Physical Transformation. (can be null)
     * @param sysinfo     SysInfo  The System information associated with a physical transformation.
     * @throws Exception
     * @return boolean   Returns true if succesfully added, returns false if error and throws exception.
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     * @see org.griphyn.common.classes.SysInfo
     * @see org.griphyn.cPlanner.classes.Profile
     */
    boolean addTCEntry( String namespace, String name, String version,
        String physicalname, TCType type,
        String resourceid,
        List lfnprofiles, List pfnprofiles,
        SysInfo sysinfo ) throws
        Exception;

    /**
     *Add additional profile to a logical transformation .
     * @param namespace String The nsamespace of the transformation to be added. (can be null)
     * @param name      String The name of the transformation to be added.
     * @param version   String The version of the transformation to be added. (can be null)
     * @param profiles  List   The List of Profile objects that are to be added to the transformation.
     * @return boolean Returns true if success, false if error.
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.Profile
     */
    boolean addTCLfnProfile( String namespace, String name,
        String version,
        List profiles ) throws Exception;

    /**
     * Add additional profile to a physical transformation.
     * @param pfn  String       The physical name of the transformation
     * @param type      TCType The type of transformation that the profile is associated with.
     * @param resourcename String The resource on which the physical transformation exists
     * @param profiles  List   The List of Profile objects that are to be added to the transformation.
     * @return boolean Returns true for success, false for error.
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.Profile
     */
    boolean addTCPfnProfile( String pfn, TCType type, String resourcename,
        List profiles ) throws Exception;

    /**
     * DELETIONS
     */

    /**
     * Delete all entries in the transformation catalog for a give logical tranformation and/or on a resource and/or of
     * a particular type
     * @param namespace   String The namespace of the transformation to be added. (can be null)
     * @param name        String The name of the transformation to be added.
     * @param version     String The version of the transformation to be added. ( can be null)
     * @param resourceid String The resource id for which the transformation is to be deleted.
     *                          If <B>NULL</B> then transformation on all resource are deleted
     * @param type TCType The type of the transformation. If <B>NULL</B> then all types are deleted for the transformation.
     * @throws Exception
     * @return boolean Returns true if success , false if there is any error.
     * @see org.griphyn.common.classes.TCType
     */
    boolean deleteTCbyLogicalName( String namespace, String name,
        String version, String resourceid,
        TCType type ) throws Exception;

    /**
     * Delete all entries in the transformation catalog for pair of logical and physical transformation.
     * @param physicalname String The physical name of the transformation
     * @param namespace String The namespace assocaited in the logical name of the transformation.
     * @param name String The name of the logical transformation.
     * @param version String The version number of the logical transformation.
     * @param resourceid String The resource on which the transformation is to be deleted.
     *                          If <B>NULL</B> then it searches all the resource id.
     * @param type TCType The type of transformation. If <B>NULL</B> then it search and deletes entries for all types.
     * @throws Exception
     * @return boolean Returns true if sucess, false if any error occurs.
     * @see org.griphyn.common.classes.TCType
     */
    boolean deleteTCbyPhysicalName( String physicalname, String namespace,
        String name, String version,
        String resourceid, TCType type ) throws
        Exception;

    /**
     * Delete a paricular type of transformation, and/or on a particular resource
     * @param type TCType The type of the transformation
     * @param resourceid String The resource on which the transformation exists.
     *                           If <B>NULL</B> then that type of transformation is deleted from all the resources.
     * @throws Exception
     * @return boolean Returns true if success, false if any error occurs.
     * @see org.griphyn.common.classes.TCType
     */
    boolean deleteTCbyType( TCType type, String resourceid ) throws
        Exception;

    /**
     * Delete all entries on a particular resource from the transformation catalog.
     * @param resourceid String The resource which you want to remove.
     * @throws Exception
     * @return boolean  Returns true if successm false if any error occurs.
     */
    boolean deleteTCbyResourceId( String resourceid ) throws Exception;

    /**
     * Deletes entries from the catalog which have a particular system information.
     * @param sysinfo SysInfo The System Information by which you want to delete
     * @return boolean Returns true for success, false if any error occurs.
     * @see org.griphyn.common.classes.SysInfo
     * @throws Exception
     */
    boolean deleteTCbySysInfo( SysInfo sysinfo ) throws Exception;

    /**
     * Deletes the entire transformation catalog. CLEAN............. USE WITH CAUTION.
     * @return boolean Returns true if delete succeeds, false if any error occurs.
     * @throws Exception
     */
    boolean deleteTC() throws Exception;

    /**
     * Delete a list of profiles or all the profiles associated with a pfn on a resource and of a type.
     * @param physicalname String The physical name of the transformation.
     * @param type TCType The type of the transformation.
     * @param resourceid String The resource of the transformation.
     * @param profiles List The list of profiles to be deleted. If <B>NULL</B> then all profiles for that pfn+resource+type are deleted.
     * @return boolean Returns true if success, false if any error occurs.
     * @see org.griphyn.cPlanner.classes.Profile
     * @throws Exception
     */
    boolean deleteTCPfnProfile( String physicalname, TCType type,
        String resourceid, List profiles ) throws
        Exception;

    /**
     * Delete a list of profiles or all the profiles associated with a logical transformation.
     * @param namespace String  The namespace of the logical transformation.
     * @param name String The name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param profiles List The List of profiles to be deleted. If <B>NULL</B> then all profiles for the logical transformation are deleted.
     * @return boolean Returns true if success, false if any error occurs.
     * @see org.griphyn.cPlanner.classes.Profile
     * @throws Exception
     */
    boolean deleteTCLfnProfile( String namespace, String name,
        String version, List profiles ) throws
        Exception;

    /**
     * Returns the TC implementation being used
     * @return String
     */
    String getTCMode();

}
