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
package edu.isi.pegasus.planner.catalog.transformation.impl;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.Boolean;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * This class implements a transformation catalog on top of a directory. This implementation does a
 * directory listing to build up the lfn, pfn mappings for the Transformation Catalog. The
 * implementation does not support association of Containers with executables.
 *
 * <p>To connect to this implementation, in Pegasus Properties set
 *
 * <p>pegasus.catalog.transformation Directory
 *
 * <p>The site attribute defaults to local unless specified in Pegasus Properties by specifying the
 * property
 *
 * <pre>
 *      pegasus.catalog.transformation.directory.site
 * </pre>
 *
 * The URL prefix for the PFN's defaults to file:// unless specified in Pegasus Properties by
 * specifying the property
 *
 * <pre>
 *      pegasus.catalog.replica.transformation.url.prefix
 * </pre>
 *
 * By default, deep LFN's are constructed while traversing through the directory, unless the
 * following property is set to true
 *
 * <pre>
 *     pegasus.catalog.transformation.directory.flat.lfn
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Directory extends Abstract implements TransformationCatalog {

    /** Describes the transformation catalog mode. */
    public static final String DESCRIPTION = "Directory backed TC";

    /** The default site handle to use. */
    public static final String DEFAULT_SITE_HANDLE = "local";

    /** The default URL Prefix to use. */
    public static final String DEFAULT_URL_PREFIX = "file://";

    /** The name of the key that specifies the path to directory. */
    public static final String DIRECTORY_PROPERTY_KEY = "directory";

    /** The name of the key that specifies the site attribute to be associated */
    public static final String SITE_PROPERTY_KEY = "directory.site";

    /** The name of the key that specifies the url prefix to be associated with the PFN's */
    public static final String URL_PRFIX_PROPERTY_KEY = "directory.url.prefix";

    /**
     * the name of the key that specifies whether we want flat lfns or not. By default it is false,
     * i.e we construct deep lfn's when traversing through the directory hierarchy
     */
    public static final String FLAT_LFN_PROPERTY_KEY = "directory.flat.lfn";

    /**
     * The LogManager object which is used to log all the messages. It's values are set in the
     * CPlanner (the main toolkit) class.
     */
    protected LogManager mLogger;

    /** The handle to the properties object. */
    private PegasusProperties mProps;

    /** The transformation store containing the transformations after parsing the file. */
    private TransformationStore mTCStore;

    /** Boolean indicating whether to flush the contents back to the file on close. */
    private boolean mFlushOnClose;

    /** Boolean indicating whether to modify the file URL or not */
    private boolean modifyFileURL = true;

    private PegasusBag mBag;

    private File mDirectory;

    /** A boolean indicating whether the catalog is to construct flat lfns or not */
    private boolean mConstructFlatLFN;

    /** The URL prefix to use */
    protected String mURLPrefix;

    /** The site handle to use. */
    protected String mSiteHandle;

    /** the system information to be assigned to created tranformation catalog entries */
    private SysInfo mSysInfo;

    /** Default constructor. */
    public Directory() {}

    /**
     * Initialize the implementation, and return an instance of the implementation. It should be in
     * the connect method, to be consistent with the other catalogs.
     *
     * @param bag the bag of Pegasus initialization objects.
     */
    public void initialize(PegasusBag bag) {
        mBag = bag;
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        mSysInfo = createSysInfo(bag);
        mFlushOnClose = false;
        modifyFileURL = true;
        // empty TCStore
        mTCStore = new TransformationStore();
        mSiteHandle = Directory.DEFAULT_SITE_HANDLE;
        mURLPrefix = Directory.DEFAULT_URL_PREFIX;
        mLogger.log(
                "Transformation Catalog Type used " + this.getDescription(),
                LogManager.CONFIG_MESSAGE_LEVEL);
    }

    /**
     * Empty for the time being. The factory still calls out to the initialize method.
     *
     * @param props the connection properties.
     * @return
     */
    public boolean connect(Properties props) {
        String dir = props.getProperty("directory");
        if (dir == null) {
            throw new RuntimeException(
                    "The directory to be used as TC should be "
                            + "defined with the property pegasus.catalog.transformation.directory");
        }
        mLogger.log(
                "Directory from where transformations will be picked up " + dir,
                LogManager.CONFIG_MESSAGE_LEVEL);

        // update the m_writeable flag if specified
        if (props.containsKey(Directory.FLAT_LFN_PROPERTY_KEY)) {
            mConstructFlatLFN =
                    Boolean.parse(props.getProperty(Directory.FLAT_LFN_PROPERTY_KEY), false);
        }

        String value = props.getProperty(Directory.SITE_PROPERTY_KEY);
        if (value != null) {
            this.mSiteHandle = value;
        }

        value = props.getProperty(Directory.URL_PRFIX_PROPERTY_KEY);
        if (value != null) {
            this.mURLPrefix = value;
        }

        if (props.containsKey(Directory.DIRECTORY_PROPERTY_KEY)) {
            return connect(props.getProperty("directory"));
        }
        return false;
    }

    /**
     * Does the file listing on the directory to create the mappings in memory.
     *
     * @param directory is the name of the file to read.
     * @return true, if the in-memory data structures appear sound.
     */
    public boolean connect(String directory) {
        // sanity check
        if (directory == null) {
            mLogger.log(
                    "The directory to be used as TC should be "
                            + "defined with the property pegasus.catalog.transformation.directory",
                    LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }

        mLogger.log(
                "Directory from where transformations will be picked up " + directory,
                LogManager.CONFIG_MESSAGE_LEVEL);
        mDirectory = new File(directory);

        try {
            if (mDirectory.exists() && mDirectory.isDirectory()) {
                traverse(mDirectory, null);
            } else {
                return false;
            }
        } catch (Exception ioe) {
            mDirectory = null;
            throw new RuntimeException(ioe); // re-throw
        }

        return true;
    }

    /**
     * Traverses a directory and populates the mappings in memory
     *
     * @param directory the directory to traverse.
     * @param prefix the LFN prefix to be applied
     */
    private void traverse(File directory, String prefix) throws Exception {
        // sanity check, if we can read it
        if (!directory.canRead()) {
            // warn and return
            mLogger.log(
                    "Ignoring. Unable to read directory " + directory,
                    LogManager.WARNING_MESSAGE_LEVEL);
            return;
        }

        for (File f : directory.listFiles()) {
            StringBuffer lfn = new StringBuffer();
            String name = f.getName();
            if (mConstructFlatLFN || prefix == null || prefix.isEmpty()) {
                lfn.append(name);
            } else {
                lfn.append(prefix).append(File.separator).append(name);
            }

            if (f.isDirectory()) {
                // the lfn is the prefix now
                traverse(f, lfn.toString());
            } else {
                // we have a mapping to populate
                String pfn = this.mURLPrefix + f.getAbsolutePath();
                if (!f.canExecute()) {
                    mLogger.log(
                            "Skipping file to create as a transformation since the execute bit is not set "
                                    + pfn,
                            LogManager.WARNING_MESSAGE_LEVEL);
                }

                TransformationCatalogEntry entry =
                        new TransformationCatalogEntry(null, lfn.toString(), null);
                entry.setResourceId(this.mSiteHandle);
                entry.setPhysicalTransformation(pfn);
                entry.setSysInfo(mSysInfo);
                entry.setType(TCType.STAGEABLE);
                mLogger.log(
                        "Created transformation catalog entry " + entry,
                        LogManager.DEBUG_MESSAGE_LEVEL);
                insert(entry);
            }
        }

        return;
    }

    /**
     * Returns whether the connection is closed or not.
     *
     * @return
     */
    public boolean isClosed() {
        // not implemented
        return this.mTCStore == null;
    }

    /** Closes the connection to the back end. */
    public void close() {
        mDirectory = null;
    }

    /**
     * Returns a textual description of the transformation mode.
     *
     * @return String containing the description.
     */
    public String getDescription() {
        return Directory.DESCRIPTION;
    }

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
            throws Exception {
        logMessage(
                "getTCEntries(String namespace,String name,String version,"
                        + "List resourceids, TCType type");
        logMessage(
                "\tgetTCEntries("
                        + namespace
                        + ", "
                        + name
                        + ", "
                        + version
                        + ", "
                        + resourceids
                        + ", "
                        + type);
        List results = null;
        if (resourceids != null) {
            for (Iterator i = resourceids.iterator(); i.hasNext(); ) {
                List tempresults = lookup(namespace, name, version, (String) i.next(), type);
                if (tempresults != null) {
                    if (results == null) {
                        results = new ArrayList();
                    }
                    results.addAll(tempresults);
                }
            }
        } else {
            List tempresults = lookup(namespace, name, version, (String) null, type);
            if (tempresults != null) {
                results = new ArrayList(tempresults.size());
                results.addAll(tempresults);
            }
        }
        return results;
    }

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
            throws Exception {
        logMessage(
                "getTCEntries(String namespace, String name, String version, "
                        + "String resourceId, TCType type)");
        logMessage(
                "\t getTCEntries("
                        + namespace
                        + ", "
                        + name
                        + ", "
                        + version
                        + ","
                        + resourceid
                        + ", "
                        + type);
        List result = null;
        String lfn = Separator.combine(namespace, name, version);
        mLogger.log(
                "Trying to get TCEntries for "
                        + lfn
                        + " on resource "
                        + ((resourceid == null) ? "ALL" : resourceid)
                        + " of type "
                        + ((type == null) ? "ALL" : type.toString()),
                LogManager.DEBUG_MESSAGE_LEVEL);

        // always returns a list , empty in case of no results
        result = mTCStore.getEntries(Separator.combine(namespace, name, version), resourceid, type);

        // API dictates we return null in case of empty
        return (result == null || result.isEmpty()) ? null : result;
    }

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
            throws Exception {
        logMessage(
                "List getTCResourceIds(String namespace, String name, String "
                        + "version, TCType type");
        logMessage("\t getTCResourceIds(" + namespace + ", " + name + ", " + version + ", " + type);

        // retrieve all entries for a transformation, matching a tc type
        List<TransformationCatalogEntry> entries =
                this.lookup(namespace, name, version, (String) null, type);

        Set<String> result = new HashSet();
        for (TransformationCatalogEntry entry : entries) {
            result.add(entry.getResourceId());
        }

        // API dictates we return null in case of empty
        return (result == null || result.isEmpty()) ? null : new LinkedList(result);
    }

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
            throws Exception {
        logMessage(
                "List getTCPhysicalNames(String namespace, String name,"
                        + "String version, String resourceid,TCType type)");
        logMessage(
                "\t getTCPhysicalNames("
                        + namespace
                        + ", "
                        + name
                        + ", "
                        + version
                        + ", "
                        + resourceid
                        + ", "
                        + type
                        + ")");

        // retrieve all entries for a transformation, matching a tc type
        List<TransformationCatalogEntry> entries =
                this.lookup(namespace, name, version, resourceid, type);

        List result = entries;

        // API dictates we return null in case of empty
        if (result == null || result.isEmpty()) {
            return null;
        }

        return result;
    }

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
    public List<String[]> getTCLogicalNames(String resourceid, TCType type) throws Exception {
        logMessage("List getTCLogicalNames(String resourceid, TCType type)");
        logMessage("\t getTCLogicalNames(" + resourceid + "," + type + ")");

        List<TransformationCatalogEntry> entries = mTCStore.getEntries(resourceid, type);

        // convert the list into the format Gaurang wants for the API.
        List result = new LinkedList();
        for (TransformationCatalogEntry entry : entries) {
            String l = entry.getLogicalTransformation();
            String r = entry.getResourceId();
            String t = entry.getType().toString();

            String[] s = {r, l, t};
            result.add(s);
        }

        // API dictates we return null in case of empty
        if (result == null || result.isEmpty()) {
            return null;
        }

        return result;
    }

    /**
     * Get the list of Profiles associated with a particular logical transformation.
     *
     * @param namespace String The namespace of the transformation to search for.
     * @param name String The name of the transformation to search for.
     * @param version String The version of the transformation to search for.
     * @return List Returns a list of Profile Objects containing profiles assocaited with the
     *     transformation. Returns <B>NULL</B> if no profiles found.
     * @throws Exception NotImplementedException if not implemented.
     * @see edu.isi.pegasus.planner.classes.Profile
     */
    public List<Profile> lookupLFNProfiles(String namespace, String name, String version)
            throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

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
     * @see edu.isi.pegasus.planner.classes.Profile
     */
    public List<Profile> lookupPFNProfiles(String pfn, String resourceid, TCType type)
            throws Exception {
        logMessage("getTCPfnProfiles(String pfn, String resourceid, TCType type)");
        logMessage("\t getTCPfnProfiles(" + pfn + "," + resourceid + "," + type + ")");

        List<Profile> result = new LinkedList<Profile>();

        // retrieve all the transformations corresponding to resource id and type
        // first
        List<TransformationCatalogEntry> entries = mTCStore.getEntries(resourceid, type);

        // traverse through the list
        for (TransformationCatalogEntry entry : entries) {
            if (entry.getPhysicalTransformation().equals(pfn)) {
                result.addAll(entry.getProfiles());
            }
        }

        // API dictates we return null in case of empty
        if (result == null || result.isEmpty()) {
            return null;
        }
        return result;
    }

    /**
     * List all the contents of the TC
     *
     * @return List Returns a List of TransformationCatalogEntry objects.
     * @throws Exception
     */
    public List<TransformationCatalogEntry> getContents() throws Exception {
        return mTCStore.getEntries((String) null, (TCType) null);
    }

    /** ADDITIONS */

    /**
     * Add multiple TCEntries to the Catalog.
     *
     * @param tcentry List Takes a list of TransformationCatalogEntry objects as input
     * @throws Exception
     * @return number of insertions On failure,throw an exception, don't use zero.
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public int insert(List<TransformationCatalogEntry> entries) throws Exception {
        for (int i = 0; i < entries.size(); i++) {
            TransformationCatalogEntry entry = ((TransformationCatalogEntry) entries.get(i));
            this.insert(entry);
        }
        return entries.size();
    }

    /**
     * Add single TCEntry to the Catalog.
     *
     * @param tcentry Takes a single TransformationCatalogEntry object as input
     * @throws Exception
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public int insert(TransformationCatalogEntry entry) throws Exception {
        return this.insert(
                entry.getLogicalNamespace(),
                entry.getLogicalName(),
                entry.getLogicalVersion(),
                entry.getPhysicalTransformation(),
                entry.getType(),
                entry.getResourceId(),
                null,
                entry.getProfiles(),
                entry.getSysInfo(),
                entry.getContainer());
    }

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
    public int insert(TransformationCatalogEntry entry, boolean write) throws Exception {
        if (this.addTCEntry(
                entry.getLogicalNamespace(),
                entry.getLogicalName(),
                entry.getLogicalVersion(),
                entry.getPhysicalTransformation(),
                entry.getType(),
                entry.getResourceId(),
                null,
                entry.getProfiles(),
                entry.getSysInfo(),
                entry.getNotifications(),
                entry.getContainer(),
                write)) {
            return 1;
        } else {
            throw new RuntimeException(
                    "Failed to add TransformationCatalogEntry " + entry.getLogicalName());
        }
    }

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
     * @see edu.isi.pegasus.planner.classes.Profile
     */
    public int insert(
            String namespace,
            String name,
            String version,
            String physicalname,
            TCType type,
            String resourceid,
            List pfnprofiles,
            List lfnprofiles,
            SysInfo system)
            throws Exception {
        if (this.addTCEntry(
                namespace,
                name,
                version,
                physicalname,
                type,
                resourceid,
                lfnprofiles,
                pfnprofiles,
                system,
                null,
                null,
                true)) {
            return 1;
        } else {
            throw new RuntimeException("Failed to add TransformationCatalogEntry " + name);
        }
    }

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
     * @param containerInfo SysInfo The Container information associated with a physical
     *     transformation.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @throws Exception
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     * @see edu.isi.pegasus.planner.catalog.classes.SysInfo
     * @see edu.isi.pegasus.planner.classes.Profile
     */
    public int insert(
            String namespace,
            String name,
            String version,
            String physicalname,
            TCType type,
            String resourceid,
            List pfnprofiles,
            List lfnprofiles,
            SysInfo system,
            Container containerInfo)
            throws Exception {
        if (this.addTCEntry(
                namespace,
                name,
                version,
                physicalname,
                type,
                resourceid,
                lfnprofiles,
                pfnprofiles,
                system,
                null,
                containerInfo,
                true)) {
            return 1;
        } else {
            throw new RuntimeException("Failed to add TransformationCatalogEntry " + name);
        }
    }

    /**
     * Add an single entry into the transformation catalog.
     *
     * @param namespace the namespace of the transformation to be added (Can be null)
     * @param name the name of the transformation to be added.
     * @param version the version of the transformation to be added. (Can be null)
     * @param physicalname the physical name/location of the transformation to be added.
     * @param type the type of the physical transformation.
     * @param resourceid the resource location id where the transformation is located.
     * @param lfnprofiles the List of <code>Profile</code> objects associated with a Logical
     *     Transformation. (can be null)
     * @param pfnprofiles the list of <code>Profile</code> objects associated with a Physical
     *     Transformation. (can be null)
     * @param system the System information associated with a physical transformation.
     * @param invokes the Notifications associated with the transformation.
     * @param write boolean to commit changes to backend catalog
     * @return boolean true if succesfully added, returns false if error and throws exception.
     * @throws Exception
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     * @see edu.isi.pegasus.planner.catalog.classes.SysInfo
     * @see edu.isi.pegasus.planner.classes.Profile
     */
    protected boolean addTCEntry(
            String namespace,
            String name,
            String version,
            String physicalname,
            TCType type,
            String resourceid,
            List pfnprofiles,
            List lfnprofiles,
            SysInfo system,
            Notifications invokes,
            Container containterInfo,
            boolean write)
            throws Exception {

        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        entry.setLogicalNamespace(namespace);
        entry.setLogicalName(name);
        entry.setLogicalVersion(version);
        entry.setPhysicalTransformation(physicalname);
        entry.setType(type);
        entry.setResourceId(resourceid);
        entry.addProfiles(lfnprofiles);
        entry.addProfiles(pfnprofiles);
        entry.setSysInfo(system);
        // entry.setVDSSysInfo( NMI2VDSSysInfo.nmiToVDSSysInfo(system) );
        entry.addNotifications(invokes);
        entry.setContainer(containterInfo);

        List<TransformationCatalogEntry> existing =
                this.lookup(namespace, name, version, resourceid, type);

        boolean add = true;

        if (existing != null) {
            // check to see if entries match
            for (TransformationCatalogEntry e : existing) {
                if (e.equals(entry)) {
                    add = false;
                    break;
                }
            }
        }

        if (add) {
            mTCStore.addEntry(entry);
            /* PM-1491 for tc-converter we need this. but this triggers error in planner because
               of PM-1214, where env profiles from job are stored in the container object.
               So different jobs referring to same container name don't match because of
               differing profiles
            if (containterInfo != null && !mTCStore.containsContainer(containterInfo)) {
                mTCStore.addContainer(containterInfo);
            }
            */
        } else {
            mLogger.log("TC Entry already exists. Skipping", LogManager.DEBUG_MESSAGE_LEVEL);
        }

        // if entry needs to be added and flushed to the backend
        // set to flag to true.
        if (write && add) {
            mFlushOnClose = true;
        }

        return true;
    }

    /**
     * Add additional profile to a logical transformation .
     *
     * @param namespace String The namespace of the transformation to be added. (can be null)
     * @param name String The name of the transformation to be added.
     * @param version String The version of the transformation to be added. (can be null)
     * @param profiles List The List of Profile objects that are to be added to the transformation.
     * @return number of insertions. On failure, throw an exception, don't use zero.
     * @throws Exception
     * @see edu.isi.pegasus.planner.classes.Profile
     */
    public int addLFNProfile(String namespace, String name, String version, List profiles)
            throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Add additional profile to a physical transformation.
     *
     * @param pfn String The physical name of the transformation
     * @param type TCType The type of transformation that the profile is associated with.
     * @param resourcename String The resource on which the physical transformation exists
     * @param profiles The List of Profile objects that are to be added to the transformation.
     * @return number of insertions. On failure, throw an exception, don't use zero.
     * @throws Exception
     * @see edu.isi.pegasus.planner.classes.Profile
     */
    public int addPFNProfile(String pfn, TCType type, String resourcename, List profiles)
            throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

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
            throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }
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
            throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

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
    public int removeByType(TCType type, String resourceid) throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Deletes entries from the catalog which have a particular system information.
     *
     * @param sysinfo SysInfo The System Information by which you want to delete
     * @return the number of removed entries.
     * @see edu.isi.pegasus.planner.catalog.classes.SysInfo
     * @throws Exception
     */
    public int removeBySysInfo(SysInfo sysinfo) throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Delete all entries on a particular resource from the transformation catalog.
     *
     * @param resourceid String The resource which you want to remove.
     * @throws Exception
     * @return the number of removed entries.
     */
    public int removeBySiteID(String resourceid) throws Exception {

        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Deletes the entire transformation catalog. CLEAN............. USE WITH CAUTION.
     *
     * @return the number of removed entries.
     * @throws Exception
     */
    public int clear() throws Exception {
        int length = (mTCStore.getEntries(null, (TCType) null)).size();
        mTCStore.clear();
        mFlushOnClose = true;
        return length;
    }

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
     * @see edu.isi.pegasus.planner.classes.Profile
     * @throws Exception
     */
    public int deletePFNProfiles(String physicalname, TCType type, String resourceid, List profiles)
            throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Delete a list of profiles or all the profiles associated with a logical transformation.
     *
     * @param namespace String The namespace of the logical transformation.
     * @param name String The name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param profiles List The List of profiles to be deleted. If <B>NULL</B> then all profiles for
     *     the logical transformation are deleted.
     * @return the number of removed entries.
     * @see edu.isi.pegasus.planner.classes.Profile
     * @throws Exception
     */
    public int deleteLFNProfiles(String namespace, String name, String version, List profiles)
            throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Returns the file source.
     *
     * @return the file source if it exists , else null
     */
    public java.io.File getFileSource() {
        return null;
    }

    protected void logMessage(String msg) {
        // mLogger.logMessage("[Shishir] Transformation Catalog : " + msg);
    }

    /**
     * Creates a system information object from the compute sites to which the workflow is planned
     * against.
     *
     * @param bag the bag of Planner objects
     * @return SysInfo to use for creating transformations
     * @throws RuntimeException if site store or planner options are null, OR multiple computes
     *     sites with different architectures is specified
     */
    protected SysInfo createSysInfo(PegasusBag bag) {
        SiteStore siteStore = bag.getHandleToSiteStore();
        PlannerOptions options = bag.getPlannerOptions();

        if (siteStore == null) {
            throw new RuntimeException(
                    "Site Store is not populated. Is required to assign the system information for generated transformations out of Directory backend");
        }
        if (options == null) {
            throw new RuntimeException(
                    "Planner Options is not populated. Is required to assign the system information for generated transformations out of Directory backend");
        }

        SysInfo result = null;
        String computeSite = null;
        for (String s : options.getExecutionSites()) {
            SysInfo sysInfo = siteStore.getSysInfo(s);
            if (sysInfo == null) {
                // still null . complain
                throw new RuntimeException("Unable to retrieve system information for site " + s);
            }
            if (result == null) {
                // assign the first time around
                computeSite = s;
                result = sysInfo;
            }

            // if multiple sites, just compare and make sure they match
            if (!result.equals(sysInfo)) {
                throw new RuntimeException(
                        "System information for compute site "
                                + computeSite
                                + "->"
                                + result
                                + " does not match for site "
                                + s
                                + "->"
                                + sysInfo);
            }
        }

        // last sanity check
        if (result == null) {
            // probably no compute sites set in the planner options
            throw new RuntimeException(
                    "Unable to create system information to associate with the transformations");
        }
        return result;
    }
}
