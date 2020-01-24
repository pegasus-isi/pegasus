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
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.Boolean;
import edu.isi.pegasus.common.util.ProfileParser;
import edu.isi.pegasus.common.util.ProfileParserException;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.NMI2VDSSysInfo;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.ScannerException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * This is the new file based TC implementation storing the contents of the file in memory. For the
 * old tc file implementation see OldTC.java
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @version $Revision$
 */
public class File extends Abstract implements TransformationCatalog {

    /** The singleton handler to the contents of the transformation catalog. */
    private static File mTCFileHandle = null;

    /**
     * The LogManager object which is used to log all the messages. It's values are set in the
     * CPlanner (the main toolkit) class.
     */
    protected LogManager mLogger;

    /** The List containing the user specified list of pools on which he wants the dag to run. */
    protected List mvExecPools;

    /** The Tree Map which stores the contents of the file. The key is the transformationname. */
    private Map mTreeMap;

    /** The path to the file based TC. */
    private String mTCFile;

    /** The handle to the properties object. */
    private PegasusProperties mProps;

    /** Boolean indicating whether to flush the contents back to the file on close. */
    private boolean mFlushOnClose;

    /** Boolean indicating whether to modify the file URL or not */
    private boolean modifyFileURL = true;

    /**
     * Returns an instance of the File TC.
     *
     * @return TransformationCatalog
     * @deprecated
     */
    public static TransformationCatalog getInstance() {
        if (mTCFileHandle == null) {
            PegasusBag bag = new PegasusBag();
            bag.add(PegasusBag.PEGASUS_LOGMANAGER, LogManagerFactory.loadSingletonInstance());
            bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
            mTCFileHandle = new File();
            mTCFileHandle.initialize(bag);
        }
        return mTCFileHandle;
    }

    /** The default constructor. */
    public void File() {}

    /**
     * Initialize the implementation, and return an instance of the implementation.
     *
     * @param bag the bag of Pegasus initialization objects.
     */
    public void initialize(PegasusBag bag) {
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        mFlushOnClose = false;
        modifyFileURL = Boolean.parse(mProps.getProperty(MODIFY_FOR_FILE_URLS_KEY), true);
        mTCFile = mProps.getTCPath();
        mTreeMap = new TreeMap();
        mLogger.log(
                "TC Mode being used is " + this.getDescription(), LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("TC File being used is " + mTCFile, LogManager.CONFIG_MESSAGE_LEVEL);

        if (mTCFile == null) {
            throw new RuntimeException(
                    "The File to be used as TC should be "
                            + "defined with the property pegasus.catalog.transformation.file");
        } else {

            java.io.File f = new java.io.File(mTCFile);

            if (f.exists()) {
                populateTC();
            } else {
                mLogger.log(
                        "The Transformation Catalog file " + mTCFile + " was not found",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }
        }
    }

    /**
     * Returns a textual description of the transformation mode.
     *
     * @return String containing the description.
     */
    @Override
    public String getDescription() {
        String st = "New FILE TC Mode";
        return st;
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
    @Override
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
        List results = null;
        String lfn = Separator.combine(namespace, name, version);
        mLogger.log(
                "Trying to get TCEntries for "
                        + lfn
                        + " on resource "
                        + ((resourceid == null) ? "ALL" : resourceid)
                        + " of type "
                        + ((type == null) ? "ALL" : type.toString()),
                LogManager.DEBUG_MESSAGE_LEVEL);
        if (resourceid != null) {
            if (mTreeMap.containsKey(resourceid)) {
                Map lfnMap = (Map) mTreeMap.get(resourceid);
                if (lfnMap.containsKey(lfn)) {
                    List l = (List) lfnMap.get(lfn);
                    if (type != null && l != null) {
                        for (Iterator i = l.iterator(); i.hasNext(); ) {
                            TransformationCatalogEntry tc = (TransformationCatalogEntry) i.next();
                            if (tc.getType().equals(type)) {
                                if (results == null) {
                                    results = new ArrayList();
                                }
                                results.add(tc);
                            }
                        }
                    } else {
                        results = l;
                    }
                }
            }
        } else {
            // since resourceid is null return entries for all sites
            if (!mTreeMap.isEmpty()) {

                for (Iterator j = mTreeMap.values().iterator(); j.hasNext(); ) {
                    // check all maps for the executable.
                    Map lfnMap = (Map) j.next();
                    if (lfnMap.containsKey(lfn)) {
                        List l = (List) lfnMap.get(lfn);
                        if (type != null && l != null) {
                            for (Iterator i = l.iterator(); i.hasNext(); ) {
                                TransformationCatalogEntry tc =
                                        (TransformationCatalogEntry) i.next();
                                if (tc.getType().equals(type)) {
                                    if (results == null) {
                                        results = new ArrayList();
                                    }
                                    results.add(tc);
                                }
                            }
                        } else {
                            // if the list returned is not empty keep adding to the result list.
                            if (l != null) {
                                if (results == null) {
                                    results = new ArrayList();
                                }
                                results.addAll(l);
                            }
                        }
                    }
                }
            }
        }
        return results;
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
        List results = null;
        List lfnList = new ArrayList();
        if (name == null) {
            if (type == null) {
                // return all the resources only
                results = new ArrayList(mTreeMap.keySet());
                return results;
            }
        }
        // return all the entries to search for type
        lfnList.addAll(mTreeMap.values());

        List entries = null;
        for (Iterator i = lfnList.iterator(); i.hasNext(); ) {
            Map lfnMap = (Map) i.next();
            if (entries == null) {
                entries = new ArrayList();
            }
            if (name == null) {
                for (Iterator j = lfnMap.values().iterator(); j.hasNext(); ) {
                    entries.addAll((List) j.next());
                }
            } else {
                if (lfnMap.containsKey(Separator.combine(namespace, name, version))) {
                    entries.addAll((List) lfnMap.get(Separator.combine(namespace, name, version)));
                }
            }
        }
        TreeSet rset = null;
        for (Iterator i = entries.iterator(); i.hasNext(); ) {
            if (rset == null) {
                rset = new TreeSet();
            }
            TransformationCatalogEntry entry = (TransformationCatalogEntry) i.next();
            if (type == null) {
                rset.add(entry.getResourceId());
            } else {
                if (entry.getType().equals(type)) {
                    rset.add(entry.getResourceId());
                }
            }
        }
        if (rset != null) {
            results = new ArrayList();
            for (Iterator i = rset.iterator(); i.hasNext(); ) {
                results.add((String) i.next());
            }
        }
        return results;
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
        List<TransformationCatalogEntry> results = null;
        List lfnMap = new ArrayList();
        /*
        int count[] = {
            0, 0, 0};
            */
        if (resourceid == null) {
            lfnMap.addAll(mTreeMap.values());
        } else {
            if (mTreeMap.containsKey(resourceid)) {
                lfnMap.add(mTreeMap.get(resourceid));
            } else {
                return null;
            }
        }

        for (Iterator i = lfnMap.iterator(); i.hasNext(); ) {
            Map lMap = (Map) i.next();
            if (lMap.containsKey(Separator.combine(namespace, name, version))) {
                for (Iterator j =
                                ((List) lMap.get(Separator.combine(namespace, name, version)))
                                        .iterator();
                        j.hasNext(); ) {
                    TransformationCatalogEntry entry = (TransformationCatalogEntry) j.next();
                    if (type != null) {
                        if (!entry.getType().equals(type)) {
                            continue;
                        }
                    }

                    if (results == null) {
                        results = new ArrayList<TransformationCatalogEntry>();
                    }
                    results.add(entry);
                }
            }
        }
        /*
        if (results != null) {
            results.add(count);
        }
        */
        return results;
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
        List result = null;
        /*
        int[] length = {
            0, 0};
            */
        List lfnMap = new ArrayList();
        String lfn = null, resource = null, tctype = null;
        if (resourceid == null) {
            lfnMap.addAll(mTreeMap.values());
        } else {
            if (mTreeMap.containsKey(resourceid)) {
                lfnMap.add((Map) mTreeMap.get(resourceid));
            } else {
                lfnMap = null;
            }
        }
        if (lfnMap != null) {
            for (Iterator i = lfnMap.iterator(); i.hasNext(); ) {
                for (Iterator j = ((Map) i.next()).values().iterator(); j.hasNext(); ) {
                    for (Iterator k = ((List) j.next()).iterator(); k.hasNext(); ) {
                        TransformationCatalogEntry tc = (TransformationCatalogEntry) k.next();
                        String l = null, r = null, t = null;
                        if (type == null) {
                            l = tc.getLogicalTransformation();
                            r = tc.getResourceId();
                            t = tc.getType().toString();

                        } else {
                            if (tc.getType().equals(type)) {
                                l = tc.getLogicalTransformation();
                                r = tc.getResourceId();
                                t = tc.getType().toString();
                            }
                        }
                        if (l != null && r != null && t != null) {
                            if (lfn == null
                                    || !(lfn.equalsIgnoreCase(l)
                                            && resource.equalsIgnoreCase(r)
                                            && tctype.equalsIgnoreCase(t))) {
                                lfn = l;
                                resource = r;
                                tctype = t;
                                String[] s = {r, l, t};
                                // columnLength(s, length);
                                if (result == null) {
                                    result = new ArrayList(5);
                                }
                                result.add(s);
                            }
                        }
                    }
                }
            }
        }
        /*
        if (result != null) {
            result.add(length);
        }
        */
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
     * @see org.griphyn.cPlanner.classes.Profile
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
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public List<Profile> lookupPFNProfiles(String pfn, String resourceid, TCType type)
            throws Exception {
        logMessage("getTCPfnProfiles(String pfn, String resourceid, TCType type)");
        logMessage("\t getTCPfnProfiles(" + pfn + "," + resourceid + "," + type + ")");

        List result = null;
        List lfnMap = new ArrayList();
        if (mTreeMap.containsKey(resourceid)) {
            lfnMap.add((Map) mTreeMap.get(resourceid));
        }
        for (Iterator i = lfnMap.iterator(); i.hasNext(); ) {
            for (Iterator j = ((Map) i.next()).values().iterator(); j.hasNext(); ) {
                for (Iterator k = ((List) j.next()).iterator(); k.hasNext(); ) {
                    TransformationCatalogEntry tc = (TransformationCatalogEntry) k.next();
                    List profiles = null;
                    if (tc.getPhysicalTransformation().equals(pfn)) {
                        if (type == null || tc.getType().equals(type)) {
                            profiles = tc.getProfiles();
                        }
                        if (profiles != null) {
                            if (result == null) {
                                result = new ArrayList(10);
                            }
                            result.addAll(profiles);
                        }
                    }
                }
            }
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
        List result = new ArrayList();
        for (Iterator i = mTreeMap.values().iterator(); i.hasNext(); ) {
            for (Iterator j = ((Map) i.next()).values().iterator(); j.hasNext(); ) {
                for (Iterator k = ((List) j.next()).iterator(); k.hasNext(); ) {
                    TransformationCatalogEntry tc = (TransformationCatalogEntry) k.next();
                    result.add(tc);
                }
            }
        }
        /*     List result = null;
            int[] length = {0, 0, 0, 0, 0};
            for ( Iterator i = mTreeMap.values().iterator(); i.hasNext(); ) {
                for ( Iterator j = ( ( Map ) i.next() ).values().iterator();
                    j.hasNext(); ) {
        for ( Iterator k = ( ( List ) j.next() ).iterator(); k.hasNext(); ) {
                        TransformationCatalogEntry tc = (
                            TransformationCatalogEntry ) k.next();
                        if ( result == null ) {
                            result = new ArrayList( 10 );
                        }
                        String[] s = {tc.getResourceId(),
                            tc.getLogicalTransformation(),
                            tc.getPhysicalTransformation(),
        tc.getType().toString(), tc.getVDSSysInfo().toString(),
                            ( ( tc.getProfiles() != null ) ?
        ProfileParser.combine( tc.getProfiles() ) : "NULL" )};
                        columnLength( s, length );
                        result.add( s );
                    }
                }
            }
            if ( result != null ) {
                result.add( length );
            }
        */
        return result;
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
                entry.getSysInfo());
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
     * @see org.griphyn.cPlanner.classes.Profile
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
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     * @see edu.isi.pegasus.planner.catalog.classes.SysInfo
     * @see org.griphyn.cPlanner.classes.Profile
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
        entry.setVDSSysInfo(NMI2VDSSysInfo.nmiToVDSSysInfo(system));
        entry.addNotifications(invokes);

        Map lfnMap = null;
        if (mTreeMap.containsKey(resourceid)) {
            lfnMap = (Map) mTreeMap.get(resourceid);
        } else {
            lfnMap = new TreeMap();
            mTreeMap.put(resourceid, lfnMap);
        }

        List pfnList = null;
        if (lfnMap.containsKey(entry.getLogicalTransformation())) {
            pfnList = (List) lfnMap.get(entry.getLogicalTransformation());
        } else {
            pfnList = new ArrayList(2);
            lfnMap.put(entry.getLogicalTransformation(), pfnList);
        }
        boolean add = true;
        for (Iterator i = pfnList.iterator(); i.hasNext(); ) {
            TransformationCatalogEntry test = (TransformationCatalogEntry) i.next();
            if (test.equals(entry)) {
                add = false;
            }
        }
        if (add) {
            pfnList.add(entry);
            if (write) {
                mFlushOnClose = true;
                // writeTC();
            }
        } else {
            mLogger.log("TC Entry already exists. Skipping", LogManager.DEBUG_MESSAGE_LEVEL);
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
     * @see org.griphyn.cPlanner.classes.Profile
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
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public int addPFNProfile(String pfn, TCType type, String resourcename, List profiles)
            throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /** DELETIONS */

    /**
     * Delete all entries in the transformation catalog for a give logical tranformation and/or on a
     * resource and/or of a particular type
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
     * @param namespace String The namespace assocaited in the logical name of the transformation.
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

        if (mTreeMap.containsKey(resourceid)) {
            mTreeMap.remove(resourceid);
            mFlushOnClose = true;
            // writeTC();
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * Deletes the entire transformation catalog. CLEAN............. USE WITH CAUTION.
     *
     * @return the number of removed entries.
     * @throws Exception
     */
    public int clear() throws Exception {
        int length = mTreeMap.size();
        mTreeMap.clear();
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
     * @see org.griphyn.cPlanner.classes.Profile
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
     * @see org.griphyn.cPlanner.classes.Profile
     * @throws Exception
     */
    public int deleteLFNProfiles(String namespace, String name, String version, List profiles)
            throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public boolean connect(java.util.Properties props) {
        // not implemented
        return true;
    }

    public boolean isClosed() {

        return (this.mTreeMap == null);
    }

    public void close() {
        try {
            if (mFlushOnClose) {
                writeTC();
                this.mFlushOnClose = false;
            }
        } finally {
            this.mTreeMap = null;
            this.mTCFile = null;
        }
    }

    private void writeTC() {
        PrintWriter writer = null;
        try {
            mLogger.log("Starting to write the TC file", LogManager.DEBUG_MESSAGE_LEVEL);
            writer = new PrintWriter(new BufferedWriter(new FileWriter(mTCFile, false)));

        } catch (IOException e) {
            mLogger.log(
                    "Unable to open TC File for writing\"" + mTCFile,
                    e,
                    LogManager.ERROR_MESSAGE_LEVEL);
        }
        int count = 0;
        for (Iterator i = mTreeMap.values().iterator(); i.hasNext(); ) {
            // get all the values from the main map
            for (Iterator j = ((Map) i.next()).values().iterator(); j.hasNext(); ) {
                // for each resource and each logical transformatino get the arraylist.
                for (Iterator k = ((List) j.next()).iterator(); k.hasNext(); ) {
                    // start printing each entry
                    writer.println(((TransformationCatalogEntry) k.next()).toTCString());
                    count++;
                }
            }
        }
        mLogger.log(
                "Written " + count + " entries back to the TC file",
                LogManager.DEBUG_MESSAGE_LEVEL);
        writer.flush();
        writer.close();
        mLogger.log("Starting to write the TC file - DONE", LogManager.DEBUG_MESSAGE_LEVEL);
    }

    /**
     * Computes the maximum column lenght for pretty printing.
     *
     * @param s String[]
     * @param count int[]
     */
    /*
    private static void columnLength(String[] s, int[] count) {
        for (int i = 0; i < count.length; i++) {
            if (s[i].length() > count[i]) {
                count[i] = s[i].length();
            }
        }

    }*/

    /**
     * Populates the internal copy of the transformation catalog from a byte stream (input stream).
     * Used in webservices, when clients upload their files. It uses the default character encoding.
     *
     * @param reader the <code>InputStrean</code> containing the bytes to be read.
     * @return boolean
     */
    private boolean populateTC(InputStream reader) {
        return populateTC(new InputStreamReader(reader));
    }

    /**
     * Populates the internal copy of the transformation catalog from the file containing the
     * transformation catalog in the 6 column format.
     *
     * @return boolean
     */
    private boolean populateTC() {
        boolean result = false;

        try {
            result = populateTC(new FileReader(mTCFile));
        } catch (IOException e) {
            mLogger.log("Unable to open the file " + mTCFile, e, LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        return result;
    }

    /**
     * Adds multiple entries into the TC. Calls the above api multiple times.
     *
     * @param reader the input stream from where to read the contents of the transformation catalog.
     * @return boolean
     */
    private boolean populateTC(Reader reader) {
        BufferedReader buf = new BufferedReader(reader);
        // String profilestring = null;
        int linecount = 0;
        int count = 0;
        try {
            String line = null;
            // buf = new BufferedReader( new FileReader( mTCFile ) );
            while ((line = buf.readLine()) != null) {
                boolean profile_error = false;
                linecount++;
                if (!(line.startsWith("#") || line.trim().length() == 0)) {
                    TransformationCatalogEntry tc = new TransformationCatalogEntry();
                    String[] tokens = line.trim().split("[ \t]+", 6);
                    if (tokens.length < 5) {
                        throw new ScannerException(linecount, "Invalid TC entry format.");
                    }
                    for (int i = 0; i < tokens.length; i++) {
                        switch (i) {
                            case 0: // poolname
                                tc.setResourceId(tokens[i]);
                                break;
                            case 1: // logical transformation name
                                if (tokens[i].indexOf("__") != -1) {
                                    mLogger.log(
                                            "Logical Transformations in the new File TC "
                                                    + "are represented as NS::NAME:VER",
                                            LogManager.ERROR_MESSAGE_LEVEL);
                                    mLogger.log(
                                            "Assuming "
                                                    + tokens[i]
                                                    + " as just the transformation NAME.",
                                            LogManager.DEBUG_MESSAGE_LEVEL);
                                }
                                tc.setLogicalTransformation(tokens[i]);
                                break;
                            case 2: // pfn
                                tc.setPhysicalTransformation(tokens[i]);
                                break;
                            case 3: // type
                                if (tokens[i].equalsIgnoreCase("null")) {
                                    tc.setType(TCType.INSTALLED);
                                } else if (tokens[i].equalsIgnoreCase("STATIC_BINARY")) {
                                    // if entry is static binary we set it to stageable
                                    tc.setType(TCType.STAGEABLE);
                                } else {
                                    // set to whatever the value was
                                    tc.setType(TCType.valueOf(tokens[i]));
                                }
                                /*
                                tc.setType( (tokens[i].equalsIgnoreCase(
                                    "null")) ?
                                           TCType.INSTALLED :
                                           TCType.valueOf(tokens[i]));
                                */
                                break;
                            case 4: // systeminfo
                                tc.setVDSSysInfo(
                                        (tokens[i].equalsIgnoreCase("null"))
                                                ? new VDSSysInfo(null)
                                                : new VDSSysInfo(tokens[i]));
                                break;
                            case 5: // profile string
                                if (!tokens[i].equalsIgnoreCase("null")) {
                                    try {
                                        tc.addProfiles(ProfileParser.parse(tokens[i]));
                                    } catch (ProfileParserException ppe) {
                                        mLogger.log(
                                                "Could not parse profile(s) for transformation \""
                                                        + tc.getLogicalTransformation()
                                                        + "\" on site \""
                                                        + tc.getResourceId()
                                                        + "\" on line "
                                                        + linecount,
                                                LogManager.ERROR_MESSAGE_LEVEL);
                                        mLogger.log(
                                                ppe.getMessage()
                                                        + " at position "
                                                        + ppe.getPosition()
                                                        + " for the string \""
                                                        + tokens[i]
                                                        + "\"",
                                                LogManager.ERROR_MESSAGE_LEVEL);
                                        mLogger.log(
                                                "Ignoring the current transformation. Please fix the profiles shown above.",
                                                LogManager.ERROR_MESSAGE_LEVEL);

                                        profile_error = true;
                                        continue;
                                    } catch (RuntimeException e) {
                                        mLogger.log(
                                                "Ignoring errors while parsing profile in Transformation Catalog on line "
                                                        + linecount,
                                                e,
                                                LogManager.WARNING_MESSAGE_LEVEL);
                                    }
                                }
                                break;
                            default:
                                mLogger.log(
                                        "Line " + linecount + " : Humm no need to be in default",
                                        LogManager.ERROR_MESSAGE_LEVEL);
                        } // end of switch
                    } // end of for loop
                    if (profile_error) {
                        // if there is an error while parsing the profile
                        // Skip adding the entry to TC (As Per JIRA PM-164
                        continue;
                    }

                    // if (count > 0) {

                    //   mLogger.logMessage("Loading line number" + linecount +
                    //                    " to the map", 1);
                    Map lfnMap = null;
                    if (!mTreeMap.containsKey(tc.getResourceId())) {
                        lfnMap = new TreeMap();
                    } else {
                        lfnMap = (Map) mTreeMap.get(tc.getResourceId());
                    }
                    List entries = null;
                    if (!lfnMap.containsKey(tc.getLogicalTransformation())) {
                        entries = new ArrayList(3);
                    } else {
                        entries = (List) lfnMap.get(tc.getLogicalTransformation());
                    }
                    if (modifyFileURL) {
                        entries.add(Abstract.modifyForFileURLS(tc));
                    } else {
                        entries.add(tc);
                    }
                    lfnMap.put(tc.getLogicalTransformation(), entries);
                    mTreeMap.put(tc.getResourceId(), lfnMap);
                    count++;
                } // end of if "#"
            } // end of while line
            mLogger.log(
                    "Loaded " + count + " entries to the TC Map", LogManager.DEBUG_MESSAGE_LEVEL);
            buf.close();
            return true;
        } catch (FileNotFoundException ex) {
            mLogger.log(
                    "The tc text file " + mTCFile + " was not found",
                    LogManager.ERROR_MESSAGE_LEVEL);
            mLogger.log("Considering it as Empty TC", LogManager.ERROR_MESSAGE_LEVEL);
            return true;
        } catch (IOException e) {
            mLogger.log("Unable to open the file " + mTCFile, e, LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        } catch (IllegalStateException e) {
            mLogger.log(
                    "On line " + linecount + "in File " + mTCFile + "\n",
                    e,
                    LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        } catch (Exception e) {
            mLogger.log(
                    "While loading entries into the map on line " + linecount + "\n",
                    e,
                    LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
    }

    /**
     * Returns the file source.
     *
     * @return the file source if it exists , else null
     */
    public java.io.File getFileSource() {
        if (mTCFile != null) {
            java.io.File f = new java.io.File(mTCFile);
            if (f.canRead()) {
                return f;
            }
        }
        return null;
    }

    /**
     * Logs the message to a logging stream. Currently does not log to any stream.
     *
     * @param msg the message to be logged.
     */
    protected void logMessage(String msg) {
        // mLogger.logMessage("[Shishir] Transformation Catalog : " + msg);
    }
}
