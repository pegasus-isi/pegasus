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

package edu.isi.pegasus.planner.catalog.transformation.impl;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import org.griphyn.cPlanner.common.PegasusProperties;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import edu.isi.pegasus.planner.catalog.transformation.classes.NMI2VDSSysInfo;

import edu.isi.pegasus.common.util.ProfileParser;
import edu.isi.pegasus.common.util.ProfileParserException;
import edu.isi.pegasus.common.util.Separator;

import edu.isi.pegasus.planner.parser.TransformationCatalogTextParser;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.griphyn.cPlanner.classes.PegasusBag;

/**
 * A File based Transformation Catalog where each entry spans multiple lines.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Text
    implements TransformationCatalog {

    /**
     * Describes the transformation catalog mode.
     */
    public  static final String DESCRIPTION = "Multiline Textual TC";

  
    /**
     * The LogManager object which is used to log all the messages.
     * It's values are set in the CPlanner (the main toolkit) class.
     */
    protected LogManager mLogger;

    
    /**
     * The path to the file based TC.
     */
    private String mTCFile;

    /**
     * The handle to the properties object.
     */
    private PegasusProperties mProps;

    /**
     * Instance to the TextParser.
     */
    private TransformationCatalogTextParser mTextParser;
    
    /**
     * The transformation store containing the transformations after parsing the
     * file.
     */
    private TransformationStore mTCStore;


    /**
     * Default constructor.
     */
    public Text(){
    }
    
    /**
     * Initialize the implementation, and return an instance of the implementation.
     * 
     * @param bag  the bag of Pegasus initialization objects.
     * 
     */
    public void initialize ( PegasusBag bag ){
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        
        mTCFile = mProps.getTCPath();
        mLogger.log("TC Mode being used is " + this.getTCMode(),
                    LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("TC File being used is " + mTCFile,
                    LogManager.CONFIG_MESSAGE_LEVEL);
        
        if (mTCFile == null) {
            throw new RuntimeException( "The File to be used as TC should be " +
                                        "defined with the property pegasus.catalog.transformation.file");
        }        


        try{
            mTextParser = new TransformationCatalogTextParser ( new FileReader(new java.io.File(  mTCFile )),
                                                            mLogger );
            mTCStore = mTextParser.parse();
        }
        catch (FileNotFoundException ex) {
            throw new RuntimeException( "Unable to find file " + mTCFile );
        }
        catch( IOException ioe ){
            throw new RuntimeException( "IOException while parsing transformation catalog" , ioe );
        }


    }
    

    /**
     * Returns a textual description of the transformation mode.
     *
     * @return String containing the description.
     */
    public String getTCMode() {
        return Text.DESCRIPTION;
    }

    /**
     * Returns TC entries for a particular logical transformation and/or on a
     * number of resources and/or of a particular type.
     *
     * @param namespace   the namespace of the logical transformation.
     * @param name        the name of the logical transformation.
     * @param version     the version of the logical transformation.
     * @param resourceids the List resourceid where the transformation is located.
     *                    If <b>NULL</b> it returns all resources.
     * @param type TCType the type of the transformation to search for. If
     *                    <b>NULL</b> it returns all types.
     *
     * @return a list of <code>TransformationCatalogEntry</code> objects
     *         containing the corresponding entries from the TC.
     *         Returns null if no entry found.
     *
     * @throws Exception
     * @see org.griphyn.common.classes.TCType
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    public List getTCEntries(String namespace, String name, String version,
                             List resourceids, TCType type) throws Exception {
        logMessage("getTCEntries(String namespace,String name,String version," +
                   "List resourceids, TCType type");
        logMessage("\tgetTCEntries(" + namespace + ", " + name + ", " +
                   version + ", " +
                   resourceids + ", " + type);
        List results = null;
        if (resourceids != null) {
            for (Iterator i = resourceids.iterator(); i.hasNext(); ) {
                List tempresults = getTCEntries(namespace, name, version,
                                                (String) i.next(), type);
                if (tempresults != null) {
                    if (results == null) {
                        results = new ArrayList();
                    }
                    results.addAll(tempresults);
                }
            }
        }
        else {
            List tempresults = getTCEntries(namespace, name, version, (String)null,
                                            type);
            if (tempresults != null) {
                results = new ArrayList(tempresults.size());
                results.addAll(tempresults);
            }

        }
        return results;
    }

    /**
     * Returns TC entries for a particular logical transformation and/or on a
     * particular resource and/or of a particular type.
     *
     * @param namespace   the namespace of the logical transformation.
     * @param name        the name of the logical transformation.
     * @param version     the version of the logical transformation.
     * @param resourceid  the resourceid where the transformation is located.
     *                    If <B>NULL</B> it returns all resources.
     * @param type TCType the type of the transformation to search for.
     *                    If <B>NULL</b> it returns all types.
     *
     * @return a list of <code>TransformationCatalogEntry</code> objects
     *         containing the corresponding entries from the TC.
     *         Returns null if no entry found.
     *
     * @throws Exception
     * @see org.griphyn.common.classes.TCType
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    public List getTCEntries(String namespace, String name, String version,
                             String resourceid, TCType type) throws Exception {
        logMessage(
            "getTCEntries(String namespace, String name, String version, " +
            "String resourceId, TCType type)");
        logMessage("\t getTCEntries(" + namespace + ", " + name + ", " +
                   version +
                   "," + resourceid + ", " + type);
        List result = null;
        String lfn = Separator.combine(namespace, name, version);
        mLogger.log("Trying to get TCEntries for " +
                    lfn +
                    " on resource " + ( (resourceid == null) ? "ALL" :
                                       resourceid) + " of type " +
                    ( (type == null) ? "ALL" : type.toString()),
                    LogManager.DEBUG_MESSAGE_LEVEL);
        
        //always returns a list , empty in case of no results
        result = mTCStore.getEntries( Separator.combine(namespace, name, version), resourceid, type );
        
        //API dictates we return null in case of empty
        return result.isEmpty() ? null : result;
    }

    /**
     * Get the list of Resource ID's where a particular transformation may reside.
     *
     * @param   namespace String The namespace of the transformation to search for.
     * @param   name      String The name of the transformation to search for.
     * @param   version   String The version of the transformation to search for.
     * @param   type      TCType The type of the transformation to search for.<BR>
     *                    (Enumerated type includes SOURCE, STATIC-BINARY,
     *                    DYNAMIC-BINARY, PACMAN, INSTALLED, SCRIPT)<BR>
     *                     If <B>NULL</B> it returns all types.
     *
     * @return  a list of Resource Id's as strings.
     *          Returns <B>NULL</B> if no results found.
     *
     * @throws Exception
     * @see org.griphyn.common.classes.TCType
     */
    public List getTCResourceIds(String namespace, String name,
                                 String version,
                                 TCType type) throws Exception {
        logMessage(
            "List getTCResourceIds(String namespace, String name, String " +
            "version, TCType type");
        logMessage("\t getTCResourceIds(" + namespace + ", " + name + ", " +
                   version +
                   ", " + type);



        //retrieve all entries for a transformation, matching a tc type
        List<TransformationCatalogEntry> entries = this.getTCEntries( namespace, name, version, (String)null, type );

        Set<String> result = new HashSet();
        for( TransformationCatalogEntry entry : entries ){
            result.add( entry.getResourceId() );
        }

        //API dictates we return null in case of empty
        return result.isEmpty() ? null : new LinkedList( result );

        
    }

    /**
     * Get the list of PhysicalNames for a particular transformation on a
     * site/sites for a particular type/types.
     *
     * @param  namespace  the namespace of the transformation to search for.
     * @param  name       the name of the transformation to search for.
     * @param  version    the version of the transformation to search for.
     * @param  resourceid the id of the resource on which you want to search. <BR>
     *                    If <B>NULL</B> then returns entries on all resources
     * @param  type       the type of the transformation to search for. <BR>
     *                    (Enumerated type includes source, binary, dynamic-binary,
     *                     pacman, installed)<BR>
     *                     If <B>NULL</B> then returns entries of all types.
     *
     * @throws Exception
     * @return List       a list of String Arrays.
     *                    Each array contains the resourceid,
     *                    the physical transformation, the type of the tr and
     *                    the systeminfo.
     *                    The last entry in the List is a int array containing
     *                    the column lengths for pretty print.
     *                    Returns <B>NULL</B> if no results found.
     *
     * @see org.griphyn.common.classes.TCType
     * @see org.griphyn.common.classes.VDSSysInfo
     */
    public List getTCPhysicalNames(String namespace, String name,
                                   String version,
                                   String resourceid, TCType type) throws
        Exception {
        logMessage("List getTCPhysicalNames(String namespace, String name," +
                   "String version, String resourceid,TCType type)");
        logMessage("\t getTCPhysicalNames(" + namespace + ", " + name + ", " +
                   version + ", " + resourceid + ", " + type + ")");
        
        
        //retrieve all entries for a transformation, matching a tc type
        List<TransformationCatalogEntry> entries = this.getTCEntries( namespace, name, version, resourceid, type );
        
        List result = new LinkedList();

        //dont know what count does and why this for pretty print.
        //ask gaurang. Karan June 11, 2010
        int count[] = {0, 0, 0};
        for( TransformationCatalogEntry entry : entries ){
            result.add( entry.getPhysicalTransformation()  );
            String[] s = {
                        entry.getResourceId(),
                        entry.getPhysicalTransformation(),
                        entry.getType().toString(),
                        entry.getSysInfo().toString()};

            columnLength(s, count);
        }

        //API dictates we return null in case of empty
        if( result.isEmpty() ){
            return null;
        }
        else{
            result.add(count);
        }
        return result;
    }

    /**
     * Gets the list of LogicalNames available on a particular resource.
     *
     * @param resourceid the id of the resource on which you want to search
     * @param type       the type of the transformation to search for. <BR>
     *                   (Enumerated type includes source, binary, dynamic-binary,
     *                    pacman, installed)<BR>
     *                   If <B>NULL</B> then return logical name for all types.
     *
     * @throws Exception
     * @return List      Returns a list of String Arrays.
     *                   Each array contains the resourceid, logical transformation
     *                   in the format namespace::name:version and type.
     *                   The last entry in the list is an array of integers
     *                   specifying the column length for pretty print.
     *                   Returns <B>NULL</B> if no results found.
     */
    public List getTCLogicalNames(String resourceid, TCType type) throws
        Exception {
        logMessage("List getTCLogicalNames(String resourceid, TCType type)");
        logMessage("\t getTCLogicalNames(" + resourceid + "," + type + ")");

        List<TransformationCatalogEntry> entries = mTCStore.getEntries( resourceid, type );
        int[] length = {0, 0};

        //convert the list into the format Gaurang wants for the API.
        List result = new LinkedList();
        for( TransformationCatalogEntry entry: entries ){
            String l = entry.getLogicalTransformation();
            String r = entry.getResourceId();
            String t = entry.getType().toString();

            String[] s = { l, r, t};
            columnLength(s, length);
        }

        //API dictates we return null in case of empty
        if( result.isEmpty() ){
            return null;
        }
        else{
            result.add( length );
        }
        return result;


    }

    /**
     * Get the list of Profiles associated with a particular logical transformation.
     *
     * @param namespace  the namespace of the transformation to search for.
     * @param name       the name of the transformation to search for.
     * @param version    the version of the transformation to search for.
     *
     * @throws NotImplementedException as not implemented as yet.
     *
     * @return List      Returns a list of Profile Objects containing profiles
     *                   assocaited with the transformation.
     *                   Returns <B>NULL</B> if no profiles found.
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public List getTCLfnProfiles(String namespace, String name,
                                 String version) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Get the list of Profiles associated with a particular physical transformation.
     *
     * @param pfn        the physical file name to search the transformation by.
     * @param resourceid the id of the resource on which you want to search.
     * @param type       the type of the transformation to search for. <br>
     *                   (Enumerated type includes source, binary, dynamic-binary,
     *                    pacman, installed)<br>
     *
     * @throws Exception
     * @return a list of <code>Profile</code> containing profiles assocaited with
     *         the transformation. Returns <B>NULL</B> if no profiless found.
     *
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public List getTCPfnProfiles(String pfn, String resourceid, TCType type) throws
        Exception {
        logMessage(
            "getTCPfnProfiles(String pfn, String resourceid, TCType type)");
        logMessage("\t getTCPfnProfiles(" + pfn + "," + resourceid + "," +
                   type + ")");

        List result = null;
        List lfnMap = new ArrayList();
        if (mTreeMap.containsKey(resourceid)) {
            lfnMap.add( (Map) mTreeMap.get(resourceid));
        }
        for (Iterator i = lfnMap.iterator(); i.hasNext(); ) {
            for (Iterator j = ( (Map) i.next()).values().iterator();
                 j.hasNext(); ) {
                for (Iterator k = ( (List) j.next()).iterator(); k.hasNext(); ) {
                    TransformationCatalogEntry tc = (
                        TransformationCatalogEntry) k.next();
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
     * List all the contents of the TC in a column format.
     *
     * @return a list of String Arrays. Each string array contains the
     *         resource, lfn, pfn, type, sysinfo and profiles.
     *         The last entry in the list is an array of integers which contain
     *         the column lengths for pretty print.
     *
     * @throws Exception
     */

    public List getTC() throws Exception {
        List result = new ArrayList();
        for (Iterator i = mTreeMap.values().iterator(); i.hasNext(); ) {
            for (Iterator j = ( (Map) i.next()).values().iterator();
                 j.hasNext(); ) {
                for (Iterator k = ( (List) j.next()).iterator(); k.hasNext(); ) {
                    TransformationCatalogEntry tc = (
                        TransformationCatalogEntry) k.next();
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

    /**
     *  ADDITIONS
     */

    /**
     * Add multiple TCEntries to the Catalog. Exception is thrown when error
     * occurs.
     *
     * @param entries list of {@link org.griphyn.common.catalog.TransformationCatalogEntry}
     * objects as input.
     *
     * @return boolean Return true if succesful, false if error.
     *
     * @throws Exception
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    public boolean addTCEntry(List entries) throws
        Exception {
        for (int i = 0; i < entries.size(); i++) {
            TransformationCatalogEntry entry = ( (TransformationCatalogEntry)
                                                entries.get(i));
            this.addTCEntry(entry);
        }
        return true;

    }

    /**
     * Add a single TCEntry to the Catalog. Exception is thrown when error
     * occurs.
     *
     * @param entry a single {@link org.griphyn.common.catalog.TransformationCatalogEntry}
     * object as input.
     *
     * @return boolean Return true if succesful, false if error.
     *
     * @throws Exception
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    public boolean addTCEntry(TransformationCatalogEntry entry) throws
        Exception {
        this.addTCEntry(entry.getLogicalNamespace(),
                        entry.getLogicalName(), entry.getLogicalVersion(),
                        entry.getPhysicalTransformation(),
                        entry.getType(), entry.getResourceId(), null,
                        entry.getProfiles(), entry.getSysInfo());
        return true;
    }

    /**
     * Add a single TCEntry to the Catalog. Exception is thrown when error
     * occurs. This method is a hack and wont commit the additions to the
     * backend catalog
     *
     * @param entry a single {@link org.griphyn.common.catalog.TransformationCatalogEntry}
     * object as input.
     * @param write boolean to commit additions to backend catalog.
     * @return boolean Return true if succesful, false if error.
     *
     * @throws Exception
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    public boolean addTCEntry(TransformationCatalogEntry entry, boolean write) throws
        Exception {
        this.addTCEntry(entry.getLogicalNamespace(),
                        entry.getLogicalName(), entry.getLogicalVersion(),
                        entry.getPhysicalTransformation(),
                        entry.getType(), entry.getResourceId(), null,
                        entry.getProfiles(), entry.getSysInfo(), write);
        return true;
    }

    /**
     * Add an single entry into the transformation catalog.
     *
     * @param namespace    the namespace of the transformation to be added (Can be null)
     * @param name         the name of the transformation to be added.
     * @param version      the version of the transformation to be added. (Can be null)
     * @param physicalname the physical name/location of the transformation to be added.
     * @param type         the type of the physical transformation.
     * @param resourceid   the resource location id where the transformation is located.
     * @param lfnprofiles  the List of <code>Profile</code> objects associated
     *                     with a Logical Transformation. (can be null)
     * @param pfnprofiles  the list of <code>Profile</code> objects associated
     *                     with a Physical Transformation. (can be null)
     * @param system       the System information associated with a physical
     *                     transformation.
     * @return boolean     true if succesfully added, returns false if error and
     *                     throws exception.
     *
     * @throws Exception
     *
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     * @see org.griphyn.common.classes.VDSSysInfo
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public boolean addTCEntry(String namespace, String name,
                              String version,
                              String physicalname, TCType type,
                              String resourceid,
                              List pfnprofiles, List lfnprofiles,
                              SysInfo system) throws
        Exception {
        return this.addTCEntry(namespace, name, version, physicalname, type,
                               resourceid, lfnprofiles, pfnprofiles, system, true);
    }

    /**
     * Add an single entry into the transformation catalog.
     *
     * @param namespace    the namespace of the transformation to be added (Can be null)
     * @param name         the name of the transformation to be added.
     * @param version      the version of the transformation to be added. (Can be null)
     * @param physicalname the physical name/location of the transformation to be added.
     * @param type         the type of the physical transformation.
     * @param resourceid   the resource location id where the transformation is located.
     * @param lfnprofiles  the List of <code>Profile</code> objects associated
     *                     with a Logical Transformation. (can be null)
     * @param pfnprofiles  the list of <code>Profile</code> objects associated
     *                     with a Physical Transformation. (can be null)
     * @param system       the System information associated with a physical
     *                     transformation.
     * @param write        boolean to commit changes to backend catalog
     * @return boolean     true if succesfully added, returns false if error and
     *                     throws exception.
     *
     * @throws Exception
     *
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     * @see org.griphyn.common.classes.VDSSysInfo
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public boolean addTCEntry(String namespace, String name,
                              String version,
                              String physicalname, TCType type,
                              String resourceid,
                              List pfnprofiles, List lfnprofiles,
                              SysInfo system, boolean write) throws
        Exception {

        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        entry.setLogicalNamespace(namespace);
        entry.setLogicalName(name);
        entry.setLogicalVersion(version);
        entry.setPhysicalTransformation(physicalname);
        entry.setType(type);
        entry.setResourceId(resourceid);
        entry.addProfiles(lfnprofiles);
        entry.addProfiles(pfnprofiles);
        entry.setVDSSysInfo( NMI2VDSSysInfo.nmiToVDSSysInfo(system) );

        Map lfnMap = null;
        if (mTreeMap.containsKey(resourceid)) {
            lfnMap = (Map) mTreeMap.get(resourceid);
        }
        else {
            lfnMap = new TreeMap();
            mTreeMap.put(resourceid, lfnMap);
        }

        List pfnList = null;
        if (lfnMap.containsKey(entry.getLogicalTransformation())) {
            pfnList = (List) lfnMap.get(entry.getLogicalTransformation());
        }
        else {
            pfnList = new ArrayList(2);
            lfnMap.put(entry.getLogicalTransformation(), pfnList);
        }
        boolean add = true;
        for (Iterator i = pfnList.iterator(); i.hasNext(); ) {
            TransformationCatalogEntry test = (TransformationCatalogEntry) i.
                next();
            if (test.equals(entry)) {
                add = false;
            }
        }
        if (add) {
            pfnList.add(entry);
            if (write) {
                writeTC();
            }
        }
        else {
            mLogger.log("TC Entry already exists. Skipping",
                        LogManager.DEBUG_MESSAGE_LEVEL);
        }
        return true;

    }

    /**
     * Add additional profiles to a matching logical transformation.
     *
     * @param namespace the nsamespace of the transformation to be added. (can be null)
     * @param name      the name of the transformation to be added.
     * @param version   the version of the transformation to be added.
     * @param profiles  list of <code>Profile</code> objects that are to be
     *                  added to the transformation.
     *
     * @return boolean
     * @throws Exception as function not implemented.
     */
    public boolean addTCLfnProfile(String namespace, String name,
                                   String version,
                                   List profiles) throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Adds additional profiles to a physical transformation.
     *
     * @param pfn          the physical name of the transformation
     * @param type         the type of transformation that the profile is
     *                     associated with. If null the profile is associated
     *                     with all the types.
     * @param resourcename the resource on which the physical transformation exists.
     * @param profiles     the List of <code>Profile</code> objects that are to
     *                     be added to the transformation.
     *
     * @return boolean
     * @throws Exception as function not implemented.
     */
    public boolean addTCPfnProfile(String pfn, TCType type,
                                   String resourcename,
                                   List profiles) throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * DELETIONS
     */

    /**
     * Delete all entries in the transformation catalog of the logical
     * transformation either at all resource or on a particular resource
     *
     * @param namespace   the nsamespace of the transformation to be added. (can be null)
     * @param name        the name of the transformation to be added.
     * @param version     the version of the transformation to be added.
     * @param resourceid  the resource id for which the transformation is to be
     *                    deleted. If null then transformation on all resource
     *                    are deleted.
     * @param type        the type of the transformation
     *
     * @throws Exception
     * @return boolean
     */
    public boolean deleteTCbyLogicalName(String namespace, String name,
                                         String version, String resourceid,
                                         TCType type) throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public boolean deleteTCbyPhysicalName(String physicalname,
                                          String namespace,
                                          String name, String version,
                                          String resourceid, TCType type) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Delete a paricular type of transformation, additionally either on all
     * resources or on a particular resource.
     *
     * @param type        the type of the transformation.
     * @param resourceid  the resource on which the transformation exists.
     *                    If null then that type of transformation is deleted
     *                    from all the resources.
     *
     * @throws Exception as function not implemented.
     * @return boolean
     */
    public boolean deleteTCbyType(TCType type, String resourceid) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Delete entries in the catalog of a particular systeminfo.
     *
     * @param sysinfo SysInfo
     *
     * @throws Exception as function not implemented.
     * @return boolean
     */
    public boolean deleteTCbySysInfo( SysInfo sysinfo) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Delete an entire resource from the transformation catalog.
     * @param resourceid String
     *
     * @return boolean
     * @throws Exception as function not implemented.
     */
    public boolean deleteTCbyResourceId(String resourceid) throws Exception {

        if (mTreeMap.containsKey(resourceid)) {
            mTreeMap.remove(resourceid);
        }
        writeTC();
        return true;
    }

    /**
     * Deletes the entire transformation catalog. Whoopa.....
     *
     * @return boolean
     * @throws Exception
     */
    public boolean deleteTC() throws Exception {
        mTreeMap.clear();
        return true;
    }

    public boolean deleteTCPfnProfile(String physicalname, TCType type,
                                      String resourceid, List profiles) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public boolean deleteTCLfnProfile(String namespace, String name,
                                      String version, List profiles) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public boolean connect(java.util.Properties props) {
        //not implemented
        return true;
    }

    public boolean isClosed() {
        //not impelemented
        return true;
    }

    public void close() {
        //not impelemented
    }

    private void writeTC() {
        PrintWriter writer = null;
        try {
            mLogger.log("Starting to write the TC file",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            writer = new PrintWriter(new BufferedWriter(new FileWriter(
                mTCFile, false)));

        }
        catch (IOException e) {
            mLogger.log(
                "Unable to open TC File for writing\"" + mTCFile, e,
                LogManager.ERROR_MESSAGE_LEVEL);
        }
        int count = 0;
        for (Iterator i = mTreeMap.values().iterator(); i.hasNext(); ) {
            //get all the values from the main map
            for (Iterator j = ( (Map) i.next()).values().iterator();
                 j.hasNext(); ) {
                //for each resource and each logical transformatino get the arraylist.
                for (Iterator k = ( (List) j.next()).iterator(); k.hasNext(); ) {
                    //start printing each entry
                    writer.println( ( (TransformationCatalogEntry) k.next()).
                                   toTCString());
                    count++;

                }

            }
        }
        mLogger.log("Written " + count +
                    " entries back to the TC file",
                    LogManager.DEBUG_MESSAGE_LEVEL);
        writer.flush();
        writer.close();
        mLogger.log( "Starting to write the TC file - DONE",
                     LogManager.DEBUG_MESSAGE_LEVEL);
    }

    /**
     * Computes the maximum column lenght for pretty printing.
     *
     * @param s String[]
     * @param count int[]
     */
    private static void columnLength(String[] s, int[] count) {
        for (int i = 0; i < count.length; i++) {
            if (s[i].length() > count[i]) {
                count[i] = s[i].length();
            }
        }

    }

    /**
     * Populates the internal copy of the transformation catalog from a byte
     * stream (input stream). Used in webservices, when clients upload their files.
     * It uses the default character encoding.
     *
     * @param reader  the <code>InputStrean</code> containing the bytes to be
     *                read.
     * @return boolean
     */
    private boolean populateTC(InputStream reader) {
        return populateTC(new InputStreamReader(reader));
    }

    /**
     * Populates the internal copy of the transformation catalog from the file
     * containing the transformation catalog in the 6 column format.
     *
     * @return boolean
     */
    private boolean populateTC() {
        boolean result = false;

        try {
            result = populateTC(new FileReader(mTCFile));
        }
        catch (FileNotFoundException ex) {
            mLogger.log("The tc text file " + mTCFile +
                        " was not found", LogManager.ERROR_MESSAGE_LEVEL);
            mLogger.log("Considering it as Empty TC",
                        LogManager.ERROR_MESSAGE_LEVEL);
            return true;
        }
        catch (IOException e) {
            mLogger.log("Unable to open the file " +
                        mTCFile, e, LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        return result;
    }

    /**
     * Adds multiple entries into the TC.  Calls the above api multiple times.
     *
     * @param reader  the input stream from where to read the contents of the
     *                transformation catalog.
     * @return boolean
     */
    private boolean populateTC(Reader reader) {
        BufferedReader buf = new BufferedReader(reader);
        // String profilestring = null;
        int linecount = 0;
        int count = 0;
        try {
            String line = null;
            //buf = new BufferedReader( new FileReader( mTCFile ) );
            while ( (line = buf.readLine()) != null) {
                linecount++;
                if (! (line.startsWith("#") ||
                       line.trim().equalsIgnoreCase(""))) {
                    TransformationCatalogEntry tc = new
                        TransformationCatalogEntry();
                    String[] tokens = line.split("[ \t]+", 6);
                    for (int i = 0; i < tokens.length; i++) {
                        switch (i) {
                            case 0: //poolname
                                tc.setResourceId(tokens[i]);
                                break;
                            case 1: //logical transformation name
                                if (tokens[i].indexOf("__") != -1) {
                                    mLogger.log(
                                        "Logical Transformations in the new File TC " +
                                        "are represented as NS::NAME:VER",
                                        LogManager.ERROR_MESSAGE_LEVEL);
                                    mLogger.log("Assuming " + tokens[i] +
                                                " as just the transformation NAME.",
                                                LogManager.DEBUG_MESSAGE_LEVEL);
                                }
                                tc.setLogicalTransformation(tokens[i]);
                                break;
                            case 2: //pfn
                                tc.setPhysicalTransformation(tokens[i]);
                                break;
                            case 3: //type
                                tc.setType( (tokens[i].equalsIgnoreCase(
                                    "null")) ?
                                           TCType.INSTALLED :
                                           TCType.fromString(tokens[i]));
                                break;
                            case 4: //systeminfo
                                tc.setVDSSysInfo( (tokens[i].equalsIgnoreCase(
                                    "null")) ?
                                              new VDSSysInfo(null) :
                                              new VDSSysInfo(tokens[i]));
                                break;
                            case 5: //profile string
                                if (!tokens[i].equalsIgnoreCase("null")) {
                                    try {
                                        tc.addProfiles(ProfileParser.parse(
                                            tokens[
                                            i]));
                                    }
                                    catch (ProfileParserException ppe) {
                                        mLogger.log(
                                            "Parsing profiles on line " +
                                            linecount + " " + ppe.getMessage() +
                                            "at position " +
                                            ppe.getPosition(), ppe,
                                            LogManager.ERROR_MESSAGE_LEVEL);

                                    }
                                    catch (RuntimeException e) {
                                        mLogger.log(
                                            "Ignoring errors while parsing profile in Transformation Catalog on line " +
                                            linecount, e,
                                            LogManager.WARNING_MESSAGE_LEVEL);
                                    }
                                }
                                break;
                            default:
                                mLogger.log("Line " + linecount +
                                            " : Humm no need to be in default",
                                            LogManager.ERROR_MESSAGE_LEVEL);
                        } //end of switch
                    } //end of for loop
                    // if (count > 0) {

                    //   mLogger.logMessage("Loading line number" + linecount +
                    //                    " to the map", 1);
                    Map lfnMap = null;
                    if (!mTreeMap.containsKey(tc.getResourceId())) {
                        lfnMap = new TreeMap();
                    }
                    else {
                        lfnMap = (Map) mTreeMap.get(tc.getResourceId());
                    }
                    List entries = null;
                    if (!lfnMap.containsKey(tc.getLogicalTransformation())) {
                        entries = new ArrayList(3);
                    }
                    else {
                        entries = (List) lfnMap.get(tc.
                            getLogicalTransformation());
                    }
                    entries.add(tc);
                    lfnMap.put(tc.getLogicalTransformation(), entries);
                    mTreeMap.put(tc.getResourceId(), lfnMap);
                    count++;
                } //end of if "#"
            } //end of while line
            mLogger.log("Loaded " + count + " entries to the TC Map",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            buf.close();
            return true;
        }
        catch (FileNotFoundException ex) {
            mLogger.log("The tc text file " + mTCFile +
                        " was not found", LogManager.ERROR_MESSAGE_LEVEL);
            mLogger.log("Considering it as Empty TC",
                        LogManager.ERROR_MESSAGE_LEVEL);
            return true;
        }
        catch (IOException e) {
            mLogger.log("Unable to open the file " +
                        mTCFile, e, LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        catch (IllegalStateException e) {
            mLogger.log("On line " + linecount + "in File " +
                        mTCFile + "\n", e, LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        catch (Exception e) {
            mLogger.log(
                "While loading entries into the map on line " + linecount +
                "\n", e, LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
    }

    /**
     * Logs the message to a logging stream. Currently does not log to any stream.
     *
     * @param msg  the message to be logged.
     */
    protected void logMessage(String msg) {
        //mLogger.logMessage("[Shishir] Transformation Catalog : " + msg);
    }

}
