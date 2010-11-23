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

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.Currently;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.NMI2VDSSysInfo;

import edu.isi.pegasus.common.util.Separator;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.parser.TransformationCatalogTextParser;

import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.Writer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * A File based Transformation Catalog where each entry spans multiple lines.
 *
 * The implementation refers to the following same format for specifying a
 * transformation catalog entry.
 *
 *
 * <pre>
 * tr example::keg:1.0 {
 *
 *  #specify profiles that apply for all the sites for the transformation
 *  #in each site entry the profile can be overriden
 *  profile env "APP_HOME" "/tmp/karan"
 *  profile env "JAVA_HOME" "/bin/java.1.5"
 * 
 *  site isi {
 *   profile env "me" "with"
 *   profile condor "more" "test"
 *   profile env "JAVA_HOME" "/bin/java.1.6"
 *   pfn "/path/to/keg"
 *   arch  "x86"
 *   os    "linux"
 *   osrelease "fc"
 *   osversion "4"
 *   type "installed"
 *  }
 *
 *  site wind {
 *   profile env "me" "with"
 *   profile condor "more" "test"
 *   pfn "/path/to/keg"
 *   arch  "x86"
 *   os    "linux"
 *   osrelease "fc"
 *   osversion "4"
 *   type "STAGEABLE"
 *  }
 * }
 *
 * </pre>
 *
 *
 *
 * @author Karan Vahi
 * @version $Revision: 2183 $
 */
public class Text extends Abstract
    implements TransformationCatalog  {

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
     * Boolean indicating whether to flush the contents back to the file on 
     * close.
     */
    private boolean mFlushOnClose;
    

    /**
     * Default constructor.
     */
    public Text(){
    }
    
    /**
     * Initialize the implementation, and return an instance of the implementation.
     * It should be in the connect method, to be consistent with the other
     * catalogs.
     * 
     * @param bag  the bag of Pegasus initialization objects.
     * 
     */
    public void initialize ( PegasusBag bag ){
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        mFlushOnClose = false;
        
        mTCFile = mProps.getTCPath();
        mLogger.log("TC Mode being used is " + this.getDescription(),
                    LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("TC File being used is " + mTCFile,
                    LogManager.CONFIG_MESSAGE_LEVEL);
        
        if (mTCFile == null) {
            throw new RuntimeException( "The File to be used as TC should be " +
                                        "defined with the property pegasus.catalog.transformation.file");
        }        


        try{
            java.io.File f = new java.io.File(  mTCFile );
            
            if( f.exists() || f.createNewFile() ){
                mTextParser = new TransformationCatalogTextParser ( new FileReader( f ),
                                                                    mLogger );
                mTCStore = mTextParser.parse();
            }
            else{
                //empty TCStore
                mTCStore = new TransformationStore();
                mLogger.log("The Transformation Catalog file " + mTCFile +
                        " was not found - Will consider only TC Entries from the DAX", LogManager.WARNING_MESSAGE_LEVEL);
            }
        }
        catch (FileNotFoundException ex) {
            throw new RuntimeException( "Unable to find file " + mTCFile );
        }
        catch( IOException ioe ){
            throw new RuntimeException( "IOException while parsing transformation catalog" , ioe );
        }


    }
    
    /**
     * Empty for the time being. The factory still calls out to the initialize
     * method.
     * 
     * @param props   the connection properties.
     * @return
     */
    public boolean connect( Properties props ) {
        //not implemented
        return true;
    }

    /**
     * Returns whether the connection is closed or not.
     * 
     * @return
     */
    public boolean isClosed() {
        //not implemented
        return this.mTCStore == null ;
    }

    /**
     * Closes the connection to the back end.
     */
    public void close() {
        if( mFlushOnClose ){
            //we flush back the contents of the internal store to the file.
            String newline = System.getProperty("line.separator", "\r\n");
            String indent = "";
            try {
                // open
                Writer out = new BufferedWriter( new FileWriter( mTCFile ) );
                String newIndent = indent + "\t";
                
                // write header
                out.write( "# multiple line text-based transformation catalog: " +
                             Currently.iso8601(false,true,true,new Date()) );
                out.write( newline );

                // write out data
                //traverse through all the logical transformations in the
                //catalog
                for ( Iterator i= this.mTCStore.getTransformations(null, null).iterator(); i.hasNext(); ) {
                    //transformation is the complete name comprised of namespace,name,version
                    String transformation = (String) i.next();
                    
                    out.write( indent );
                    out.write( "tr " );
                    out.write( transformation );
                    out.write( " {"  );
                    out.write( newline );
	
                    //get all the entries for that transformations on all sites
                    for( TransformationCatalogEntry entry: mTCStore.getEntries( transformation, (String)null ) ){
                        //write out all the entries for the transformation
                        out.write(  toText( entry, newline, newIndent ) );
                    }
                
                    
                    out.write( indent );
                    out.write( "}"  );
                    out.write( newline );
                    out.write( newline );
                }

                // close
                out.close();
                this.mFlushOnClose = false;
            }
            catch ( IOException ioe ) {
                throw new RuntimeException( "Unable to write contents of TC to " + mTCFile,
                                            ioe );
            } 
            finally {
                this.mTCStore = null;
                this.mTCFile = null;
            }
        }
    }
    
    /**
     * Converts the transformation catalog entry object to the multi line
     * textual representation. e.g.
     * 
     * site wind {
     *   profile env "me" "with"
     *   profile condor "more" "test"
     *   pfn "/path/to/keg"
     *   arch  "x86"
     *   os    "linux"
     *   osrelease "fc"
     *   osversion "4"
     *   type "STAGEABLE"
     *  }
     * 
     * @param entry   the transformation catalog entry
     * @param newline the newline characters
     * @param indent  the indentation to use
     * 
     * @return the textual description
     */
    public String toText( TransformationCatalogEntry entry, String newline , String indent ){
        StringBuffer sb = new StringBuffer();
        indent = (indent != null && indent.length() > 0 ) ?
                 indent:
                 "";
        String newIndent = indent + "\t";
        
        sb.append( indent );
        sb.append( "site" ).append( " " ).append( entry.getResourceId() ).append( " {" ).append( newline );
                
        //list out all the profiles
        List<Profile> profiles = entry.getProfiles();
        if( profiles != null ){
            for( Profile p : profiles ){
                sb.append( newIndent ).append( "profile" ).append( " " ).
                   append( p.getProfileNamespace() ).append( " " ).
                   append( quote( p.getProfileKey() ) ).append( " ").
                   append( quote( p.getProfileValue())).append( " ").
                   append( newline );
            }
        }
        
        //write out the pfn
        addLineToText( sb, newIndent, newline, "pfn", entry.getPhysicalTransformation() );
        
        //write out sysinfo
        SysInfo s = entry.getSysInfo();
        SysInfo.Architecture arch = s.getArchitecture();
        if( arch != null ){
            addLineToText( sb, newIndent, newline, "arch", arch.toString() );
        }
        SysInfo.OS os = s.getOS();
        if( os != null ){
            addLineToText( sb, newIndent, newline, "os", os.toString() );
        }
        String osrelease = s.getOSRelease();
        if( osrelease != null && osrelease.length() > 0 ){
            addLineToText( sb, newIndent, newline, "osrelease", osrelease );
        }
        String osversion = s.getOSVersion();
        if( osversion != null && osversion.length() > 0 ){
            addLineToText( sb, newIndent, newline, "osversion", osversion );
        }
        String glibc = s.getGlibc();
        if( glibc != null && glibc.length() > 0 ){
            addLineToText( sb, newIndent, newline, "glibc", glibc );
        }
        
        //write out the type
        addLineToText( sb, newIndent, newline, "type", entry.getType().toString() );
        
                
        sb.append( indent ).append( "}" ).append( newline );
        
        return sb.toString();
    }
    
    
    /**
     * Convenience method to add a line to the internal textual representation.
     * 
     * @param sb         the StringBuffer to which contents are to be added.
     * @param newIndent  the indentation
     * @paran newline    the newline character
     * @param key        the key
     * @param value      the value
     */
    protected void addLineToText( StringBuffer sb, 
                                  String newIndent, 
                                  String newline,
                                  String key, 
                                  String value) {
        
        sb.append( newIndent ).append( key ).append( " " ).
           append( quote( value ) ).append( newline );
    }
    
    
    
    /**
     * Quotes a String.
     * 
     * @param str  the String to be quoted.
     * 
     * @return quoted version
     */
    public String quote( String str ){
        //maybe should use the escape class also?
        StringBuffer sb = new StringBuffer();
        sb.append( "\"" ).append( str ).append( "\"" );
        return sb.toString();
    }

    /**
     * Returns a textual description of the transformation mode.
     *
     * @return String containing the description.
     */
    public String getDescription() {
        return Text.DESCRIPTION;
    }

    /**
     * Returns TC entries for a particular logical transformation and/or on a
     * number of resources and/or of a particular type.
     * 
     * @param namespace String The namespace of the logical transformation.
     * @param name String the name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param resourceids List The List resourceid where the transformation is located. 
     *                    If <b>NULL</b> it returns all resources.
     * @param type TCType The type of the transformation to search for.
     *              If <b>NULL</b> it returns all types.
     * 
     * @return List Returns a list of TransformationCatalogEntry objects containing 
     *          the corresponding entries from the TC. Returns null if no entry found.
     * @throws Exception
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public List<TransformationCatalogEntry> lookup( String namespace, String name, String version,
        List resourceids, TCType type ) throws Exception {
        logMessage("getTCEntries(String namespace,String name,String version," +
                   "List resourceids, TCType type");
        logMessage("\tgetTCEntries(" + namespace + ", " + name + ", " +
                   version + ", " +
                   resourceids + ", " + type);
        List results = null;
        if (resourceids != null) {
            for (Iterator i = resourceids.iterator(); i.hasNext(); ) {
                List tempresults = lookup(namespace, name, version,
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
            List tempresults = lookup(namespace, name, version, (String)null,
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
     * @param namespace String The namespace of the logical transformation.
     * @param name String the name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param resourceid String The resourceid where the transformation is located.
     *                  If <B>NULL</B> it returns all resources.
     * @param type  TCType The type of the transformation to search for.
     *              If <B>NULL</b> it returns all types.
     * 
     * @return List Returns a list of TransformationCatalogEntry objects 
     *         containing the corresponding entries from the TC.
     *         Returns null if no entry found.
     * @throws Exception
     * 
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public List<TransformationCatalogEntry> lookup( String namespace, String name, String version,
        String resourceid, TCType type ) throws Exception {
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
        return ( result == null || result.isEmpty() ) ? null : result;
    }

    /**
     * Get the list of Resource ID's where a particular transformation may reside.
     * @param   namespace String The namespace of the transformation to search for.
     * @param   name      String The name of the transformation to search for.
     * @param   version   String The version of the transformation to search for.
     * @param   type      TCType The type of the transformation to search for.<BR>
     *                    (Enumerated type includes SOURCE, STATIC-BINARY, DYNAMIC-BINARY, PACMAN, INSTALLED, SCRIPT)<BR>
     *                     If <B>NULL</B> it returns all types.
     * 
     * @return  List      Returns a list of Resource Id's as strings. Returns <B>NULL</B> if no results found.
     * 
     * @throws  Exception NotImplementedException if not implemented
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     */
    public List<String> lookupSites( String namespace, String name, String version,
        TCType type ) throws Exception {
        logMessage(
            "List getTCResourceIds(String namespace, String name, String " +
            "version, TCType type");
        logMessage("\t getTCResourceIds(" + namespace + ", " + name + ", " +
                   version +
                   ", " + type);



        //retrieve all entries for a transformation, matching a tc type
        List<TransformationCatalogEntry> entries = this.lookup( namespace, name, version, (String)null, type );

        Set<String> result = new HashSet();
        for( TransformationCatalogEntry entry : entries ){
            result.add( entry.getResourceId() );
        }

        //API dictates we return null in case of empty
        return ( result == null ||result.isEmpty()) ? null : new LinkedList( result );

        
    }

    /**
     * Get the list of PhysicalNames for a particular transformation on a site/sites 
     * for a particular type/types;
     * 
     * @param  namespace  String The namespace of the transformation to search for.
     * @param  name       String The name of the transformation to search for.
     * @param  version    String The version of the transformation to search for.
     * @param  resourceid String The id of the resource on which you want to search. <BR>
     *                    If <B>NULL</B> then returns entries on all resources
     * @param  type       TCType The type of the transformation to search for. <BR>
     *                        (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<BR>
     *                        If <B>NULL</B> then returns entries of all types.
     * 
     * @return List       Returns a List of <TransformationCatalongEntry> objects 
     *                    with the profiles not populated.
     * 
     * @throws Exception  NotImplementedException if not implemented.
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo
     */
    public List <TransformationCatalogEntry> lookupNoProfiles( String namespace, 
           String name,String version,String resourceid, TCType type ) throws Exception {
        logMessage("List getTCPhysicalNames(String namespace, String name," +
                   "String version, String resourceid,TCType type)");
        logMessage("\t getTCPhysicalNames(" + namespace + ", " + name + ", " +
                   version + ", " + resourceid + ", " + type + ")");
        
        
        //retrieve all entries for a transformation, matching a tc type
        List<TransformationCatalogEntry> entries = this.lookup( namespace, name, version, resourceid, type );
        
        List result = entries;

        //API dictates we return null in case of empty
        if( result == null || result.isEmpty() ){
            return null;
        }
        
        return result;
    }

    /**
     * Get the list of LogicalNames available on a particular resource.
     * @param resourceid String The id of the resource on which you want to search
     * @param type       TCType The type of the transformation to search for. <BR>
     *                  (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<BR>
     *                   If <B>NULL</B> then return logical name for all types.
     * 
     * @return List      Returns a list of String Arrays.
     *                   Each array contains the resourceid, logical transformation
     *                   in the format namespace::name:version and type.
     *                   Returns <B>NULL</B> if no results found.
     * 
     * @throws Exception  NotImplementedException if not implemented.
     */
    public List<String[]> getTCLogicalNames( String resourceid, TCType type ) throws
        Exception {
        logMessage("List getTCLogicalNames(String resourceid, TCType type)");
        logMessage("\t getTCLogicalNames(" + resourceid + "," + type + ")");

        List<TransformationCatalogEntry> entries = mTCStore.getEntries( resourceid, type ); 
                

        //convert the list into the format Gaurang wants for the API.
        List result = new LinkedList();
        for( TransformationCatalogEntry entry: entries ){
            String l = entry.getLogicalTransformation();
            String r = entry.getResourceId();
            String t = entry.getType().toString();

            String[] s = { l, r, t};
            result.add( s );
        }

        //API dictates we return null in case of empty
        if( result == null || result.isEmpty() ){
            return null;
        }
        
        return result;


    }

    /**
     * Get the list of Profiles associated with a particular logical transformation.
     * @param namespace  String The namespace of the transformation to search for.
     * @param name       String The name of the transformation to search for.
     * @param version    String The version of the transformation to search for.
     * 
     * @return List      Returns a list of Profile Objects containing profiles
     *                   assocaited with the transformation.
     *                   Returns <B>NULL</B> if no profiles found.
     * 
     * @throws Exception NotImplementedException if not implemented.
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public List<Profile> lookupLFNProfiles( String namespace, String name, String version ) throws
        Exception {
        throw new UnsupportedOperationException( "Not Implemented" );
    }

    /**
     * Get the list of Profiles associated with a particular physical transformation.
     * @param pfn        The physical file name to search the transformation by.
     * @param resourceid String The id of the resource on which you want to search.
     * @param type       TCType The type of the transformation to search for. <br>
     *                   (Enumerated type includes source, binary, dynamic-binary, pacman, installed)<br>
     *
     * @throws Exception NotImplementedException if not implemented.
     * @return List      Returns a list of Profile Objects containing profiles 
     *                   assocaited with the transformation.
     *                   Returns <B>NULL</B> if no profiless found.
     * 
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public List<Profile> lookupPFNProfiles( String pfn, String resourceid, TCType type ) throws
        Exception {
        logMessage(
            "getTCPfnProfiles(String pfn, String resourceid, TCType type)");
        logMessage("\t getTCPfnProfiles(" + pfn + "," + resourceid + "," +
                   type + ")");

        List<Profile> result = new LinkedList<Profile>();

        //retrieve all the transformations corresponding to resource id and type
        //first
        List<TransformationCatalogEntry> entries = mTCStore.getEntries( resourceid, type );

        //traverse through the list
        for( TransformationCatalogEntry entry : entries ){
            if( entry.getPhysicalTransformation().equals( pfn ) ){
                result.addAll( entry.getProfiles() );
            }
        }

        //API dictates we return null in case of empty
        if( result == null || result.isEmpty() ){
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
        return mTCStore.getEntries( (String)null, (TCType)null );
    }

    /**
     *  ADDITIONS
     */

    /**
     * Add multiple TCEntries to the Catalog.
     * 
     * @param tcentry List Takes a list of TransformationCatalogEntry objects as input
     * 
     * @throws Exception
     * @return number of insertions On failure,throw an exception, don't use zero.
     * 
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public int insert(List<TransformationCatalogEntry> entries) throws
        Exception {
        for (int i = 0; i < entries.size(); i++) {
            TransformationCatalogEntry entry = ( (TransformationCatalogEntry)
                                                entries.get(i));
            this.insert(entry);
        }
        return entries.size();

    }

    /**
     * Add single TCEntry to the Catalog.
     * @param tcentry Takes a single TransformationCatalogEntry object as input
     * @throws Exception
     * 
     * @return number of insertions, should always be 1. On failure,
     * throw an exception, don't use zero.
     * 
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public int insert(TransformationCatalogEntry entry) throws
        Exception {
    	return this.insert(entry.getLogicalNamespace(),
                        entry.getLogicalName(), entry.getLogicalVersion(),
                        entry.getPhysicalTransformation(),
                        entry.getType(), entry.getResourceId(), null,
                        entry.getProfiles(), entry.getSysInfo());
        
    }

    /**
     * Add single TCEntry object temporarily to the in memory Catalog.
     * This is a hack to get around for adding soft state entries to the TC
     * @param tcentry Takes a single TransformationCatalogEntry object as input
     * @param write boolean enable write commits to backed catalog or not.
     * @throws Exception
     * 
     * @return number of insertions, should always be 1. On failure,
     * throw an exception, don't use zero.
     *
     * 
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     */
    public int insert(TransformationCatalogEntry entry, boolean write) throws
        Exception {
        if(this.addTCEntry(entry.getLogicalNamespace(),
                        entry.getLogicalName(), entry.getLogicalVersion(),
                        entry.getPhysicalTransformation(),
                        entry.getType(), entry.getResourceId(), null,
                        entry.getProfiles(), entry.getSysInfo(), write)) {
        	return 1;
        }else{
        	throw new RuntimeException("Failed to add TransformationCatalogEntry " + entry.getLogicalName());
        }
    }

    /**
     * Add an single entry into the transformation catalog.
     *
     * @param namespace    String The namespace of the transformation to be added (Can be null)
     * @param name         String The name of the transformation to be added.
     * @param version      String The version of the transformation to be added. (Can be null)
     * @param physicalname String The physical name/location of the transformation to be added.
     * @param type        TCType  The type of the physical transformation.
     * @param resourceid   String The resource location id where the transformation is located.
     * @param lfnprofiles     List   The List of Profile objects associated with a Logical Transformation. (can be null)
     * @param pfnprofiles     List   The List of Profile objects associated with a Physical Transformation. (can be null)
     * @param sysinfo     VDSSysInfo  The System information associated with a physical transformation.
     * 
     * @return number of insertions, should always be 1. On failure,
     * throw an exception, don't use zero.
     *
     * 
     * @throws Exception
     *
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public int insert(String namespace, String name,
                              String version,
                              String physicalname, TCType type,
                              String resourceid,
                              List pfnprofiles, List lfnprofiles,
                              SysInfo system) throws
        Exception {
        if(this.addTCEntry(namespace, name, version, physicalname, type,
                               resourceid, lfnprofiles, pfnprofiles, system, true)){
        	return 1;
        }else{
        	throw new RuntimeException("Failed to add TransformationCatalogEntry " + name);
        }
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
     * @see edu.isi.pegasus.planner.catalog.TransformationCatalogEntry
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

        List<TransformationCatalogEntry> existing = this.lookup( namespace, name, version, resourceid, type );
        
        boolean add = true;
        
        if( existing != null ){
            //check to see if entries match
            for( TransformationCatalogEntry e: existing ){
                if ( e.equals( entry ) ){
                   add = false;
                   break;
                }
            }
        }

        if( add ){
            mTCStore.addEntry( entry );
        }
        else {
            mLogger.log("TC Entry already exists. Skipping",
                        LogManager.DEBUG_MESSAGE_LEVEL);
        }
        
        //if entry needs to be added and flushed to the backend
        //set to flag to true.
        if( write && add ) {
            mFlushOnClose = true;
        }
        
        return true;
    }

    /**
     * Add additional profile to a logical transformation .
     * 
     * @param namespace String The namespace of the transformation to be added. (can be null)
     * @param name      String The name of the transformation to be added.
     * @param version   String The version of the transformation to be added. (can be null)
     * @param profiles  List   The List of Profile objects that are to be added 
     *                  to the transformation.
     * 
     * @return number of insertions. On failure, throw an exception, don't use zero.
     *
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public int addLFNProfile(String namespace, String name,
                                   String version,
                                   List profiles) throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Add additional profile to a physical transformation.
     * @param pfn  String       The physical name of the transformation
     * @param type      TCType The type of transformation that the profile is 
     *                  associated with.
     * @param resourcename String The resource on which the physical transformation exists
     * @param profiles   The List of Profile objects that are to be added
     *                   to the transformation.
     * @return number of insertions. On failure, throw an exception, don't use zero.
     *
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public int addPFNProfile(String pfn, TCType type,
                                   String resourcename,
                                   List profiles) throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * DELETIONS
     */

    /**
     * Delete all entries in the transformation catalog for a give logical transformation and/or on a resource and/or of
     * a particular type
     * @param namespace   String The namespace of the transformation to be deleted. (can be null)
     * @param name        String The name of the transformation to be deleted.
     * @param version     String The version of the transformation to be deleted. ( can be null)
     * @param resourceid String The resource id for which the transformation is to be deleted.
     *                          If <B>NULL</B> then transformation on all resource are deleted
     * @param type TCType The type of the transformation. If <B>NULL</B> then all types are deleted for the transformation.
     * @throws Exception
     * 
     * @return the number of removed entries.
     * 
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     */
    public int removeByLFN(String namespace, String name,
                                         String version, String resourceid,
                                         TCType type) throws Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }
    /**
     * Delete all entries in the transformation catalog for pair of logical and physical transformation.
     * @param physicalname String The physical name of the transformation
     * @param namespace String The namespace associated in the logical name of the transformation.
     * @param name String The name of the logical transformation.
     * @param version String The version number of the logical transformation.
     * @param resourceid String The resource on which the transformation is to be deleted.
     *                          If <B>NULL</B> then it searches all the resource id.
     * @param type TCType The type of transformation. If <B>NULL</B> then it search and deletes entries for all types.
     * @throws Exception
     * 
     * @return the number of removed entries.
     * 
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     */
    public int removeByPFN(String physicalname,
                                          String namespace,
                                          String name, String version,
                                          String resourceid, TCType type) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Delete a particular type of transformation, and/or on a particular resource
     * @param type TCType The type of the transformation
     * @param resourceid String The resource on which the transformation exists.
     *                           If <B>NULL</B> then that type of transformation is deleted from all the resources.
     * @throws Exception
     * 
     * @return the number of removed entries.
     * 
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.TCType
     */
    public int removeByType(TCType type, String resourceid) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Deletes entries from the catalog which have a particular system information.
     * @param sysinfo VDSSysInfo The System Information by which you want to delete
     * 
     * @return the number of removed entries.
     * 
     * @see edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo
     * @throws Exception
     */
    public int removeBySysInfo( SysInfo sysinfo) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Delete all entries on a particular resource from the transformation catalog.
     * @param resourceid String The resource which you want to remove.
     * @throws Exception
     * 
     * @return the number of removed entries.
     */
    public int removeBySiteID(String resourceid) throws Exception {

       throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Deletes the entire transformation catalog. CLEAN............. USE WITH CAUTION.
     * 
     * @return the number of removed entries.
     * 
     * @throws Exception
     */
    public int clear() throws Exception {
    	int length = (mTCStore.getEntries(null,(TCType)null)).size();
        mTCStore.clear();
        mFlushOnClose = true;
        return length;
    }
    
    /**
     * Delete a list of profiles or all the profiles associated with a pfn on a 
     * resource and of a type.
     * 
     * @param physicalname String The physical name of the transformation.
     * @param type TCType The type of the transformation.
     * @param resourceid String The resource of the transformation.
     * @param profiles List The list of profiles to be deleted. If <B>NULL</B> then all profiles for that pfn+resource+type are deleted.
     * 
     * @return the number of removed entries.
     * 
     * @see org.griphyn.cPlanner.classes.Profile
     * @throws Exception
     */
    public int deletePFNProfiles(String physicalname, TCType type,
                                      String resourceid, List profiles) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
    }

    /**
     * Delete a list of profiles or all the profiles associated with a logical 
     * transformation.
     * 
     * @param namespace String  The namespace of the logical transformation.
     * @param name String The name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param profiles List The List of profiles to be deleted. If <B>NULL</B> 
     *                 then all profiles for the logical transformation are deleted.
     * 
     * @return the number of removed entries.
     * 
     * @see org.griphyn.cPlanner.classes.Profile
     * @throws Exception
     */
    public int deleteLFNProfiles(String namespace, String name,
                                      String version, List profiles) throws
        Exception {
        throw new UnsupportedOperationException("Not Implemented");
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
     * Logs the message to a logging stream. Currently does not log to any stream.
     *
     * @param msg  the message to be logged.
     */
    protected void logMessage(String msg) {
        //mLogger.logMessage("[Shishir] Transformation Catalog : " + msg);
    }


}
