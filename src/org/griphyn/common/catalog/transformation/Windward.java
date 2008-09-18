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
package org.griphyn.common.catalog.transformation;

import org.griphyn.cPlanner.classes.Profile;

import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.classes.TCType;
import org.griphyn.common.classes.SysInfo;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import edu.isi.ikcap.workflows.ac.ProcessCatalog;

import edu.isi.ikcap.workflows.ac.classes.TransformationCharacteristics;
import edu.isi.ikcap.workflows.ac.classes.EnvironmentVariable;

import edu.isi.ikcap.workflows.sr.template.Component;

import edu.isi.ikcap.workflows.sr.util.PropertiesHelper;

import edu.isi.pegasus.common.logging.LoggerFactory;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Properties;
import org.griphyn.cPlanner.classes.PegasusBag;

/**
 * The implementation that allows us to inteface with Windward Process Catalogs.
 * Only a subset of query functions are implemented.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Windward  implements TransformationCatalog {

    /**
     * The property that designates which Process catalog impl to pick up.
     */
    public static final String PROCESS_CATALOG_IMPL_PROPERTY = "pegasus.catalog.transformation.windward";

    /**
     * The handle to the ProcessCatalog API.
     */
    protected ProcessCatalog mProcessCatalog;

    /**
     * The name of the Process Catalog implementation to interface to.
     */
    protected String mPCImpl;
    /**
     * The handle to PegasusProperties.
     */
    protected PegasusProperties mProps;

    /**
     * The handle to the log manager.
     */
    protected LogManager mLogger;
    
    /**
     * The request id .
     */
    protected String mRequestID;

    /**
     * Returns an instance of the File TC.
     *
     * @return TransformationCatalog
     * @deprecated
     */
    public static TransformationCatalog getInstance() {
        PegasusBag bag = new PegasusBag();
        bag.add( PegasusBag.PEGASUS_LOGMANAGER,  LoggerFactory.loadSingletonInstance() );
        bag.add( PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance() );
        TransformationCatalog result = new Windward();
        result.initialize( bag );
        return result;
    }


    /**
     * The default constructor.
     */
    public Windward() {
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
        mPCImpl = mProps.getProperty( this.PROCESS_CATALOG_IMPL_PROPERTY );
        mRequestID = mProps.getWingsRequestID();
        if( mRequestID == null ){
            throw new RuntimeException( "Specify the request id by specifying pegasus.wings.request.id property" );
        }
        
        String wings = mProps.getWingsPropertiesFile();
        //do sanity check
        if( wings == null ){
            throw new RuntimeException( "Path to wings properties file needs to be mentioned in Pegasus properties." +
                                         "Set pegasus.wings.properties  property.");
        }
        
        PropertiesHelper.loadWingsProperties( wings );
        

        //instantiate the process catalog in the connect method
        this.connect( mProps.matchingSubset( "pegasus.catalog.transformation.windward", false ) );
    }
    

    /**
     * Add an single entry into the transformation catalog.
     *
     * @param namespace String The namespace of the transformation to be
     *   added (Can be null)
     * @param name String The name of the transformation to be added.
     * @param version String The version of the transformation to be added.
     *   (Can be null)
     * @param physicalname String The physical name/location of the
     *   transformation to be added.
     * @param type TCType The type of the physical transformation.
     * @param resourceid String The resource location id where the
     *   transformation is located.
     * @param lfnprofiles List The List of Profile objects associated with a
     *   Logical Transformation. (can be null)
     * @param pfnprofiles List The List of Profile objects associated with a
     *   Physical Transformation. (can be null)
     * @param sysinfo SysInfo The System information associated with a
     *   physical transformation.
     * @throws Exception
     * @return boolean Returns true if succesfully added, returns false if
     *   error and throws exception.
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean addTCEntry(String namespace, String name, String version,
                              String physicalname, TCType type,
                              String resourceid, List lfnprofiles,
                              List pfnprofiles, SysInfo sysinfo) throws
        Exception {
        return false;
    }

    /**
     * Add multiple TCEntries to the Catalog.
     *
     * @param tcentry List Takes a list of TransformationCatalogEntry
     *   objects as input
     * @throws Exception
     * @return boolean Return true if succesful, false if error. Exception
     *   is thrown when error occurs.
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean addTCEntry(List tcentry) throws Exception {
        return false;
    }

    /**
     * Add single TCEntry object temporarily to the in memory Catalog.
     *
     * @param tcentry Takes a single TransformationCatalogEntry object as
     *   input
     * @param write boolean enable write commits to backed catalog or not.
     * @throws Exception
     * @return boolean Return true if succesful, false if error. Exception
     *   is thrown when error occurs.
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean addTCEntry(TransformationCatalogEntry tcentry, boolean write) throws
        Exception {
        return false;
    }

    /**
     * Add single TCEntry to the Catalog.
     *
     * @param tcentry Takes a single TransformationCatalogEntry object as
     *   input
     * @throws Exception
     * @return boolean Return true if succesful, false if error. Exception
     *   is thrown when error occurs.
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean addTCEntry(TransformationCatalogEntry tcentry) throws
        Exception {
        return false;
    }

    /**
     * Add additional profile to a logical transformation .
     *
     * @param namespace String The nsamespace of the transformation to be
     *   added. (can be null)
     * @param name String The name of the transformation to be added.
     * @param version String The version of the transformation to be added.
     *   (can be null)
     * @param profiles List The List of Profile objects that are to be added
     *   to the transformation.
     * @return boolean Returns true if success, false if error.
     * @throws Exception
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean addTCLfnProfile(String namespace, String name,
                                   String version, List profiles) throws
        Exception {
        return false;
    }

    /**
     * Add additional profile to a physical transformation.
     *
     * @param pfn String The physical name of the transformation
     * @param type TCType The type of transformation that the profile is
     *   associated with.
     * @param resourcename String The resource on which the physical
     *   transformation exists
     * @param profiles List The List of Profile objects that are to be added
     *   to the transformation.
     * @return boolean Returns true for success, false for error.
     * @throws Exception
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean addTCPfnProfile(String pfn, TCType type, String resourcename,
                                   List profiles) throws Exception {
        return false;
    }

    /**
     * Explicitely free resources before the garbage collection hits.
     *
     * @todo Implement this org.griphyn.common.catalog.Catalog method
     */
    public void close() {
    }

    /**
     * Establishes a link between the implementation and the thing the
     * implementation is build upon.
     *
     * @param props contains all necessary data to establish the link.
     * @return true if connected now, or false to indicate a failure.
     */
    public boolean connect( Properties props ) {
        boolean connect = true;
        //figure out how to specify via properties
        try{
            //mProcessCatalog = ProcessCatalogFactory.loadInstance( mPCImpl, props );
            mProcessCatalog = PropertiesHelper.getPCFactory().getPC(
				PropertiesHelper.getDCDomain(), 
				PropertiesHelper.getPCDomain(), null );
            mProcessCatalog.setRequestId( mRequestID );
        }catch( Exception e ){
            connect = false;
            mLogger.log( "Unable to connect ot process catalog " + e,
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }
        return connect;
    }

    /**
     * Deletes the entire transformation catalog.
     *
     * @return boolean Returns true if delete succeeds, false if any error
     *   occurs.
     * @throws Exception
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean deleteTC() throws Exception {
        return false;
    }

    /**
     * Delete a list of profiles or all the profiles associated with a
     * logical transformation.
     *
     * @param namespace String The namespace of the logical transformation.
     * @param name String The name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param profiles List The List of profiles to be deleted. If
     *   <B>NULL</B> then all profiles for the logical transformation are
     *   deleted.
     * @return boolean Returns true if success, false if any error occurs.
     * @throws Exception
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean deleteTCLfnProfile(String namespace, String name,
                                      String version, List profiles) throws
        Exception {
        return false;
    }

    /**
     * Delete a list of profiles or all the profiles associated with a pfn on
     * a resource and of a type.
     *
     * @param physicalname String The physical name of the transformation.
     * @param type TCType The type of the transformation.
     * @param resourceid String The resource of the transformation.
     * @param profiles List The list of profiles to be deleted. If
     *   <B>NULL</B> then all profiles for that pfn+resource+type are
     *   deleted.
     * @return boolean Returns true if success, false if any error occurs.
     * @throws Exception
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean deleteTCPfnProfile(String physicalname, TCType type,
                                      String resourceid, List profiles) throws
        Exception {
        return false;
    }

    /**
     * Delete all entries in the transformation catalog for a give logical
     * tranformation and/or on a resource and/or of a particular type
     *
     * @param namespace String The namespace of the transformation to be
     *   added. (can be null)
     * @param name String The name of the transformation to be added.
     * @param version String The version of the transformation to be added.
     *   ( can be null)
     * @param resourceid String The resource id for which the transformation
     *   is to be deleted. If <B>NULL</B> then transformation on all
     *   resource are deleted
     * @param type TCType The type of the transformation. If <B>NULL</B>
     *   then all types are deleted for the transformation.
     * @throws Exception
     * @return boolean Returns true if success , false if there is any error.
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean deleteTCbyLogicalName(String namespace, String name,
                                         String version, String resourceid,
                                         TCType type) throws Exception {
        return false;
    }

    /**
     * Delete all entries in the transformation catalog for pair of logical
     * and physical transformation.
     *
     * @param physicalname String The physical name of the transformation
     * @param namespace String The namespace assocaited in the logical name
     *   of the transformation.
     * @param name String The name of the logical transformation.
     * @param version String The version number of the logical
     *   transformation.
     * @param resourceid String The resource on which the transformation is
     *   to be deleted. If <B>NULL</B> then it searches all the resource id.
     * @param type TCType The type of transformation. If <B>NULL</B> then it
     *   search and deletes entries for all types.
     * @throws Exception
     * @return boolean Returns true if sucess, false if any error occurs.
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean deleteTCbyPhysicalName(String physicalname, String namespace,
                                          String name, String version,
                                          String resourceid, TCType type) throws
        Exception {
        return false;
    }

    /**
     * Delete all entries on a particular resource from the transformation
     * catalog.
     *
     * @param resourceid String The resource which you want to remove.
     * @throws Exception
     * @return boolean Returns true if successm false if any error occurs.
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean deleteTCbyResourceId(String resourceid) throws Exception {
        return false;
    }

    /**
     * Deletes entries from the catalog which have a particular system
     * information.
     *
     * @param sysinfo SysInfo The System Information by which you want to
     *   delete
     * @return boolean Returns true for success, false if any error occurs.
     * @throws Exception
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean deleteTCbySysInfo(SysInfo sysinfo) throws Exception {
        return false;
    }

    /**
     * Delete a paricular type of transformation, and/or on a particular
     * resource
     *
     * @param type TCType The type of the transformation
     * @param resourceid String The resource on which the transformation
     *   exists. If <B>NULL</B> then that type of transformation is deleted
     *   from all the resources.
     * @throws Exception
     * @return boolean Returns true if success, false if any error occurs.
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public boolean deleteTCbyType(TCType type, String resourceid) throws
        Exception {
        return false;
    }

    /**
     * List all the contents of the TC
     *
     * @return List Returns a List of TransformationCatalogEntry objects.
     * @throws Exception
     * @todo Implement this org.griphyn.common.catalog.TransformationCatalog
     *   method
     */
    public List getTC() throws Exception {
        return null;
    }

    /**
     * Returns TC entries for a particular logical transformation and/or on a
     * number of resources and/or of a particular type.
     *
     * @param namespace String The namespace of the logical transformation.
     * @param name String the name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param resourceids List The List resourceid where the transformation
     *   is located. If <b>NULL</b> it returns all resources.
     * @param type TCType The type of the transformation to search for. If
     *   <b>NULL</b> it returns all types.
     * @return List Returns a list of TransformationCatalogEntry objects
     *   containing the corresponding entries from the TC. Returns null if
     *   no entry found.
     */
    public List getTCEntries(String namespace, String name, String version,
                             List resourceids, TCType type) throws Exception {


        List result = new LinkedList();

        //Iterate through the list of resourceid
        for ( Iterator it = resourceids.iterator(); it.hasNext(); ){
            String resourceid = (String)it.next();
            List r = this.getTCEntries( namespace, name, version, resourceid, type );
            if( r != null ){
                result.addAll( r );
            }
        }

        return result.isEmpty()? null: result;


    }

    /**
     * Returns TC entries for a particular logical transformation and/or on a
     * particular resource and/or of a particular type.
     *
     * @param namespace String The namespace of the logical transformation.
     * @param name String the name of the logical transformation.
     * @param version String The version of the logical transformation.
     * @param resourceid String The resourceid where the transformation is
     *   located. If <B>NULL</B> it returns all resources.
     * @param type TCType The type of the transformation to search for. If
     *   <B>NULL</b> it returns all types.
     *
     * @return List Returns a list of TransformationCatalogEntry objects
     *   containing the corresponding entries from the TC. Returns null if
     *   no entry found.
     *
     */
    public List getTCEntries(String namespace, String name, String version,
                             String resourceid, TCType type) throws Exception {

        //fix the disconnect. get from ptmc the id?
        Component c = new Component( namespace, name, version );
        List locs = mProcessCatalog.getDeploymentRequirements( c, resourceid );
        List result = new LinkedList();

        boolean all = type == null;

        //iterate through the candidate locations
        for( Iterator it = locs.iterator(); it.hasNext(); ){
            TransformationCharacteristics txChar = ( TransformationCharacteristics )it.next();

            if ( all || //do no matching
                 txChar.getCharacteristic( TransformationCharacteristics.COMPILATION_METHOD ).equals( type.toString() ) ){//types match

                result.add( Adapter.convert( txChar ) );
            }

        }

        //System.out.println( "Returning  " + result );

        return result.isEmpty()? null : result;

    }

    /**
     * Get the list of Profiles associated with a particular logical
     * transformation.
     *
     * @param namespace String The namespace of the transformation to search
     *   for.
     * @param name String The name of the transformation to search for.
     * @param version String The version of the transformation to search for.
     *
     * @return List Returns a list of Profile Objects containing profiles
     *   assocaited with the transformation. Returns <B>NULL</B> if no
     *   profiles found.
     *
     *
     * @throws UnsupportedOperationException
     */
    public List getTCLfnProfiles( String namespace, String name, String version ) throws
        Exception {

        throw new UnsupportedOperationException(
        "No Notion of PFN Profiles. Unsupported operation getTCLfnProfiles( String, String, String)" );

    }

    /**
     * Get the list of LogicalNames available on a particular resource.
     *
     * @param resourceid String The id of the resource on which you want to
     *   search
     * @param type TCType The type of the transformation to search for. <BR>
     *   (Enumerated type includes source, binary, dynamic-binary, pacman,
     *   installed)<BR> If <B>NULL</B> then return logical name for all
     *   types.
     * @return List Returns a list of String Arrays. Each array contains the
     *   resourceid, logical transformation in the format
     *   namespace::name:version and type. The last entry in the list is an
     *   array of integers specifying the column length for pretty print.
     *   Returns <B>NULL</B> if no results found.
     *
     * @throws UnsupportedOperationException
     */
    public List getTCLogicalNames(String resourceid, TCType type) throws
        Exception {
        throw new UnsupportedOperationException(
        "Unsupported operation getTCLogicalNames( String,  TCType )" );
    }

    /**
     * Returns the TC implementation being used
     *
     * @return String
     */
    public String getTCMode() {
        return "TC Implementation to interface with Windward Process Catalog";
    }

    /**
     * Get the list of Profiles associated with a particular physical
     * transformation.
     *
     * @param pfn The physical file name to search the transformation by.
     * @param resourceid String The id of the resource on which you want to
     *   search.
     * @param type TCType The type of the transformation to search for. <br>
     *   (Enumerated type includes source, binary, dynamic-binary, pacman,
     *   installed)<br>
     * @throws Exception NotImplementedException if not implemented.
     *
     * @return List Returns a list of Profile Objects containing profiles
     *   assocaited with the transformation. Returns <B>NULL</B> if no
     *   profiless found.
     *
     * @throws UnsupportedOperationException
     */
    public List getTCPfnProfiles(String pfn, String resourceid, TCType type) throws
        Exception {

        throw new UnsupportedOperationException(
        "No Notion of PFN Profiles. Unsupported operation getTCPfnProfiles( String, String, String, String, TCType )" );


    }

    /**
     * Get the list of PhysicalNames for a particular transformation on a
     * site/sites for a particular type/types;
     *
     * @param namespace String The namespace of the transformation to search
     *   for.
     * @param name String The name of the transformation to search for.
     * @param version String The version of the transformation to search for.
     * @param resourceid String The id of the resource on which you want to
     *   search. <BR> If <B>NULL</B> then returns entries on all resources
     * @param type TCType The type of the transformation to search for. <BR>
     *   (Enumerated type includes source, binary, dynamic-binary, pacman,
     *   installed)<BR> If <B>NULL</B> then returns entries of all types.
     * @return List Returns a list of String Arrays. Each array contains the
     *   resourceid, the physical transformation, the type of the tr and the
     *   systeminfo. The last entry in the List is a int array containing
     *   the column lengths for pretty print. Returns <B>NULL</B> if no
     *   results found.
     *
     * @throws UnsupportedOperationException
     */
    public List getTCPhysicalNames(String namespace, String name,
                                   String version, String resourceid,
                                   TCType type) throws Exception {

        throw new UnsupportedOperationException(
        "Unsupported operation getTCPhysicalNames( String, String, String, String, TCType )" );
    }

    /**
     * Get the list of Resource ID's where a particular transformation may
     * reside.
     *
     * @param namespace String The namespace of the transformation to search
     *   for.
     * @param name String The name of the transformation to search for.
     * @param version String The version of the transformation to search for.
     * @param type TCType The type of the transformation to search for.<BR>
     *   (Enumerated type includes SOURCE, STATIC-BINARY, DYNAMIC-BINARY,
     *   PACMAN, INSTALLED, SCRIPT)<BR> If <B>NULL</B> it returns all types.
     *
     * @return List Returns a list of Resource Id's as strings. Returns
     *   <B>NULL</B> if no results found.
     */
    public List getTCResourceIds(String namespace, String name, String version,
                                 TCType type) throws Exception {

        //fix the disconnect. get from ptmc the id?
        Component c = new Component( namespace, name, version );
        List locs = mProcessCatalog.findCandidateInstallations( c );
        List result = new LinkedList();

        boolean all = type == null;

        //iterate through the candidate locations
        for( Iterator it = locs.iterator(); it.hasNext(); ){
            TransformationCharacteristics txChar = ( TransformationCharacteristics )it.next();

            if ( all || //do no matching
                 txChar.getCharacteristic( TransformationCharacteristics.COMPILATION_METHOD ).equals( type.toString() ) ){//types match
                result.add( txChar.getCharacteristic( TransformationCharacteristics.SITE_HANDLE ) );
            }

        }

        return result.isEmpty()? result : null;
    }

    /**
     * Predicate to check, if the connection with the catalog's
     * implementation is still active.
     *
     * @return true, if the implementation is disassociated, false otherwise.
     * @todo Implement this org.griphyn.common.catalog.Catalog method
     */
    public boolean isClosed() {
        return mProcessCatalog == null;
    }
}

/**
 * An adapter class that converts the TransformationCharacterisitcs object into
 * a TransformationCatalogEntry object.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
class Adapter{

    /**
     * Converts a TransformationCharacteristics object to TransformationCatalogEntry
     * object.
     *
     * @param from   the TransformationCharacteristics object that has to be converted.
     *
     * @return TransformationCatalogEntry object.
     */
    public static TransformationCatalogEntry convert( TransformationCharacteristics from ){
        TransformationCatalogEntry result =
            new TransformationCatalogEntry( null,
                                            (String)from.getCharacteristic( TransformationCharacteristics.NAME ),
                                            null );


        //set the tc type
        result.setType( TCType.fromString(
                                     (String)from.getCharacteristic( TransformationCharacteristics.COMPILATION_METHOD ) ));


        //do a sanity check on glibc
        String glibc = (String)from.getCharacteristic( TransformationCharacteristics.DEPENDANT_LIBRARY );
        if( glibc != null && glibc.startsWith( "glibc_" ) ){
            glibc = glibc.substring( glibc.indexOf( "glibc_" ));
        }

        //set the sysinfo
        result.setSysInfo( new SysInfo( (String)from.getCharacteristic( TransformationCharacteristics.ARCHITECTURE ),
                                        (String)from.getCharacteristic( TransformationCharacteristics.OPERATING_SYSTEM ),
                                        glibc
                           ) );

        //put the site handle and the location
        result.setPhysicalTransformation( (String)from.getCharacteristic( TransformationCharacteristics.CODE_LOCATION ) );
        result.setResourceId( (String)from.getCharacteristic( TransformationCharacteristics.SITE_HANDLE )  );

        //convert all the environment variables to profiles
        List envs = ( List )from.getCharacteristic( TransformationCharacteristics.ENVIRONMENT_VARIABLE );
        for( Iterator it = envs.iterator(); it.hasNext(); ){
            EnvironmentVariable env = ( EnvironmentVariable )it.next();
            result.setProfile( new Profile( Profile.ENV ,
                                            env.getKey(),
                                            env.getValue() ));
        }

        //convert to globus profiles the runtime information etc
        //To do still.

        return result;
    }

}
