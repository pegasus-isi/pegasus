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

package org.griphyn.common.catalog.replica;

import org.griphyn.common.catalog.ReplicaCatalog;
import org.griphyn.common.catalog.ReplicaCatalogEntry;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.ikcap.workflows.dc.DataCharacterization;

import edu.isi.ikcap.workflows.dc.classes.DataSourceLocationObject;

import edu.isi.ikcap.workflows.sr.util.PropertiesHelper;
import edu.isi.ikcap.workflows.util.FactoryException;

import edu.isi.pegasus.common.logging.LogManagerFactory;

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Properties;
import org.griphyn.cPlanner.common.PegasusProperties;

/**
 * An implementation of the Replica Catalog interface that talks to Windward
 * Data Characterization Catalog.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Windward  implements ReplicaCatalog {

    /**
     * The property that designates which data characterization catalog impl to pick up.
     */
    public static final String DATA_CHARACTERIZATION_IMPL_PROPERTY = "windward";


    /**
     * The availability time attribute key, that designates how soon the data
     * will be available.
     */
    public static final String DATA_AVAILABILITY_KEY = "data_availability";


    /**
     * The handle to the Data Characterization Catalog.
     */
    private DataCharacterization mDCharCatalog;

    /**
     * The handle to the log manager.
     */
    protected LogManager mLogger;


    /**
     * Converts a DataSourceLocationObject to ReplicaCatalogEntry.
     *
     *
     * @param ds  the DataSourceLocation to be converted.
     *
     * @return ReplicaCatalogEntry
     */
    public static ReplicaCatalogEntry convertToRCE( DataSourceLocationObject ds ){
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry( );
        rce.setPFN( ds.getLocation() );
        rce.setResourceHandle( ds.getSite() );
        rce.addAttribute( DATA_AVAILABILITY_KEY,  ds.getAvailabilityTime() );

        //for SE18 the PFN is always null set to dummy value in /tmp
        if( rce.getPFN() == null ){
            rce.setPFN( "/tmp/dummy" );
        }
        //other attributes to converted later on.

        return rce;
    }

    /**
     * The default constructor.
     */
    public Windward() {
        mLogger =  LogManagerFactory.loadSingletonInstance();
    }

    /**
     * Removes everything. The SR DC API does not support removes or deletes.
     * Always returns 0.
     *
     * @return the number of removed entries.
     */
    public int clear() {
        return 0;
    }

    /**
     * Explicitely free resources before the garbage collection hits.
     *
     *
     */
    public void close() {

        mDCharCatalog.close();
        mDCharCatalog = null;
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
        
        System.out.println( "Properties are "  + props );
        //figure out how to specify via properties
        try{
            String implementor = props.getProperty( Windward.DATA_CHARACTERIZATION_IMPL_PROPERTY );
            //String id = props.getProperty( "dax.id" );
            String id = PegasusProperties.getInstance().getProperty( "pegasus.wings.request.id" );
            mDCharCatalog =   PropertiesHelper.getDCFactory().getDC(
				PropertiesHelper.getDCDomain(), id );
        }catch( Exception e ){
            connect = false;
            mLogger.log( "Unable to connect to Data Characterization Catalog " + e,
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return connect;

    }

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where
     * the PFN attribute is found, and matches exactly the object value.
     * The SR DC API does not support removes or deletes. Always returns 0.
     *
     * @param lfn is the logical filename to look for.
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     * @return the number of removed entries.
     */
    public int delete(String lfn, String name, Object value) {
        return 0;
    }

    /**
     * Deletes a very specific mapping from the replica catalog.
     * The SR DC API does not support removes or deletes.
     * Always returns 0.
     *
     * @param lfn is the logical filename in the tuple.
     * @param tuple is a description of the PFN and its attributes.
     * @return the number of removed entries, either 0 or 1.
     */
    public int delete(String lfn, ReplicaCatalogEntry tuple) {
        return 0;
    }

    /**
     * Deletes multiple mappings into the replica catalog.
     * The SR DC API does not support removes or deletes.
     * Always returns 0.
     *
     * @param x is a map from logical filename string to list of replica
     *   catalog entries.
     * @param matchAttributes whether mapping should be deleted only if all
     *   attributes match.
     *
     * @return the number of deletions.
     *
     */
    public int delete(Map x, boolean matchAttributes) {
        return 0;
    }

    /**
     * Deletes a specific mapping from the replica catalog.
     * The SR DC API does not support removes or deletes.
     * Always returns 0.
     *
     * @param lfn is the logical filename in the tuple.
     * @param pfn is the physical filename in the tuple.
     *
     * @return the number of removed entries.
     */
    public int delete( String lfn, String pfn ) {
        return 0;
    }

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where
     * the resource handle is found. The SR DC API does not support removes or deletes.
     * Always returns 0.
     *
     * @param lfn is the logical filename to look for.
     * @param handle is the resource handle
     *
     * @return the number of entries removed.
     */
    public int deleteByResource(String lfn, String handle) {
        return 0;
    }

    /**
     * Inserts a new mapping into the replica catalog.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param pfn is the physical filename associated with it.
     * @param handle is a resource handle where the PFN resides.
     * @return number of insertions, should always be 1. On failure, throw
     *   an exception, don't use zero.
     *
     */
    public int insert(String lfn, String pfn, String handle) {
        DataSourceLocationObject ds = new DataSourceLocationObject( );
        ds.setDataObjectID( lfn );
        ds.setLocation( pfn );
        ds.setSite( handle );

        return mDCharCatalog.registerDataObject( ds ) ?
               1:
               0;
    }

    /**
     * Inserts a new mapping into the replica catalog.
     * For the time being only the pfn and site attribute are registered.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param tuple is the physical filename and associated PFN attributes.
     *
     * @return number of insertions, should always be 1. On failure, throw
     *   an exception, don't use zero.
     */
    public int insert( String lfn, ReplicaCatalogEntry tuple ) {

        //for the time being only populating the pfn and site attribute
        DataSourceLocationObject ds = new DataSourceLocationObject();
        ds.setDataObjectID( lfn );
        ds.setLocation( tuple.getPFN() );
        ds.setSite( tuple.getResourceHandle() );

        return mDCharCatalog.registerDataObject( ds ) ?
               1:
               0;
    }

    /**
     * Inserts multiple mappings into the replica catalog.
     * For the time being only the pfn and site attribute are registered for
     * each entry.
     *
     * @param x is a map from logical filename string to list of replica
     *   catalog entries.
     *
     * @return the number of insertions.
     *
     */
    public int insert( Map x ) {
        int result = 0;
        //traverse through the entries and insert one by one
        for( Iterator it = x.entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = ( Map.Entry )it.next();
            String lfn = ( String )entry.getKey();

            //traverse through rce's for each lfn
            for( Iterator rceIt = ((List)entry.getValue()).iterator(); rceIt.hasNext(); ){
                ReplicaCatalogEntry rce = ( ReplicaCatalogEntry )rceIt.next();
                result += this.insert( lfn, rce );
            }

        }
        return result;
    }

    /**
     * Predicate to check, if the connection with the catalog's
     * implementation is still active.
     *
     * @return true, if the implementation is disassociated, false otherwise.
     */
    public boolean isClosed() {
        return mDCharCatalog == null ;
    }

    /**
     * Lists a subset of all logical filenames in the catalog.
     *
     * @param constraint is a constraint for the logical filename only. It
     *   is a string that has some meaning to the implementing system. This
     *   can be a SQL wildcard for queries, or a regular expression for
     *   Java-based memory collections.
     *
     * @return A set of logical filenames that match. The set may be empty
     *
     * @throws UnsupportedOperationException
     *
     */
    public Set list( String constraint ) {
        throw new UnsupportedOperationException( "Unsupported operation list( String )" );
    }

    /**
     * Lists all logical filenames in the catalog.
     *
     * @return A set of all logical filenames known to the catalog.
     *
     *
     * @throws UnsupportedOperationException
     */
    public Set list() {
        throw new UnsupportedOperationException( "Unsupported operation list( String )" );
    }

    /**
     * Retrieves the entry for a given filename and resource handle from the
     * replica catalog.
     *
     * @param lfn is the logical filename to obtain information for.
     * @param handle is the resource handle to obtain entries for.
     *
     * @return the (first) matching physical filename, or <code>null</code>
     *   if no match was found.
     */
    public String lookup( String lfn, String handle) {
        String result = null;

        Collection l = mDCharCatalog.findDataObjectLocationsAndAccessAttributes( lfn );

        //sanity check
        if( l == null ){ return result; }

        for( Iterator it = l.iterator(); it.hasNext(); ){
            DataSourceLocationObject ds = ( DataSourceLocationObject )it.next();
            if( ( result = ds.getSite() ).equals( handle ) ){
                return result;
            }
        }

        return null;
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog.
     * For the time being only the pfn and site attribute are retrieved for
     * the lfn.
     *
     * @param lfn is the logical filename to obtain information for.
     *
     * @return a collection of replica catalog entries
     */
    public Collection lookup( String lfn ) {
        Collection result = new LinkedList();

        Collection l = mDCharCatalog.findDataObjectLocationsAndAccessAttributes( lfn );

        //sanity check
        if( l == null ){ return result; }

        for( Iterator it = l.iterator(); it.hasNext(); ){
            DataSourceLocationObject ds = (DataSourceLocationObject) it.next();
            result.add( convertToRCE( ds ) );
        }

        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog. For the time being only the pfn and site attribute are
     * retrieved for the lfn.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     *
     * @return a map indexed by the LFN. Each value is a collection of
     *   replica catalog entries (all attributes).
     */
    public Map lookup(Set lfns, String handle ) {
        Map result = new HashMap();

        Map m = mDCharCatalog.findDataObjectLocationsAndAccessAttributes( lfns );

        //iterate through the entries and match for handle
        for( Iterator it = m.entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = ( Map.Entry )it.next();
            String lfn = ( String )entry.getKey();
            List rces  = new LinkedList();

            //traverse through all the DataSourceLocationObjects for lfn
            List l = ( List )entry.getValue();
            //sanity check
            if( l == null ){  continue ; }

            for( Iterator dit = l.iterator(); dit.hasNext(); ){
                DataSourceLocationObject ds = ( DataSourceLocationObject )dit.next();
                if( ds.getSite().equals( handle ) ){
                    //add to the rces list
                    rces.add( convertToRCE( ds ) );
                }
            }

            //populate the entry for lfn into result
            //Do we checks for empty rce list ???
            result.put( lfn, rces );
        }

        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog.
     *
     * @param constraints is mapping of keys 'lfn', 'pfn', or any attribute
     *   name, e.g. the resource handle 'site', to a string that has some
     *   meaning to the implementing system. This can be a SQL wildcard for
     *   queries, or a regular expression for Java-based memory collections.
     *   Unknown keys are ignored. Using an empty map requests the complete
     *   catalog.
     *
     * @return a map indexed by the LFN. Each value is a collection of
     *   replica catalog entries.
     *
     * @throws new UnsupportedOperationException( "Unsupported operation list( String )" );
     */
    public Map lookup( Map constraints ) {
        throw new UnsupportedOperationException( "Unsupported operation lookup( Map )" );
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog. For the time being only the pfn and site attribute are
     * retrieved for the lfn.
     *
     * @param lfns is a set of logical filename strings to look up.
     *
     * @return a map indexed by the LFN. Each value is a collection of
     *   replica catalog entries for the LFN.
     */
    public Map lookup( Set lfns ) {
        Map result = new HashMap();

        Map m = mDCharCatalog.findDataObjectLocationsAndAccessAttributes( lfns );

        //iterate through the entries and match for handle
        for( Iterator it = m.entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = ( Map.Entry )it.next();
            String lfn = ( String )entry.getKey();
            List rces  = new LinkedList();

            //traverse through all the DataSourceLocationObjects for lfn
            List l = ( List )entry.getValue();
            //sanity check
            if( l == null ){  continue ; }

            for( Iterator dit = l.iterator(); dit.hasNext(); ){
                DataSourceLocationObject ds = ( DataSourceLocationObject )dit.next();
                ReplicaCatalogEntry rce = convertToRCE( ds );
                mLogger.log( "Replica Catalog Entry retrieved from DC for lfn " + lfn + " is " + rce,
                             LogManager.DEBUG_MESSAGE_LEVEL );
                //add to the rces list
                rces.add( rce );
            }

            //populate the entry for lfn into result
            //Do we checks for empty rce list ???
            result.put( lfn, rces );
        }

        return result;

    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog.
     *
     * @param lfn is the logical filename to obtain information for.
     *
     * @return a set of PFN strings
     *
     */
    public Set lookupNoAttributes( String lfn ) {
        Set result = new HashSet();

        for( Iterator it = this.lookup( lfn ).iterator(); it.hasNext(); ){
            ReplicaCatalogEntry rce = ( ReplicaCatalogEntry )it.next();
            result.add( rce.getPFN() );
        }


        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog.
     *
     * @param lfns is a set of logical filename strings to look up.
     *
     * @return a map indexed by the LFN. Each value is a set of PFN strings.
     */
    public Map lookupNoAttributes( Set lfns ) {
        Map result = new HashMap();

        for( Iterator it = this.lookup( lfns ).entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = ( Map.Entry )it.next();
            String lfn = ( String ) entry.getKey();
            Set pfns = new HashSet();
            //traverse through the rce's and put only pfn's
            for( Iterator rceIt = (( Collection )entry.getValue()).iterator(); rceIt.hasNext(); ){
                ReplicaCatalogEntry rce = ( ReplicaCatalogEntry )rceIt.next();
                pfns.add( rce.getPFN() );
            }

            //populate the entry for lfn into result
            //Do we checks for empty rce list ???
            result.put( lfn, pfns );

        }
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     *
     * @return a map indexed by the LFN. Each value is a set of physical
     *   filenames.
     */
    public Map lookupNoAttributes(Set lfns, String handle) {
        Map result = new HashMap();

        for( Iterator it = this.lookup( lfns, handle ).entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = ( Map.Entry )it.next();
            String lfn = ( String ) entry.getKey();
            Set pfns = new HashSet();
            //traverse through the rce's and put only pfn's
            for( Iterator rceIt = (( Collection )entry.getValue()).iterator(); rceIt.hasNext(); ){
                ReplicaCatalogEntry rce = ( ReplicaCatalogEntry )rceIt.next();
                pfns.add( rce.getPFN() );
            }

            //populate the entry for lfn into result
            //Do we checks for empty rce list ???
            result.put( lfn, pfns );

        }
        return result;

    }

    /**
     * Removes all mappings for an LFN from the replica catalog.
     *
     * @param lfn is the logical filename to remove all mappings for.
     * @return the number of removed entries.
     */
    public int remove(String lfn) {
        return 0;
    }

    /**
     * Removes all mappings for a set of LFNs.
     * The SR DC API does not support removes or deletes. Always returns 0.
     *
     * @param lfns is a set of logical filename to remove all mappings for.
     *
     * @return the number of removed entries.
     *
     */
    public int remove(Set lfns) {
        return 0;
    }

    /**
     * Removes all entries from the replica catalog where the PFN attribute
     * is found, and matches exactly the object value.
     *
     * The SR DC API does not support removes or deletes. Always returns 0.
     *
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     *
     * @return the number of removed entries.
     *
     */
    public int removeByAttribute(String name, Object value) {
        return 0;
    }

    /**
     * Removes all entries associated with a particular resource handle.
     *
     *
     * The SR DC API does not support removes or deletes. Always returns 0.
     *
     * @param handle is the site handle to remove all entries for.
     *
     * @return the number of removed entries.
     */
    public int removeByAttribute(String handle) {
        return 0;
    }
}
