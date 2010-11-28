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

package edu.isi.pegasus.planner.catalog.replica.impl;

import edu.isi.pegasus.planner.catalog.replica.*;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.CatalogException;

import edu.isi.pegasus.common.util.CommonProperties;

import org.globus.replica.rls.RLSClient;
import org.globus.replica.rls.RLSException;
import org.globus.replica.rls.RLSAttribute;
import org.globus.replica.rls.RLSAttributeObject;
import org.globus.replica.rls.RLSString2Bulk;
import org.globus.replica.rls.RLSString2;
import org.globus.replica.rls.RLSOffsetLimit;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class implements the VDS replica catalog interface on top of the
 * LRC. This class implementation ends up talking to a single LRC.
 * It is accessed internally from the RLI implementation.
 * RLS Exceptions are being caught here. They probably should be thrown
 * and caught at the calling class (i.e the RLI implementation).
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 */
public class LRC implements ReplicaCatalog {

    /**
     * The number of entries searched in each bulk query to RLS.
     */
    public static final int RLS_BULK_QUERY_SIZE = 1000;

    /**
     * The default timeout in seconds to be used while querying the LRC.
     */
    public static final String DEFAULT_LRC_TIMEOUT = "30";

    /**
     * The key that is used to get hold of the timeout value from the properties
     * object.
     */
    public static final String RLS_TIMEOUT_KEY = "rls.timeout";


    /**
     * The key that is used to get hold of the timeout value from the properties
     * object.
     */
    public static final String LRC_TIMEOUT_KEY = "lrc.timeout";

    /**
     * The properties key that allow us to associate a site with a LRC URL,
     * and hence providing a value for the SITE_ATTRIBUTE. 
     * User will specify lrc.site.isi_viz rls://isi.edu  to associate
     * site isi_viz with rls://isi.edu
     */
    public static final String LRC_SITE_TO_LRC_URL_KEY = "lrc.site.";

    /**
     * The attribute in RLS that maps to a site handle.
     */
    public static final String SITE_ATTRIBUTE = ReplicaCatalogEntry.RESOURCE_HANDLE;

    /**
     * The undefined pool attribute value. The pool attribute is assigned this
     * value if the pfn queried does not have a pool associated with it.
     */
    public static final String UNDEFINED_SITE = "UNDEFINED_POOL";

    /**
     * The key that is used to get hold of the url from the properties object.
     */
    public static final String URL_KEY = "url";

    /**
     * The key that if set, specifies the proxy to be picked up while connecting
     * to the RLS.
     */
    public static final String PROXY_KEY = "proxy";

    /**
     * The error message for not connected to LRC.
     */
    public static final String LRC_NOT_CONNECTED_MSG = "Not connected to LRC ";


    /**
     * The handle to the logging object. Should be log4j soon.
     */
    private LogManager mLogger;

    /**
     * The string holding the message that is logged in the logger.
     */
    private String mLogMsg;

    /**
     * The URL pointing to the LRC to which this instance of class talks to.
     */
    private String mLRCURL;

    /**
     * The handle to the client that allows access to the RLS running at the
     * url specified while connecting.
     */
    private RLSClient mRLS;

    /**
     * The handle to the client that allows access to the LRC running at the
     * url specified while connecting.
     */
    private RLSClient.LRC mLRC;

    /**
     * The batch size while querying the LRC in the bulk mode.
     */
    private int mBatchSize;

    /**
     * The timeout in seconds while querying to the LRC.
     */
    private int mTimeout;
    
    /**
     * The default site attribute to be associated with the results.
     */
    private String mDefaultSiteAttribute;

    /**
     * The default constructor, that creates an object which is not linked with
     * any RLS. Use the connect method to connect to the LRC and use it.
     *
     * @see #connect(Properties).
     */
    public LRC() {
        mRLS = null;
        mLRC = null;
        mLogger =  LogManagerFactory.loadSingletonInstance();
        mBatchSize = LRC.RLS_BULK_QUERY_SIZE;
        mTimeout   = Integer.parseInt(LRC.DEFAULT_LRC_TIMEOUT);
    }

    /**
     * Establishes a connection to the LRC.
     *
     * @param props contains all necessary data to establish the link.
     * @return true if connected now, or false to indicate a failure.
     */
    public boolean connect(Properties props) {
        boolean con = false;
        Object obj = props.get(URL_KEY);
        mLRCURL = (obj == null) ? null : (String) obj;

        if (mLRCURL == null) {
            //nothing to connect to.
            log("The LRC url is not specified",
                LogManager.ERROR_MESSAGE_LEVEL);
            return con;
        }

        //try to see if a proxy cert has been specified or not
        String proxy = props.getProperty(PROXY_KEY);

        //determine timeout
        mTimeout = getTimeout(props);

        //set the batch size for querie
        setBatchSize(props);
        
        //stripe out the properties that assoicate site handle to lrc url
        Properties site2LRC = CommonProperties.matchingSubset( props, LRC.LRC_SITE_TO_LRC_URL_KEY, false);
        //traverse through the properties to figure out
        //the default site attribute for the URL
        for( Iterator it = site2LRC.entrySet().iterator(); it.hasNext(); ){
            Map.Entry<String,String> entry = (Map.Entry<String,String>)it.next();
            if( entry.getValue().equalsIgnoreCase( mLRCURL ) ){
                mDefaultSiteAttribute = entry.getKey();
            }
        }
        
        if( mDefaultSiteAttribute != null ){
            mLogger.log( "Default Site attribute is " + mDefaultSiteAttribute,
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }
        
        return connect(mLRCURL, proxy);
    }

    /**
     * Establishes a connection to the LRC, picking up the proxy from the default
     * location usually /tmp/ directory.
     *
     * @param url    the url to lrc to connect to.
     *
     * @return true if connected now, or false to indicate a failure.
     */
    public boolean connect(String url) {
        return connect(url,null);
    }


    /**
     * Establishes a connection to the LRC.
     *
     * @param url    the url to lrc to connect to.
     * @param proxy  the path to the proxy file to be picked up. null denotes
     *               default location.
     *
     * @return true if connected now, or false to indicate a failure.
     *
     * @throws ReplicaCatalogException in case of
     */
    public boolean connect(String url, String proxy) {
        mLRCURL = url;
        try {
            mRLS = (proxy == null) ?
                new RLSClient(url) : //proxy is picked up from default loc /tmp
                new RLSClient(url, proxy);

            //set the timeout
            mRLS.SetTimeout(mTimeout);

            //connect is only successful if we have
            //successfully connected to the LRC
            mLRC = mRLS.getLRC();

        }
        catch (RLSException e) {
            log("RLS Exception", e,LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        return true;
    }

    /**
     * Gets a handle to the LRC that is associated with the RLS running at
     * url.
     *
     * @return <code>RLSClient.LRC</code> that points to the RLI that is
     *         running , or null in case connect method not being called.
     * @see #mLRCURL
     */
    public RLSClient.LRC getLRC() {
        return (isClosed()) ? null : mLRC;
    }

    /**
     * Retrieves the entry for a given filename and resource handle from
     * the LRC.
     *
     * @param lfn is the logical filename to obtain information for.
     * @param handle is the resource handle to obtain entries for.
     * @return the (first) matching physical filename, or
     * <code>null</code> if no match was found.
     *
     * @throws ReplicaCatalogException in case of any error that is throw by LRC
     *                                that can't be handled.
     */
    public String lookup(String lfn, String handle) {
        //sanity check
        if (this.isClosed()) {
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }
        String pfn = null;
        String site = null;

        //query the lrc
        try {
            List l = mLRC.getPFN(lfn);
            for (Iterator it = l.iterator(); it.hasNext(); ) {
                //query for the pool attribute
                pfn = ( (RLSString2) it.next()).s2;
                site = getSiteHandle(pfn);
                if (site.equalsIgnoreCase(handle)) {
                    //ok we have the first pfn with for the site and lfn
                    break;
                }
            }
        }
        catch (RLSException ex) {
            if(ex.GetRC() == RLSClient.RLS_LFN_NEXIST ||
               ex.GetRC() == RLSClient.RLS_MAPPING_NEXIST){
                   pfn = null;
                }
                else{
                    throw exception("lookup(String,String)", ex);
                }
        }

        return pfn;
    }

    /**
     * Retrieves all entries for a given LFN from the LRC.
     * Each entry in the result set is a tuple of a PFN and all its
     * attributes.
     *
     * @param lfn is the logical filename to obtain information for.
     * @return a collection of replica catalog entries,  or null in case of
     *         unable to connect to RLS or error.
     *
     * @throws ReplicaCatalogException in case of any error that is throw by LRC
     *                                that can't be handled.
     * @see ReplicaCatalogEntry
     */
    public Collection lookup(String lfn) throws ReplicaCatalogException {
        //sanity check
        if (this.isClosed()) {
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }

        List res = new ArrayList(3);

        //query the lrc
        try {
            List l = mLRC.getPFN(lfn);
            for (Iterator it = l.iterator(); it.hasNext(); ) {
                String pfn = ( (RLSString2) it.next()).s2;
                //get hold of all attributes
                ReplicaCatalogEntry entry = new ReplicaCatalogEntry(pfn,
                    getAttributes(pfn));
                res.add(entry);
            }
        }
        catch (RLSException ex) {
            if(ex.GetRC() == RLSClient.RLS_LFN_NEXIST ||
               ex.GetRC() == RLSClient.RLS_MAPPING_NEXIST){
                    log("Mapping for lfn " + lfn + " does not exist",
                        LogManager.DEBUG_MESSAGE_LEVEL);
                }
                else{
                    throw exception("lookup(String)", ex);
                }
        }

        return res;

    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog.
     * Each entry in the result set is just a PFN string. Duplicates
     * are reduced through the set paradigm.
     *
     * @param lfn is the logical filename to obtain information for.
     * @return a set of PFN strings, or null in case of unable to connect
     *         to RLS.
     *
     */
    public Set lookupNoAttributes(String lfn) {
        //sanity check
        if (this.isClosed()) {
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }

        Set res = new HashSet(3);

        //query the lrc
        try {
            List l = mLRC.getPFN(lfn);
            for (Iterator it = l.iterator(); it.hasNext(); ) {
                String pfn = ( (RLSString2) it.next()).s2;
                res.add(pfn);
            }
        }
        catch (RLSException ex) {
            //am not clear whether to throw the exception or what
            log("lookup(String,String):", ex,LogManager.ERROR_MESSAGE_LEVEL);
            return null;
        }

        return res;

    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete LRC. It uses the bulk query api to the LRC to query for stuff.
     * Bulk query has been in RLS since version 2.0.8. Internally, the bulk
     * queries are done is sizes specified by variable mBatchSize.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a collection
     * of replica catalog entries for the LFN.
     * @see ReplicaCatalogEntry
     * @see #getBatchSize()
     */
    public Map lookup(Set lfns) throws ReplicaCatalogException {
        //one has to do a bulk query in batches
        Set s = null;
        int size = mBatchSize;
        Map map = new HashMap(lfns.size());

        log("Number of files to query LRC " + lfns.size() +
            " in batch sizes of " + size, LogManager.DEBUG_MESSAGE_LEVEL);

        for (Iterator it = lfns.iterator(); it.hasNext(); ) {
            s = new HashSet(size);
            for (int j = 0; (j < size) && (it.hasNext()); j++) {
                s.add(it.next());
            }
            if (!s.isEmpty()) {
                //there is no conflict, as the keys are unique
                //via the set paradigm. Passing null as we want
                //to get hold of all attributes.
                map.putAll(bulkLookup(s, null));
            }

        }

        return map;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete LRC.
     *
     * The <code>noAttributes</code> flag is missing on purpose, because
     * due to the resource handle, attribute lookups are already required.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     * @return a map indexed by the LFN. Each value is a collection
     * of replica catalog entries.
     *
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Set lfns, String handle) {
        return lookup(lfns,SITE_ATTRIBUTE,handle);
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete LRC. It returns the complete RCE for each entry i.e all the
     * attributes a pfn is associated with in addition to the one that is
     * the key for matching.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param name is the name of the attribute.
     * @param value is the value of the attribute.
     *
     * @return a map indexed by the LFN. Each value is a collection
     * of replica catalog entries.
     *
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Set lfns, String name, Object value) {
        //one has to do a bulk query in batches
        Set s = null;
        int size = mBatchSize;
        Map map = new HashMap(lfns.size());

        log("Number of files to query LRC " + lfns.size() +
            " in batch sizes of " + size, LogManager.DEBUG_MESSAGE_LEVEL);

        for (Iterator it = lfns.iterator(); it.hasNext(); ) {
            s = new HashSet(size);
            for (int j = 0; (j < size) && (it.hasNext()); j++) {
                s.add(it.next());
            }
            if (!s.isEmpty()) {
                //there is no conflict, as the keys are unique
                //via the set paradigm.
                //temp contains results indexed by lfn but each value
                //is a collection of ReplicaCatalogEntry objects
                //we query for all attributes as we are to return
                //complete RCE as stipulated by the interface.
                Map temp = bulkLookup(s, null);
                //iterate thru it
                for (Iterator it1 = temp.entrySet().iterator(); it1.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) it1.next();
                    Set pfns = subset( (Collection) entry.getValue(),
                                               name, value);
                    if (!pfns.isEmpty()) {
                        map.put(entry.getKey(), pfns);
                    }
                }
            }

        }

        return map;
    }


    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog. Retrieving full catalogs should be harmful, but
     * may be helpful in an online display or portal.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a set
     * of PFN strings.
     */
    public Map lookupNoAttributes(Set lfns) {
        //one has to do a bulk query in batches
        Set s = null;
        int size = mBatchSize;
        size = (size > lfns.size())?lfns.size():size;
        Map map = new HashMap(lfns.size());

        log("Number of files to query LRC " + lfns.size() +
            " in batch sizes of " + size,LogManager.DEBUG_MESSAGE_LEVEL);

        for (Iterator it = lfns.iterator(); it.hasNext(); ) {
            s = new HashSet(size);
            for (int j = 0; (j < size) && (it.hasNext()); j++) {
                s.add(it.next());
            }
            if (!s.isEmpty()) {
                //there is no conflict, as the keys are unique
                //via the set paradigm.
                map.putAll(bulkLookupNoAttributes(s));
            }

        }

        return map;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog. Retrieving full catalogs should be harmful, but
     * may be helpful in online display or portal.<p>
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     *
     * @return a map indexed by the LFN. Each value is a set of
     * physical filenames.
     */
    public Map lookupNoAttributes(Set lfns, String handle) {
        return lookupNoAttributes(lfns,SITE_ATTRIBUTE,handle);
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog. Retrieving full catalogs should be harmful, but
     * may be helpful in online display or portal.<p>
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     *
     * @return a map indexed by the LFN. Each value is a set of
     * physical filenames.
     */
    public Map lookupNoAttributes(Set lfns, String name, Object value) {
        //one has to do a bulk query in batches
        Set s = null;
        Collection c ;
        int size = mBatchSize;
        Map map = new HashMap(lfns.size());

        log("Number of files to query LRC " + lfns.size() +
            " in batch sizes of " + size,LogManager.DEBUG_MESSAGE_LEVEL);

        for (Iterator it = lfns.iterator(); it.hasNext(); ) {
            s = new HashSet(size);
            for (int j = 0; (j < size) && (it.hasNext()); j++) {
                s.add(it.next());
            }
            if (!s.isEmpty()) {
                //there is no conflict, as the keys are unique
                //via the set paradigm.
                //temp contains results indexed by lfn but each value
                //is a collection of ReplicaCatalogEntry objects
                Map temp = bulkLookup(s, name,value);
                //iterate thru it
                for (Iterator it1 = temp.entrySet().iterator(); it1.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) it1.next();
                    c = (Collection) entry.getValue();
                    //System.out.println("Entry is " + entry);
                    Set pfns = new HashSet(c.size());
                    for(Iterator cit = c.iterator();cit.hasNext();){
                        pfns.add( ((ReplicaCatalogEntry)cit.next()).getPFN());
                    }
                    if (!pfns.isEmpty()) {
                        map.put(entry.getKey(), pfns);
                    }
                }
            }

        }

        return map;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog. Retrieving full catalogs should be harmful, but
     * may be helpful in online display or portal.<p>
     *
     * At present it DOES NOT SUPPORT ATTRIBUTE MATCHING.
     *
     * @param constraints is mapping of keys 'lfn', 'pfn', to a string that
     * has some meaning to the implementing system. This can be a SQL
     * wildcard for queries, or a regular expression for Java-based memory
     * collections. Unknown keys are ignored. Using an empty map requests
     * the complete catalog.
     *
     * @return a map indexed by the LFN. Each value is a collection
     * of replica catalog entries.
     *
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Map constraints) throws ReplicaCatalogException{
        return (constraints.isEmpty())?
            lookup(list()):
            getAttributes(lookupLFNPFN(constraints),null,null);
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog. Retrieving full catalogs should be harmful, but
     * may be helpful in online display or portal. At present it does not
     * support attribute matching.
     *
     * @param constraints is mapping of keys 'lfn', 'pfn', or any
     * attribute name, e.g. the resource handle 'site', to a string that
     * has some meaning to the implementing system. This can be a SQL
     * wildcard for queries, or a regular expression for Java-based memory
     * collections. Unknown keys are ignored. Using an empty map requests
     * the complete catalog.
     *
     * @return A list of <code>MyRLSString2Bulk</code> objects containing
     *         the lfn in s1 field, and pfn in s2 field. The list is
     *         grouped by lfns. The set may be empty.
     */
    public List lookupLFNPFN(Map constraints) {
        if(isClosed()){
            //not connected to LRC
            //throw an exception??
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }
        /*
        if(constraints == null || constraints.isEmpty()){
            //return the set of all LFNs in the catalog
            return list();
        }*/

        List result = new ArrayList();
        boolean notFirst = false;

        for (Iterator i = constraints.keySet().iterator(); i.hasNext(); ) {
            String key = (String) i.next();
            if ( key.equals("lfn") ) {
                if(notFirst){
                    //do the AND(intersect)operation
                    result.retainAll(listLFNPFN((String)constraints.get(key),true));
                }
                else{
                    result = listLFNPFN( (String) constraints.get(key), true);
                }
            }
            else if ( key.equals("pfn") ) {
                if(notFirst){
                    //do the AND(intersect)operation
                    result.retainAll(listLFNPFN((String)constraints.get(key),false));
                }
                else{
                    result = listLFNPFN( (String) constraints.get(key), false);
                }
            }
            else{
                //just a warning
                log("Implementation does not support constraint " +
                    "matching of type " + key,
                    LogManager.WARNING_MESSAGE_LEVEL);
            }
            if(result.isEmpty()){
                //the intersection is already empty. No use matching further
                break;
            }
            notFirst = true;
        }

        //sort according to lfn
        Collections.sort(result,new RLSString2BulkComparator());

        return result;
    }

    /**
     * Lists all logical filenames in the catalog.
     *
     * @return a set of all logical filenames known to the catalog or null in
     *         case of not connected to the LRC or error.
     */
    public Set list() {
        return list("*");
    }


    /**
     * Lists a subset of all logical filenames in the catalog.
     *
     * @param constraint is a constraint for the logical filename only. It
     * is a string that has some meaning to the implementing system. This
     * can be a SQL wildcard for queries, or a regular expression for
     * Java-based memory collections.
     *
     * @return A set of logical filenames that match. The set may be empty
     */
    public Set list(String constraint) {
        List l = listLFNPFN(constraint,true);
        Set result = new HashSet(l.size());
        for(Iterator it = l.iterator();it.hasNext();){
            RLSString2Bulk rs = (RLSString2Bulk)it.next();
            result.add(rs.s1);
        }
        return result;
    }

    /**
     * Lists a subset of all LFN,PFN pairs in the catalog matching to
     * a pfn or a lfn constraint.
     *
     * @param constraint is a constraint for the logical filename only. It
     * is a string that has some meaning to the implementing system. This
     * can be a SQL wildcard for queries, or a regular expression for
     * Java-based memory collections.
     *
     * @return A set a list of <code>MyRLSString2Bulk</code> objects containing
     *         the lfn in s1 field, and pfn in s2 field. The list is
     *         grouped by lfns. The set may be empty.
     *
     * @see #getAttributes(List,String,Object)
     */
    public List listLFNPFN( String constraint, boolean lfnConstraint )
           throws ReplicaCatalogException{
        if(isClosed()){
            //not connected to LRC
            //throw an exception??
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }

        int size = getBatchSize();
        List l   = new ArrayList();
        ArrayList result = new ArrayList();
        int capacity = size;

        //do a wildcard query in batch sizes
        RLSOffsetLimit offset = new RLSOffsetLimit(0,size);
        while(true){
            try{
                l = (lfnConstraint)?
                    //do lfn matching
                    mLRC.getPFNWC(constraint, offset):
                    //do pfn matching
                    mLRC.getLFNWC(constraint, offset);

                    //we need to group pfns by lfn
                Collections.sort(l, new RLSString2Comparator());
            }
            catch(RLSException e){
                if(e.GetRC() == RLSClient.RLS_PFN_NEXIST ||
                   e.GetRC() == RLSClient.RLS_LFN_NEXIST ||
                   e.GetRC() == RLSClient.RLS_MAPPING_NEXIST){
                    log("listLFNPFN(String, boolean) :Mapping matching constraint " +
                        constraint + " does not exist",LogManager.ERROR_MESSAGE_LEVEL);
                }
                else{
                    //am not clear whether to throw the exception or what
                    log("list()", e,LogManager.ERROR_MESSAGE_LEVEL);
                }
                //return empty list
                return new ArrayList(0);
            }
            //result = new ArrayList(l.size());
            //increment the size of the list
            //but first the capacity
            capacity += l.size();
            result.ensureCapacity(capacity);

            for(Iterator it = l.iterator();it.hasNext();){
                RLSString2 res = (RLSString2)it.next();
                result.add(convert(res));
            }
            if(offset.offset == -1)
                break;//offset is set to -1 when no more results

        }

        return result;

    }


    /**
     * Inserts multiple mappings into the replica catalog. The input is a
     * map indexed by the LFN. The value for each LFN key is a collection
     * of replica catalog entries. Ends up doing a sequential insert for all
     * the entries instead of doing a bulk insert. Easier to track failure this
     * way.
     *
     * @param x is a map from logical filename string to list of replica
     * catalog entries.
     * @return the number of insertions.
     * @see ReplicaCatalogEntry
     */
    public int insert(Map x) {
        int result = 0;
        String lfn;
        ReplicaCatalogEntry rce = null;

//        Not doing sequential inserts any longer
//        Karan April 9, 2006
//        Collection c;
//        for(Iterator it = x.entrySet().iterator();it.hasNext();){
//            Map.Entry entry = (Map.Entry)it.next();
//            lfn = (String)entry.getKey();
//            c   = (Collection)entry.getValue();
//            log("Inserting entries for lfn " + lfn,
//                LogManager.DEBUG_MESSAGE_LEVEL);
//            for(Iterator pfnIt = c.iterator();pfnIt.hasNext();){
//                try{
//                    rce = (ReplicaCatalogEntry)pfnIt.next();
//                    insert(lfn,rce);
//                    res += 1;
//                }
//                catch(ReplicaCatalogException e){
//                    log("Inserting lfn->pfn " +
//                        lfn + "->" + rce.getPFN(),e,
//                        LogManager.ERROR_MESSAGE_LEVEL);
//                }
//            }
//        }
//        return res;

        int size = this.getBatchSize();
        int current = 0;
        String pfn;
        List lfnPfns = new ArrayList(size);
        List attrs   = new ArrayList(size);
        CatalogException exception = new ReplicaCatalogException();

        //indexed by pfn and values as RLSAttributeObject objects
        Map attrMap  = new HashMap(size);

        for (Iterator it = x.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry)it.next();
            lfn = (String)entry.getKey();
            Collection c = (Collection)entry.getValue();

            //traverse through the rce's for the pfn's
            for(Iterator pfnIt = c.iterator();pfnIt.hasNext();){
                rce = (ReplicaCatalogEntry)pfnIt.next();
                pfn = rce.getPFN();
                lfnPfns.add(new RLSString2(lfn,pfn));

                //increment current only once per pfn
                if(rce.getAttributeCount() == 0)current++;

                //build the attributes list
                for(Iterator attrIt = rce.getAttributeIterator(); attrIt.hasNext();current++){
                    String key   = (String)attrIt.next();
                    RLSAttribute attr = new RLSAttribute(key,RLSAttribute.LRC_PFN,
                                                         (String)rce.getAttribute(key));
                    attrs.add(new RLSAttributeObject(attr,pfn));
                    attrMap.put(pfn,new RLSAttributeObject(attr,pfn));
                }
            }
            //check if diff is more than batch size
            if( current >= size){
                //we have the subset of RCE's on which we
                //want to do bulk inserts, and the value till
                //we want to do bulk inserts
                try{
                    result += bulkInsert(lfnPfns, attrMap);
                }
                catch(ReplicaCatalogException e){exception.setNextException(e);}

                //reset data structures
                current = 0;
                lfnPfns.clear();
                attrs.clear();
                attrMap.clear();
            }
        }
        //check for the last bunch
        if(!lfnPfns.isEmpty()){
            //we have the subset of RCE's on which we
            //want to do bulk inserts, and the value till
            //we want to do bulk inserts
            try{
                result += bulkInsert(lfnPfns, attrMap);
            }catch(ReplicaCatalogException e){exception.setNextException(e);}
            current = 0;
        }

        //throw an exception only if a nested exception
        if( (exception = exception.getNextException()) != null) throw exception;

        return result;
    }

    /**
     * Calls the bulk delete on the mappings. This function can timeout if the
     * size of the list passed is too large.
     *
     * @param lfnPfns  list of <code>RLSString2</code> objects containing the
     *                 lfn pfn mappings to be deleted.
     *
     * @return the number of items deleted
     *
     * @throws ReplicaCatalogException in case of error
     */
    private int bulkDelete(List lfnPfns) throws ReplicaCatalogException{
        if(isClosed()){
            //not connected to LRC
            //throw an exception??
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }

        //result only tracks successful lfn->pfn mappings
        int result = lfnPfns.size();
        Collection failedDeletes;
        CatalogException exception = new ReplicaCatalogException();

        //do a bulk delete
        //FIX ME: The deletes should have been done in batches.
        try{
            failedDeletes = mLRC.deleteBulk( (ArrayList)lfnPfns);
        }
        catch(RLSException e){
            mLogger.log("RLS: Bulk Delete " ,e , LogManager.ERROR_MESSAGE_LEVEL);
            throw new ReplicaCatalogException("RLS: Bulk Delete " + e.getMessage());
        }

        if(!failedDeletes.isEmpty()){
            result -= failedDeletes.size();
            //FIXME: Do we really care about failed deletes
            //and reporting why deletes failed.
            // i think we do.
            RLSString2Bulk rs;
            int error;
            for(Iterator it = failedDeletes.iterator();it.hasNext();){
                rs = (RLSString2Bulk)it.next();
                error = rs.rc;

                if(error == RLSClient.RLS_PFN_NEXIST ||
                   error == RLSClient.RLS_LFN_NEXIST ||
                   error == RLSClient.RLS_MAPPING_NEXIST){

                    log("Mapping " + rs.s1 + "->" + rs.s2 +
                        " does not exist",LogManager.DEBUG_MESSAGE_LEVEL);
                }
                else{
                    exception.setNextException(exception(rs));
                }
            }

        }

        //throw an exception only if a nested exception
        if( (exception = exception.getNextException()) != null) throw exception;

        return result;

    }

    /**
     * Calls the bulk insert on the mappings. This function can timeout if the
     * size of the list passed is too large.
     *
     * @param lfnPfns  list of <code>RLSString2</code> objects containing the
     *                 lfn pfn mappings to be inserted.
     * @param attrMap  a map indexed by pfn and values as RLSAttributeObject objects.
     *
     * @return the number of items inserted
     *
     * @throws ReplicaCatalogException in case of error
     */
    private int bulkInsert(List lfnPfns, Map attrMap){
        if(isClosed()){
            //not connected to LRC
            //throw an exception??
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }

        //result only tracks successful lfn->pfn mappings
        int result = lfnPfns.size();

        List failedCreates;
        List failedAdds;
        CatalogException exception = new ReplicaCatalogException();


        try{
            /* bulk insert on mappings starts*/
            failedCreates = mLRC.createBulk( (ArrayList) lfnPfns);

            //try to do a bulkAdd on the failed creates
            List bulkAdd = new ArrayList(failedCreates.size());
            for (Iterator it = failedCreates.iterator(); it.hasNext(); ) {
                RLSString2Bulk rs = (RLSString2Bulk) it.next();
                if (rs.rc == RLSClient.RLS_LFN_EXIST) {
                    //s1 is lfn and s2 is pfn
                    bulkAdd.add(new RLSString2(rs.s1, rs.s2));
                }
                else {
                    exception.setNextException(exception(rs));
                    result--;
                }
            }

            //do a bulk add if list non empty
            if (!bulkAdd.isEmpty()) {
                failedAdds = mLRC.addBulk( (ArrayList) bulkAdd);
                //pipe all the failed adds to the exception
                for (Iterator it = failedAdds.iterator(); it.hasNext(); ) {
                    RLSString2Bulk rs = (RLSString2Bulk) it.next();

                    //just log that mapping already exists
                    if (rs.rc == RLSClient.RLS_MAPPING_EXIST) {
                        //we want to log instead of throwning an exception
                        log("LFN-PFN Mapping alreadys exists in LRC "
                            + mLRCURL + " for " + rs.s1 + "->" + rs.s2,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                        result --;

                    }
                    else {
                        exception.setNextException(exception(rs));
                        result--;
                    }
                }
            }
            /*bulk insert on mappings ends */

            /*bulk insert on attributes starts */
            ArrayList failedAttrs;//the failed attributes

            //build the attribute list
            ArrayList attrs = new ArrayList(attrMap.size());
            int num = 0;
            for(Iterator it = attrMap.values().iterator();it.hasNext();num++){
                attrs.add(it.next());
            }

            //try a bulk add on attributes assuming attrs already exist
            failedAttrs = mLRC.attributeAddBulk((ArrayList)attrs);

            //go through the failed attributes and create them
            for(Iterator it = failedAttrs.iterator();it.hasNext();){
                RLSString2Bulk s2b = (RLSString2Bulk)it.next();
                /*
                RLSAttribute attributeToAdd =
                                 new RLSAttribute(s2b.s2,RLSAttribute.LRC_PFN,
                                                  (String)tuple.getAttribute(s2b.s2));
                */


                //s1 is the pfn
                //s2 is the attribute name
                String pfn = s2b.s1;
                RLSAttributeObject attrObject = (RLSAttributeObject)attrMap.get(pfn);
                RLSAttribute attributeToAdd =
                                      attrObject.attr;

                if(s2b.rc == RLSClient.RLS_ATTR_NEXIST){
                    //we need to create the attribute
                    log("Creating an attribute name " + s2b.s2 +
                        " for pfn " + pfn, LogManager.DEBUG_MESSAGE_LEVEL);
                    try{
                        //FIXME : should have done a bulkAttributeCreate that doesnt exist
                        mLRC.attributeCreate(s2b.s2, RLSAttribute.LRC_PFN,
                                             RLSAttribute.STR);
                        //add the attribute in sequentially instead of bulk
                        mLRC.attributeAdd(pfn,attributeToAdd);

                    }
                    catch(RLSException e){
                        //ignore any attribute already exist error
                        //case of multiple creates of same attribute
                        if(e.GetRC() != RLSClient.RLS_ATTR_EXIST){
                            exception.setNextException(
                                   new ReplicaCatalogException("Adding attrib to pfn " +
                                   pfn + " " + e.getMessage()));
                        }
                    }
                }
                else if(s2b.rc == RLSClient.RLS_ATTR_EXIST){
                    log("Attribute " + s2b.s2 + " for pfn " + pfn +
                        " already exists", LogManager.DEBUG_MESSAGE_LEVEL);
                    //get the existing value of attribute
                    List l = null;
                    try{
                        l = mLRC.attributeValueGet(pfn, s2b.s2, RLSAttribute.LRC_PFN);
                    }
                    catch(RLSException e){
                        exception.setNextException(
                            new ReplicaCatalogException("Getting value of existing attrib "+
                                                            e.getMessage()));
                    }
                    if(l == null || l.isEmpty() || l.size() > 1){
                        log("Contents of list are " + l,LogManager.DEBUG_MESSAGE_LEVEL);
                        //should never happen
                        log("Unreachable case.",LogManager.FATAL_MESSAGE_LEVEL);
                        throw new ReplicaCatalogException(
                            "Whammy while trying to get value of an exisiting attribute " +
                            s2b.s2 + " associated with PFN " + pfn);
                    }

                    //try to see if it matches with the existing value
                    RLSAttribute attribute = (RLSAttribute)l.get(0);
                    if(!attribute.GetStrVal().equalsIgnoreCase(
                        attributeToAdd.GetStrVal())){

                        //log a warning saying updating value
                        mLogMsg = "Existing value for attribute " + s2b.s2 +
                            " associated with PFN " + pfn +
                            " updated with new value " + attributeToAdd.GetStrVal();

                        //update the value
                        try{
                            mLRC.attributeModify(pfn, attributeToAdd);
                            log(mLogMsg,LogManager.WARNING_MESSAGE_LEVEL);
                        }
                        catch(RLSException e){
                            exception.setNextException(
                                new ReplicaCatalogException("RLS Exception "+ e.getMessage()));
                        }
                    }
                }
                else {
                    exception.setNextException(exception(s2b));
                }
            }

            /*bulk insert on attributes ends */

        }
        catch(RLSException e){
            exception.setNextException(
                new ReplicaCatalogException("RLS Exception "+ e.getMessage()));
        }



        //throw an exception only if a nested exception
        if( (exception = exception.getNextException()) != null) throw exception;

        return result;
    }

    /**
     * Inserts a new mapping into the replica catalog. The attributes are added
     * in bulk assuming the attribute definitions already exist. If an attribute
     * definition does not exist, it is created and inserted. Note there is no
     * notion of transactions in LRC. It assumes all the attributes are of type
     * String.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param tuple is the physical filename and associated PFN attributes.
     *
     * @return number of insertions, should always be 1. On failure,
     * throws an exception instead of returning zero.
     */
    public int insert(String lfn, ReplicaCatalogEntry tuple) {
        Map m = new HashMap(1);
        List l = new ArrayList(1); l.add(tuple);
        m.put(lfn,l);
        return insert(m);

//        Just composing the call to insert(Map method)
//        Only one code handles inserts. Karan April 12, 2006
//        if(isClosed()){
//            //not connected to LRC
//            //throw an exception??
//            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
//        }
//        int res = 0;

//        //we have no notion of transaction at this point.
//        String pfn = tuple.getPFN();
//        try{
//            //insert the pfn
//            mLRC.add(lfn, pfn);
//        }
//        catch(RLSException e){
//            if(e.GetRC() == RLSClient.RLS_LFN_NEXIST){
//                //the first instance of the lfn, so we add
//                //instead of creating the mapping
//                try{
//                    mLRC.create(lfn, pfn);
//                }
//                catch(RLSException ex){
//                    throw new ReplicaCatalogException("RLS Exception "+ ex.getMessage());
//                }
//            }
//            else if(e.GetRC() == RLSClient.RLS_MAPPING_EXIST){
//                log("LFN-PFN Mapping alreadys exists in LRC "
//                    + mLRCURL + " for " + lfn + "->" + pfn,
//                    LogManager.ERROR_MESSAGE_LEVEL);
//                return res;
//            }
//            else
//                throw new ReplicaCatalogException("RLS Exception "+ e.getMessage());
//        }
//
//        //we need to add attributes in bulk
//        String key;
//        ArrayList failedAttrs;//the failed attributes
//        ArrayList attrs = new ArrayList(tuple.getAttributeCount());
//        for(Iterator it = tuple.getAttributeIterator(); it.hasNext();){
//            key   = (String)it.next();
//            RLSAttribute attr = new RLSAttribute(key,RLSAttribute.LRC_PFN,
//                                                 (String)tuple.getAttribute(key));
//            attrs.add(new RLSAttributeObject(attr,pfn));
//
//        }
//
//        try{
//            failedAttrs = mLRC.attributeAddBulk(attrs);
//        }
//        catch(RLSException e){
//            throw new ReplicaCatalogException("RLS Exception "+ e.getMessage());
//        }
//
//        //go through the failed attributes and create them
//        for(Iterator it = failedAttrs.iterator();it.hasNext();){
//            RLSString2Bulk s2b = (RLSString2Bulk)it.next();
//            RLSAttribute attributeToAdd = new RLSAttribute(s2b.s2,RLSAttribute.LRC_PFN,
//                                                          (String)tuple.getAttribute(s2b.s2));
//            //s1 is the pfn
//            //s2 is the attribute name
//            if(s2b.rc == RLSClient.RLS_ATTR_NEXIST){
//                //we need to create the attribute
//                log("Creating an attribute name " + s2b.s2 +
//                    " for pfn " + pfn, LogManager.DEBUG_MESSAGE_LEVEL);
//                try{
//                    mLRC.attributeCreate(s2b.s2, RLSAttribute.LRC_PFN,
//                                         RLSAttribute.STR);
//                    //add the attribute in sequentially instead of bulk
//                    mLRC.attributeAdd(pfn,attributeToAdd);
//
//                }
//                catch(RLSException e){
//                    throw new ReplicaCatalogException("RLS Exception "+ e.getMessage());
//                }
//            }
//            else if(s2b.rc == RLSClient.RLS_ATTR_EXIST){
//                log("Attribute " + s2b.s2 + " for pfn " + pfn +
//                    " already exists", LogManager.DEBUG_MESSAGE_LEVEL);
//                //get the existing value of attribute
//                List l = null;
//                try{
//                    l = mLRC.attributeValueGet(pfn, s2b.s2, RLSAttribute.LRC_PFN);
//                }
//                catch(RLSException e){
//                    throw new ReplicaCatalogException("RLS Exception "+ e.getMessage());
//                }
//                if(l == null || l.isEmpty() || l.size() > 1){
//                    log("Contents of list are " + l,LogManager.DEBUG_MESSAGE_LEVEL);
//                    //should never happen
//                    log("Unreachable case.",LogManager.FATAL_MESSAGE_LEVEL);
//                    throw new ReplicaCatalogException(
//                        "Whammy while trying to get value of an exisiting attribute " +
//                        s2b.s2 + " associated with PFN " + pfn);
//                }
//                //try to see if it matches with the existing value
//                RLSAttribute attribute = (RLSAttribute)l.get(0);
//                if(!attribute.GetStrVal().equalsIgnoreCase(
//                                                   attributeToAdd.GetStrVal())){
//
//                    //log a warning saying updating value
//                    mLogMsg = "Existing value for attribute " + s2b.s2 +
//                        " associated with PFN " + pfn +
//                        " updated with new value " + attributeToAdd.GetStrVal();
//
//                    //update the value
//                    try{
//                        mLRC.attributeModify(pfn, attributeToAdd);
//                        log(mLogMsg,LogManager.WARNING_MESSAGE_LEVEL);
//                    }
//                    catch(RLSException e){
//                        throw new ReplicaCatalogException("RLS Exception" +
//                                                          e.getMessage());
//                    }
//                }
//            }
//            else{
//                throw new ReplicaCatalogException(
//                    "Unknown Error while adding attributes. RLS Error Code " +
//                    s2b.rc);
//            }
//        }
//
//        return 1;

    }

    /**
     * Inserts a new mapping into the replica catalog. This is a
     * convenience function exposing the resource handle. Internally,
     * the <code>ReplicaCatalogEntry</code> element will be contructed, and passed to
     * the appropriate insert function.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param pfn is the physical filename associated with it.
     * @param handle is a resource handle where the PFN resides.
     *
     * @return number of insertions, should always be 1. On failure,
     * throw an exception, don't use zero.
     *
     * @see #insert( String, ReplicaCatalogEntry )
     * @see ReplicaCatalogEntry
     */
    public int insert(String lfn, String pfn, String handle) {
        //prepare the appropriate ReplicaCatalogEntry object
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry(pfn,handle);
        return insert(lfn,rce);
    }

    /**
     * Deletes multiple mappings into the replica catalog. The input is a
     * map indexed by the LFN. The value for each LFN key is a collection
     * of replica catalog entries. On setting matchAttributes to false, all entries
     * having matching lfn pfn mapping to an entry in the Map are deleted.
     * However, upon removal of an entry, all attributes associated with the pfn
     * also evaporate (cascaded deletion).
     * The deletes are done in batches.
     *
     * @param x                is a map from logical filename string to list of
     *                         replica catalog entries.
     * @param matchAttributes  whether mapping should be deleted only if all
     *                         attributes match.
     *
     * @return the number of deletions.
     * @see ReplicaCatalogEntry
     */
    public int delete( Map x , boolean matchAttributes){
        int result = 0;
        if(isClosed()){
            //not connected to LRC
            //throw an exception??
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }

        String lfn,pfn;
        ReplicaCatalogEntry rce;
        Collection c;
        CatalogException exception = new ReplicaCatalogException();

        if(matchAttributes){
            //do a sequential delete for the time being
            for(Iterator it = x.entrySet().iterator();it.hasNext();){
                Map.Entry entry = (Map.Entry)it.next();
                lfn = (String)entry.getKey();
                c   = (Collection)entry.getValue();

                //iterate through all RCE's for this lfn and delete
                for(Iterator rceIt = c.iterator();rceIt.hasNext();){
                    rce = (ReplicaCatalogEntry)it.next();
                    result += delete(lfn,rce);
                }
            }
        }
        else{
            //we can use bulk delete
            int size = this.getBatchSize();
            int current = 0;
            List lfnPfns = new ArrayList(size);
            for (Iterator it = x.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry entry = (Map.Entry)it.next();
                lfn = (String)entry.getKey();
                c = (Collection)entry.getValue();

                //traverse through the rce's for the pfn's
                for(Iterator pfnIt = c.iterator();pfnIt.hasNext();){
                    rce = (ReplicaCatalogEntry) pfnIt.next();
                    pfn = rce.getPFN();
                    lfnPfns.add(new RLSString2(lfn, pfn));
                    current++;

                    //check if diff is more than batch size
                    if( current >= size){
                        //we have the subset of RCE's on which we
                        //want to do bulk deletes
                        try{
                            result += bulkDelete(lfnPfns);
                        }
                        catch(ReplicaCatalogException e){exception.setNextException(e);}

                        current = 0;
                        lfnPfns.clear();
                    }

                }
            }
            //check for the last bunch
            if(!lfnPfns.isEmpty()){
                //we have the subset of RCE's on which we
                //we want to do bulk deletes
                try{
                    result += bulkDelete(lfnPfns);
                }
                catch(ReplicaCatalogException e){exception.setNextException(e);}

                current = 0;
            }
        }

        //throw an exception only if a nested exception
        if( (exception = exception.getNextException()) != null) throw exception;

        return result;
    }



    /**
     * Deletes a specific mapping from the replica catalog. We don't care
     * about the resource handle. More than one entry could theoretically
     * be removed. Upon removal of an entry, all attributes associated
     * with the PFN also evaporate (cascading deletion) automatically at the
     * RLS server end.
     *
     * @param lfn is the logical filename in the tuple.
     * @param pfn is the physical filename in the tuple.
     *
     * @return the number of removed entries.
     */
    public int delete(String lfn, String pfn) {
        int res = 0;
        if(isClosed()){
            //not connected to LRC
            //throw an exception??
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }

        try{
            mLRC.delete(lfn,pfn);
            res++;
        }
        catch(RLSException e){
            if(e.GetRC() == RLSClient.RLS_PFN_NEXIST ||
               e.GetRC() == RLSClient.RLS_LFN_NEXIST ||
               e.GetRC() == RLSClient.RLS_MAPPING_NEXIST){
                log("Mapping " + lfn + "->" + pfn +
                    " does not exist",LogManager.DEBUG_MESSAGE_LEVEL);
            }
            else{
                throw new ReplicaCatalogException("Error while deleting mapping " +
                                           e.getMessage());
            }
        }
        return res;
    }

    /**
     * Deletes a very specific mapping from the replica catalog. The LFN
     * must be matches, the PFN, and all PFN attributes specified in the
     * replica catalog entry. More than one entry could theoretically be
     * removed. Upon removal of an entry, all attributes associated with
     * the PFN also evaporate (cascading deletion).
     *
     * @param lfn is the logical filename in the tuple.
     * @param tuple is a description of the PFN and its attributes.
     * @return the number of removed entries, either 0 or 1.
     */
    public int delete(String lfn, ReplicaCatalogEntry tuple) {
        int res = 0;
        if(isClosed()){
            //not connected to LRC
            //throw an exception??
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }

        //get hold of all the RCE in this LRC matching to lfn
        Collection c = lookup(lfn);
        ReplicaCatalogEntry rce;
        for(Iterator it = c.iterator();it.hasNext();){
            rce = (ReplicaCatalogEntry)it.next();
            if(rce.equals(tuple)){
                //we need to delete the rce
                //cascaded deletes take care of the attribute deletes
                delete(lfn,tuple.getPFN());
                res++;
            }
        }

        return res;
    }

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog
     * where the PFN attribute is found, and matches exactly the object
     * value. This method may be useful to remove all replica entries that
     * have a certain MD5 sum associated with them. It may also be harmful
     * overkill.
     *
     * @param lfn is the logical filename to look for.
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     *
     * @return the number of removed entries.
     */
    public int delete(String lfn, String name, Object value) {
        int result = 0;
        Collection c = null;
        if(isClosed()){
            //not connected to LRC
            //throw an exception??
            throw new ReplicaCatalogException(LRC_NOT_CONNECTED_MSG + this.mLRCURL);
        }

        //query lookup for that lfn and delete accordingly.
        Set s = new HashSet(1);
        s.add(lfn);
        Map map = this.lookupNoAttributes(s,name,value);
        if(map == null || map.isEmpty()){
            return 0;
        }

        //we need to pipe this into a list of RLSString2 objects
        ArrayList lfnPfns = new ArrayList(3);
        for(Iterator it = map.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry) it.next();
            lfn = (String)entry.getKey();

            for (Iterator it1 = ( (Set) entry.getValue()).iterator();
                 it1.hasNext(); ) {
                RLSString2 lfnPfn = new RLSString2(lfn, (String) it1.next());
                lfnPfns.add(lfnPfn);
                result++;
            }

        }

        try{
            c = mLRC.deleteBulk(lfnPfns);
        }
        catch(RLSException e){
            log("remove(Set)" ,e,LogManager.ERROR_MESSAGE_LEVEL);
        }

        //c should be empty ideally
        if(!c.isEmpty()){
            result -= c.size();
            log("Removing lfns remove(Set)" + c, LogManager.ERROR_MESSAGE_LEVEL);
        }
        return result;

    }

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog
     * where the resource handle is found. Karan requested this
     * convenience method, which can be coded like
     * <pre>
     *  delete( lfn, SITE_ATTRIBUTE, handle )
     * </pre>
     *
     * @param lfn is the logical filename to look for.
     * @param handle is the resource handle
     *
     * @return the number of entries removed.
     *
     * @see #SITE_ATTRIBUTE
     */
    public int deleteByResource(String lfn, String handle) {
        return delete(lfn,SITE_ATTRIBUTE,handle);
    }

    /**
     * Removes all mappings for a set of LFNs.
     *
     * @param lfn is a set of logical filename to remove all mappings for.
     *
     * @return the number of removed entries.
     */
    public int remove(String lfn) {
        //first get hold of all the pfn mappings for the lfn
        Collection c = this.lookupNoAttributes(lfn);
        int result   = 0;
        if(c == null || c.isEmpty()){
            return 0;
        }

        //we need to pipe this into a list of RLSString2Bulk objects
        result = c.size();
        ArrayList lfnPfns = new ArrayList(result);
        for(Iterator it = c.iterator();it.hasNext();){
            RLSString2 lfnPfn = new RLSString2(lfn,(String)it.next());
            lfnPfns.add(lfnPfn);
        }

        //do a bulk delete
        try{
            c = mLRC.deleteBulk(lfnPfns);
        }
        catch(RLSException e){
            log("remove(String)",e,LogManager.ERROR_MESSAGE_LEVEL);
        }

        //c should be empty ideally
        if(!c.isEmpty()){
            result -= c.size();
            log("remove(String)" + c,LogManager.ERROR_MESSAGE_LEVEL);
        }
        return result;
    }

    /**
     * Removes all mappings for a set of LFNs.
     *
     * @param lfns is a set of logical filename to remove all mappings for.
     *
     * @return the number of removed entries.
     */
    public int remove(Set lfns) {
        String lfn   = null;
        Collection c = null;
        int result   = 0;

        //first get hold of all the pfn mappings for the lfn
        Map map = this.lookupNoAttributes(lfns);
        if(map == null || map.isEmpty()){
            return 0;
        }
        //we need to pipe this into a list of RLSString2 objects
        ArrayList lfnPfns = new ArrayList(map.keySet().size());
        for(Iterator it = map.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry) it.next();
            lfn = (String)entry.getKey();
            for (Iterator it1 = ( (Set) entry.getValue()).iterator();
                 it1.hasNext(); ) {
                RLSString2 lfnPfn = new RLSString2(lfn, (String) it1.next());
                lfnPfns.add(lfnPfn);
                result++;
            }
        }

        //do a bulk delete
        //FIX ME: The deletes should have been done in batches.
        try{
            c = mLRC.deleteBulk(lfnPfns);
        }
        catch(RLSException e){
            log("remove(Set)" + e,LogManager.ERROR_MESSAGE_LEVEL);
        }

        //c should be empty ideally
        if(!c.isEmpty()){
            result -= c.size();
            log("remove(Set)" + c,LogManager.ERROR_MESSAGE_LEVEL);
            for(Iterator it = c.iterator();it.hasNext();){
                RLSString2Bulk rs2 = (RLSString2Bulk)it.next();
                System.out.println("(" + rs2.s1 + "->" + rs2.s2 +"," +
                                   rs2.rc + ")");
            }
        }
        return result;

    }

    /**
     * Removes all entries associated with a particular resource handle.
     * This is useful, if a site goes offline. It is a convenience method,
     * which calls the generic <code>removeByAttribute</code> method.
     *
     * @param handle is the site handle to remove all entries for.
     *
     * @return the number of removed entries.
     *
     * @see #removeByAttribute( String, Object )
     */
    public int removeByAttribute(String handle) {
        return removeByAttribute(SITE_ATTRIBUTE,handle);
    }

    /**
     * Removes all entries from the replica catalog where the PFN attribute
     * is found, and matches exactly the object value.
     *
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     *
     * @return the number of removed entries.
     */
    public int removeByAttribute(String name, Object value) {
        String lfn   = null;
        String pfn   = null;
        Collection c = null;
        int result   = 0;

        //get hold of all the lfns in the lrc
        Set s = list();

        //first get hold of all the pfn mappings for the lfn
        Map map = this.lookup(s,name,value);
        if(map == null || map.isEmpty()){
            return 0;
        }

        //we need to pipe this into a list of RLSString2 objects
        ArrayList lfnPfns = new ArrayList(result);
        for(Iterator it = map.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry) it.next();
            lfn = (String)entry.getKey();
            //System.out.println(lfn + " ->");
            for (Iterator it1 = ( (Set) entry.getValue()).iterator();
                 it1.hasNext(); ) {
                pfn = ((ReplicaCatalogEntry)it1.next()).getPFN();
                RLSString2 lfnPfn = new RLSString2(lfn, pfn);
                lfnPfns.add(lfnPfn);
                result++;
                //System.out.print(lfnPfn.s2 + ",");
            }
        }

        //do a bulk delete
        //FIX ME: The deletes should have been done in batches.
        try{
            c = mLRC.deleteBulk(lfnPfns);
        }
        catch(RLSException e){
            throw new ReplicaCatalogException("Bulk Delete: " + e.getMessage());
        }

        //c should be empty ideally
        if(!c.isEmpty()){
            result -= c.size();
            log("removeByAttribute(String,Object)" + c,
                        LogManager.ERROR_MESSAGE_LEVEL);
        }
        return result;
    }

    /**
     * Removes everything. Use with caution!
     *
     * @return the number of removed entries.
     */
    public int clear() {

        //do a bulk delete
        //FIX ME: The deletes should have been done in batches.
        try{
            mLRC.clear();
        }
        catch(RLSException e){
            log("clear()",e,LogManager.ERROR_MESSAGE_LEVEL);
        }
        return 0;
        /*
        String lfn   = null;
        String pfn   = null;
        Collection c = null;
        int result   = 0;

        //get hold of all the lfns in the lrc
        Set s = list();

        //first get hold of all the pfn mappings for the lfn
        Map map = this.lookupNoAttributes(s);
        if(map == null || map.isEmpty()){
            return 0;
        }

        //we need to pipe this into a list of RLSString2 objects
        ArrayList lfnPfns = new ArrayList(result);
        for(Iterator it = map.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry) it.next();
            lfn = (String)entry.getKey();
            //System.out.println(lfn + " ->");
            for (Iterator it1 = ( (Set) entry.getValue()).iterator();
                 it1.hasNext(); ) {
                pfn = ((String)it1.next());
                RLSString2 lfnPfn = new RLSString2(lfn, pfn);
                lfnPfns.add(lfnPfn);
                result++;
                //System.out.print(lfnPfn.s2 + ",");
            }
        }

        //do a bulk delete
        //FIX ME: The deletes should have been done in batches.
        try{
            c = mLRC.deleteBulk(lfnPfns);
        }
        catch(RLSException e){
            log("clear()",e,LogManager.ERROR_MESSAGE_LEVEL);
        }

        //c should be empty ideally
        if(!c.isEmpty()){
            result -= c.size();
            log("clear()" + c,LogManager.ERROR_MESSAGE_LEVEL);
        }
        return result;
         */
    }

    /**
     * Explicitely free resources before the garbage collection hits.
     */
    public void close() {
        try {
            if (mRLS != null) {
                mRLS.Close();
            }
        }
        catch (RLSException e) {
            //ignore
        }
        finally {
            mRLS = null;
        }
    }

    /**
     * Returns whether the connection to the RLS with which this instance is
     * associated is closed or not.
     *
     * @return true, if the implementation is disassociated, false otherwise.
     * @see #close()
     */
    public boolean isClosed() {
        return (mRLS == null);
    }


    /**
     * It returns the timeout value in seconds after which to timeout in case of
     * no activity from the LRC.
     *
     * Referred to by the "lrc.timeout" property.
     *
     * @return the timeout value if specified else,
     *         the value specified by "rls.timeout" property, else
     *         DEFAULT_LRC_TIMEOUT.
     *
     * @see #DEFAULT_LRC_TIMEOUT
     */
    protected int getTimeout(Properties properties) {
        String prop = properties.getProperty( this.LRC_TIMEOUT_KEY);

        //if prop is null get rls timeout,
        prop = (prop == null)? properties.getProperty(this.RLS_TIMEOUT_KEY):prop;

        int val = 0;
        try {
            val = Integer.parseInt( prop );
        } catch ( Exception e ) {
            val = Integer.parseInt( DEFAULT_LRC_TIMEOUT );
        }
        return val;

    }


    /**
     * Returns the site handle associated with the pfn at the lrc to which
     * the instance of this application binds. It returns <code>UNDEFINED_SITE
     * </code> even when the pfn is not in the lrc.
     *
     * @param pfn            The pfn with which the attribute is associated.
     *
     * @return value of the attribute if found
     *         else UNDEFINED_POOL
     */
    private String getSiteHandle(String pfn) {
        return getSiteHandle(mLRC, pfn);
    }

    /**
     * Returns the site handle associated with a pfn at the lrc associated
     * with the <code>RLSClient</code> passed. It returns <code>UNDEFINED_SITE
     * </code> even when the pfn is not in the lrc.
     *
     * @param lrc  the handle to the lrc , where the attributes are stored.
     * @param pfn  the pfn with which the attribute is associated.
     *
     * @return value of the attribute if found
     *         else UNDEFINED_POOL
     *
     */
    private String getSiteHandle(RLSClient.LRC lrc, String pfn) {
        String poolAttr = getAttribute(lrc, pfn, SITE_ATTRIBUTE);
        return (poolAttr == null) ?
            defaultResourceHandle() :
            poolAttr;
    }

    
    
    /**
     * Returns the default value that is to be assigned to site handle
     * for a replica catalog entry.
     * 
     * @return default site handle
     */
    private String defaultResourceHandle(){
        return ( this.mDefaultSiteAttribute == null ) ?
                 LRC.UNDEFINED_SITE:
                 this.mDefaultSiteAttribute;
    }
    
    /**
     * Sets the  resource handle in an attribute map.
     * The resource handle is set to the default site handle if the map
     * does not contain the site attribute key.
     * 
     * @param m the attribute map.
     * 
     * @see #defaultResourceHandle() 
     */
    private void setResourceHandle( Map<String,String> m ){
        String dflt = defaultResourceHandle();
        //update the site attribute only if the default
        //attribute is other than undefined site
        if( m.containsKey( LRC.SITE_ATTRIBUTE) && !dflt.equals(LRC.UNDEFINED_SITE ) ){
            //populate the default site handle
            m.put( LRC.SITE_ATTRIBUTE, dflt );
        }
        else if( !m.containsKey( LRC.SITE_ATTRIBUTE ) ){
            //populate the default site handle
            m.put( LRC.SITE_ATTRIBUTE, dflt );
        }
    }
    
    /**
     * Sets the  resource handle in an attribute map.
     * The resource handle is set to the default site handle if the map
     * does not contain the site attribute key.
     * 
     * @param rce   the <code>ReplicaCatalogEntry</code>
     * 
     * @see #defaultResourceHandle() 
     */
    private void setResourceHandle( ReplicaCatalogEntry rce ){
        String dflt = defaultResourceHandle();
        //update the site attribute only if the default
        //attribute is other than undefined site
        if( rce.hasAttribute( LRC.SITE_ATTRIBUTE) && !dflt.equals(LRC.UNDEFINED_SITE ) ){
            //populate the default site handle
            rce.setResourceHandle( dflt );
        }
        else if( ! rce.hasAttribute( LRC.SITE_ATTRIBUTE ) ){
            //populate the default site handle
            rce.setResourceHandle( dflt );
        }
    }
    
    /**
     * Retrieves from the lrc, associated with this instance all the
     * attributes associated with the <code>pfn</code> in a map. All the
     * attribute values are stored as String.
     *
     * @param pfn  the pfn with which the attribute is associated.
     *
     * @return <code>Map</code>containing the attribute keys and values,
     *         else an empty Map.
     */
    private Map getAttributes(String pfn) {
        return getAttributes(mLRC, pfn);
    }

    /**
     * Retrieves from the lrc associated with this instance, all the
     * attributes associated with the lfn-pfns in a map indexed by the lfn.
     * The value for each entry is a collection of
     * <code>ReplicaCatalogEntry</code> objects.
     * All the attribute values are stored as String.
     *
     * If the attribute value passed is not null, then explicit matching occurs
     * on attribute values in addition.
     *
     * @param lfnPfns  a list of <code>RLSString2Bulk</code> objects containing
     *                 the lfn in s1 field, and pfn in s2 field. The list is
     *                 assumed to be grouped by lfns.
     * @param attrKey  the name of attribute that needs to be queried for each
     *                 pfn. a value of null denotes all attributes.
     * @param attrVal  the value of the attribute that should be matching.
     *
     * @return a map indexed by the LFN. Each value is a collection
     * of replica catalog entries for the LFN.
     */
    private Map getAttributes(List lfnPfns, String attrKey, Object attrVal) {
        Map result = new HashMap();
        String curr = null;
        String prev = null;
        //loss of information. i should have known the size at this pt!
        List l = new ArrayList();
        ArrayList pfns = new ArrayList(lfnPfns.size());
        int size = mBatchSize;
        ReplicaCatalogEntry entry = null;
        Map temp = new HashMap();
        Map pfnMap = new HashMap(); //contains pfn and their ReplicaCatalogEntry objects

        //sanity check
        if(lfnPfns == null || lfnPfns.isEmpty()){
            return result;
        }

        //put just the pfns in a list that needs
        //to be sent to the RLS API
        for (Iterator it = lfnPfns.iterator(); it.hasNext(); ) {
            pfns.add( ( (RLSString2Bulk) it.next()).s2);
        }
        //now query for the attributes in bulk
        List attributes = null;
        try {
            attributes = mLRC.attributeValueGetBulk(pfns, attrKey,
                RLSAttribute.LRC_PFN);
        }
        catch (RLSException e) {
            //some other error, but we can live with it.
            //just flag as warning
            mLogMsg = "getAttributes(List,String,Object)";
            log(mLogMsg, e,LogManager.ERROR_MESSAGE_LEVEL);
            return result;
        }

        //we need to sort them on the basis of the pfns
        //which is the populate the key field
        Collections.sort(attributes, new RLSAttributeComparator());
        /*
        System.out.println("Sorted attributes are ");
        for(Iterator it = attributes.iterator(); it.hasNext();){
            RLSAttributeObject obj = (RLSAttributeObject) it.next();
            if(obj.rc == RLSClient.RLS_ATTR_NEXIST){
                System.out.print("\tAttribute does not exist");
            }
            System.out.println("\t" + obj.key + "->rc" + obj.rc);
        }
        */

        for (Iterator it = attributes.iterator(); it.hasNext(); ) {
            RLSAttributeObject attr = (RLSAttributeObject) it.next();
            Object value = (attr.rc == RLSClient.RLS_ATTR_NEXIST)?
                            null: //assign an empty value
                            getAttributeValue(attr.attr);//retrieve the value

            curr = attr.key;

            //push in the attribute into the temp map only
            //if prev is null or the prev and current pfn's match
            if((prev == null || curr.equalsIgnoreCase(prev))
               &&
               (value != null)//value being null means no attribute associated
               &&
               ((attrVal == null)
                 || (attrVal.equals(value)) )){
                temp.put(attr.attr.name,value);
            }
            else{
                //push it into the map all attributes for a single pfn
                //only if the map is not empty or there was no matching
                //being done attrVal (i.e it is null)
                if(attrVal == null || !temp.isEmpty()){
                    entry = new ReplicaCatalogEntry(prev, temp);
                    //System.out.println("0:Entry being made is " + entry);
                    //the entry has to be put in a map keyed by the pfn name
                    pfnMap.put(prev, entry);
                    temp = new HashMap();
                }
                //added June 15,2005
                if(value != null &&
                   ( attrVal == null || attrVal.equals(value))){
                    temp.put(attr.attr.name,value);
                }
            }
            //push in the last attribute entry
            if(!it.hasNext()){
                //push it into the map all attributes for a single pfn
                //only if the map is not empty or there was no matching
                //being done attrVal (i.e it is null)
                if(attrVal == null || !temp.isEmpty()){
                    entry = new ReplicaCatalogEntry(curr, temp);
                    //System.out.println("1:Entry being made is " + entry);
                    //the entry has to be put in a map keyed by the pfn name
                    pfnMap.put(curr, entry);
                }
            }
            prev = curr;

        }

        //the final iteration that groups the pfn and their
        //attributes according to the lfn
        prev = null;
        for (Iterator it = lfnPfns.iterator(); it.hasNext(); ) {
            RLSString2Bulk lfnPfn = (RLSString2Bulk) it.next();
            curr = lfnPfn.s1;

            entry = (ReplicaCatalogEntry) pfnMap.get(lfnPfn.s2);
            if(entry == null){
                //means no match on attribute or attribute value was found
                continue;
            }

            if (!curr.equalsIgnoreCase(prev) && (prev != null)) {
                //push it into the map
                //we have entry for one lfn and all pfns constructed
                //System.out.println("Putting in entry for " + prev + " " + l);
                result.put(prev, l);
                l = new ArrayList();
            }
            
            //set a site handle if not already set
            setResourceHandle( entry );
            
            l.add(entry);
            //if this was the last one push it in result
            if(!it.hasNext()){
                //System.out.println("Putting in entry for " + curr + " " + l);
                result.put(curr, l);
            }
            prev = curr;
        }

        return result;
    }

    /**
     * Retrieves from the lrc, all the attributes associated with the <code>pfn
     * </code> in a map. All the attribute values are stored as String.
     *
     * @param lrc  the handle to the lrc , where the attributes are stored.
     * @param pfn  the pfn with which the attribute is associated.
     * @return <code>Map</code>containing the attribute keys and values,
     *         else an empty Map.
     */
    private Map getAttributes(RLSClient.LRC lrc, String pfn) {
        String val = null;
        List attrList = null;
        Map m = new HashMap();
        RLSAttribute att = null;

        try {
            //passing null denotes to get
            //hold of all attributes
            attrList = lrc.attributeValueGet(pfn, null,
                                             RLSAttribute.LRC_PFN);
        }
        catch (RLSException e) {
            //attribute does not exist error means no attributes
            //associated, return empty map else just denote a warning
            if(e.GetRC() != RLSClient.RLS_ATTR_NEXIST){
                //some other error, but we can live with it.
                //just flag as warning
                mLogMsg = "getAttributes(RLSClient.LRC,String)";
                log(mLogMsg, e,LogManager.ERROR_MESSAGE_LEVEL);
            }
            
            //associate a default value if required.
            setResourceHandle( m );
            
            return m;
        }

        //iterate throught the list and push all
        //the attributes in the map
        for (Iterator it = attrList.iterator(); it.hasNext(); ) {
            att = (RLSAttribute) it.next();
            //the list can contain a null attribute key
            //we dont want that.
            if( att.name != null ){
                m.put(att.name, att.GetStrVal());
            }
        }

        //populate default site handle if
        //site attribute is not specified
        setResourceHandle( m );
        
        return m;
    }

    /**
     * Retrieves from the lrc associated with this instance all, the attribute
     * value associated with the <code>pfn</code> for a given attribute name.
     *
     * @param pfn  the pfn with which the attribute is associated.
     * @param name the name of the attribute for which we want to search.
     *
     * @return value of the attribute if found
     *         else null
     */
    private String getAttribute(String pfn, String name) {
        return getAttribute(mLRC, pfn, name);
    }

    /**
     * Retrieves from the lrc, the attribute value associated with the <code>pfn
     * </code> for a given attribute name.
     *
     * @param lrc  the handle to the lrc , where the attributes are stored.
     * @param pfn  the pfn with which the attribute is associated.
     * @param name the name of the attribute for which we want to search.
     *
     * @return value of the attribute if found
     *         else null
     */
    private String getAttribute(RLSClient.LRC lrc, String pfn, String name) {
        String val = null;
        List attrList = null;

        try {
            attrList = lrc.attributeValueGet(pfn, name,
                                             RLSAttribute.LRC_PFN);
        }
        catch (RLSException e) {
            if (e.GetRC() == RLSClient.RLS_ATTR_NEXIST) {
                //attribute does not exist we return null
            }
            else {
                //some other error, but we can live with it.
                //just flag as warning
                mLogMsg = "getAttribute(String,String,String):";
                log(mLogMsg, e,LogManager.ERROR_MESSAGE_LEVEL);
            }
            return null;
        }

        return (attrList.isEmpty()) ?
            null :
            //we return the first attribute value
            //Does not make much sense for
            //more than one attribute value
            //for the same key and pfn
            attrList.get(0).toString();
    }


    /**
     * Retrieves the attribute value as an object from the <code>RLSAttribute</code>
     * object. Does automatic boxing (i.e converts int to Integer) etc.
     * The value is returned of the type as determined from the internal value
     * type.
     *
     * @param attr the <code>RLSAttribute</code> from which to extract the value.
     *
     * @return Object containing the value.
     *
     * @throws ReplicaCatalogException if illegal value associated.
     */
    private Object getAttributeValue(RLSAttribute attr){
        Object obj = null;
        int type = attr.GetValType();

        switch(type){
            case RLSAttribute.STR:
               obj = attr.GetStrVal();
               break;

            case RLSAttribute.DATE:
                obj = attr.GetDateVal();
                break;

            case RLSAttribute.DOUBLE:
                obj = new Double(attr.GetDoubleVal());
                break;

            case RLSAttribute.INT:
                obj = new Integer(attr.GetIntVal());
                break;

            default:
                throw new ReplicaCatalogException("Invalid value type associated " + type);
        }

        return obj;
    }

    /**
     * Sets the number of lfns in each batch while querying the lrc in the
     * bulk mode.
     *
     * @param properties  the properties passed while connecting.
     *
     */
    private void setBatchSize(Properties properties) {
        String s = properties.getProperty(this.BATCH_KEY);
        int size = this.RLS_BULK_QUERY_SIZE;
        try{
            size = Integer.parseInt(s);
        }
        catch(Exception e){}
        mBatchSize = size;
    }


    /**
     * Returns the number of lfns in each batch while querying the lrc in the
     * bulk mode.
     *
     * @return the batch size.
     */
    private int getBatchSize() {
        return mBatchSize;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete LRC. It uses the bulk query api to the LRC to query for stuff.
     * Bulk query has been in RLS since version 2.0.8. All the lfns in set
     * are put in one single bulk query to the LRC. There is a risk of seeing
     * a timeout error in case of large set of lfns. User should use the
     * lookup function that internally does the bulk query in batches.
     * Passing a null value for the attribute key results in the querying for all
     * attributes. The function returns <code>ReplicaCatalogEntry</code> objects
     * that have the attribute identified by attribute key passed.
     *
     * @param lfns     set of logical filename strings to look up.
     * @param attrKey  the name of attribute that needs to be queried for each
     *                 pfn. a value of null denotes all attributes.
     *
     * @return a map indexed by the LFN. Each value is a collection
     * of replica catalog entries for the LFN.
     * @see ReplicaCatalogEntry
     * @see #lookup(Set)
     */
    private Map bulkLookup(Set lfns, String attrKey) {
        return bulkLookup(lfns,attrKey,null);
    }


    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete LRC. It uses the bulk query api to the LRC to query for stuff.
     * Bulk query has been in RLS since version 2.0.8. All the lfns in set
     * are put in one single bulk query to the LRC. There is a risk of seeing
     * a timeout error in case of large set of lfns. User should use the
     * lookup function that internally does the bulk query in batches.
     * Passing a null value for the attribute key results in the querying for all
     * attributes. A null value for the attribute value, disables attribute matching
     * and results in the <code>ReplicaCatalogEntry</code> objects that have
     * the attribute identified by attribute key passed.
     *
     * @param lfns     set of logical filename strings to look up.
     * @param attrKey  the name of attribute that needs to be queried for each
     *                 pfn. a value of null denotes all attributes.
     * @param attrVal  the value of the attribute that should be matching.
     *
     * @return a map indexed by the LFN. Each value is a collection
     * of replica catalog entries for the LFN.
     * @see ReplicaCatalogEntry
     * @see #lookup(Set)
     */
    private Map bulkLookup(Set lfns, String attrKey, Object attrVal) {
        List list = null;
        List lfnsFound = null;
        RLSString2Bulk curr = null;
        int size = mBatchSize;
        Map result = new HashMap(lfns.size());

        try {
            list = mLRC.getPFNBulk( new ArrayList(lfns));
            //we need to group pfns by lfn
            Collections.sort(list, new RLSString2BulkComparator());
            /*
            System.out.println("Sorted list is ");
            for(Iterator it = list.iterator(); it.hasNext();){
                RLSString2Bulk s2b = (RLSString2Bulk) it.next();
                System.out.println("\t" + s2b.s1 + "->" + s2b.s2);
            }
            */
            size = list.size() <= size ? list.size() :size;
            for (Iterator it = list.iterator(); it.hasNext(); ) {
                //the pfn themseleves need to be queried
                //in batches to avoid timeout errors but the batch size
                //should have all the pfns for a lfn!!
                List l = new ArrayList(size);
                String prev = "";
                if (curr != null) {
                    //this is the case where the current
                    //item is not in any of the sublists
                    l.add(curr);
                }
                for (int j = 0; (it.hasNext()); ) {
                    RLSString2Bulk s2b = (RLSString2Bulk) it.next();
                    //s1 is the lfn
                    //s2 denotes the pfn
                    //rc is the exit status returned by the RLI
                    if (s2b.rc == RLSClient.RLS_SUCCESS) {
                        curr = s2b;
                        if (s2b.s2 != null) {
                            //query for the pool attribute
                            //for that pfn to the lrc
                            //if none is found or you do not
                            //query for the attribute
                            //pool is set to UNDEFINED_POOL
                            if (!curr.s1.equalsIgnoreCase(prev)) {
                                //do nothing
                                //check if j > size
                                if (j >= size) {
                                    //break out of the loop.
                                    //current needs to go into the next list
                                    break;
                                }
                            }

                            l.add(s2b);
                            j++;
                        }
                        else {
                            mLogMsg =
                                "bulkLookup(Set): Unexpected Mapping with no pfn for lfn: " +
                                s2b.s1;
                            log(mLogMsg,LogManager.ERROR_MESSAGE_LEVEL);
                        }
                        prev = curr.s1;
                    }
                    else if (s2b.rc != RLSClient.RLS_LFN_NEXIST) {
                        mLogMsg = "bulkLookup(Set): " +
                            mRLS.getErrorMessage(s2b.rc);
                        log(mLogMsg,LogManager.ERROR_MESSAGE_LEVEL);
                    }
                    //prev = curr.s1;
                }
                //get hold of all attributes for the pfn's
                result.putAll(getAttributes(l, attrKey,attrVal));

            }

        }
        catch (Exception e) {
            log("bulkLookup(Set)", e,LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }

        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete LRC. It uses the bulk query api to the LRC to query for stuff.
     * Bulk query has been in RLS since version 2.0.8. All the lfns in set
     * are put in one single bulk query to the LRC. There is a risk of seeing
     * a timeout error in case of large set of lfns. User should use the
     * lookup function that internally does the bulk query in batches.
     *
     * @param lfns is a set of logical filename strings to look up.
     *
     * @return a map indexed by the LFN. Each value is a set
     * of PFN strings.
     *
     * @see #lookupNoAttributes(Set)
     */
    private Map bulkLookupNoAttributes(Set lfns) {
        List list = null;
        List lfnsFound = null;
        Map result = new HashMap(lfns.size());
        String prev = null;
        String curr = null;
        Set s = new HashSet();

        try {
            list = mLRC.getPFNBulk( new ArrayList(lfns));
            //we need to group pfns by lfn
            Collections.sort(list, new RLSString2BulkComparator());

            for (Iterator it = list.iterator(); it.hasNext(); ) {
                RLSString2Bulk s2b = (RLSString2Bulk) it.next();

                //s1 is the lfn
                //s2 denotes the pfn
                //rc is the exit status returned by the RLI
                if (s2b.rc == RLSClient.RLS_SUCCESS) {
                    curr = s2b.s1;
                    if (s2b.s2 != null) {
                        if (!curr.equalsIgnoreCase(prev) && (prev != null)) {
                            //push it into the map
                            //we have entry for one lfn and all pfns constructed
                            result.put(prev, s);
                            s = new HashSet();
                        }
                        s.add(s2b.s2);
                        //if this was the last one push it in result
                        if(!it.hasNext()){
                            result.put(curr,s);
                        }
                    }
                    else {
                        mLogMsg =
                            "bulkLookupNoAttributes(Set): Unexpected Mapping with no pfn for lfn: " +
                            s2b.s1;
                        log(mLogMsg,LogManager.ERROR_MESSAGE_LEVEL);
                    }
                    prev = curr;
                }
                else if (s2b.rc != RLSClient.RLS_LFN_NEXIST) {
                    mLogMsg = "bulkLookupNoAttributes(Set): " +
                        mRLS.getErrorMessage(s2b.rc);
                    log(mLogMsg,LogManager.ERROR_MESSAGE_LEVEL);
                }
                //prev = curr;
            }
        }
        catch (Exception e) {
            log("bulkLookupNoAttributes(Set):", e,
                LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }

        return result;
    }

    /**
     * Constructs replica catalog exception out the RLSException that is
     * thrown by the API underneath.
     *
     * @param prefix   the prefix that is to be applied to the message
     *                 passed while creating the exception.
     * @param e        the RLSException that is caught underneath.
     *
     * @return a ReplicaCatalogException
     */
    private ReplicaCatalogException exception(String prefix,RLSException e){
        StringBuffer message = new StringBuffer(32);
        message.append("{LRC ").append(mLRCURL).append("} ")
               .append(prefix).append(": ").append(e.getMessage());
        return new ReplicaCatalogException(message.toString(),e);
    }


    /**
     * Constructs an exception from the <code>RLSString2Bulk</code> object.
     *
     * @return a ReplicaCatalogException
     */
    private ReplicaCatalogException exception(RLSString2Bulk rs){
        StringBuffer sb = new StringBuffer(32);
        sb.append("Error (lfn,pfn,ec)").append(" (")
          .append(rs.s1).append(',')
          .append(rs.s2).append(',')
          .append(rs.rc).append(',').append(mRLS.getErrorMessage(rs.rc))
          .append(')');
        return new ReplicaCatalogException(sb.toString());

    }

    /**
     * Returns a subset of a collection of <code>ReplicaCatalogEntry</code>
     * objects that have attributes matchin to the attribute identified by
     * the parameters passed.
     *
     * @param collection  the collection of <code>ReplicaCatalogEntry</code>
     *                    objects.
     * @param name        the attribute name to match.
     * @param value       the attribute value.
     *
     * @return Set of matching <code>ReplicaCatalogEntry</code> objects.
     */
    private Set subset(Collection collection, String name,
                       Object value) {
        return subset(collection,name,value,false);
    }


    /**
     * Returns a subset of a collection of <code>ReplicaCatalogEntry</code>
     * objects that have attributes matchin to the attribute identified by
     * the parameters passed.
     *
     * @param collection  the collection of <code>ReplicaCatalogEntry</code>
     *                    objects.
     * @param name        the attribute name to match.
     * @param value       the attribute value.
     * @param onlyPFN     boolean to denote if we only want the PFN's
     *
     * @return Set of <code>ReplicaCatalogEntry</code> objects if onlyPfn
     *         parameter is set to false, else a Set of pfns.
     */
    private Set subset(Collection collection, String name,
                                Object value, boolean onlyPFN) {
        Set s = new HashSet();
        ReplicaCatalogEntry rce;
        Object attrVal;
        for (Iterator it = collection.iterator(); it.hasNext(); ) {
            rce = (ReplicaCatalogEntry) it.next();
            //System.out.println("RCE is " + rce);
            attrVal = rce.getAttribute(name);
            if ( attrVal != null &&  attrVal.equals(value)) {
                //adding to the set only if
                //the attribute existed in the rce
                s.add(onlyPFN?
                      (Object)rce.getPFN():
                      rce);
            }
        }

        return s;
    }

    /**
     * A helper method that converts RLSString2 to MyRLSString2Bulk object.
     *
     * @param obj  the <code>RLSString2</code> to convert.
     *
     * @return the converted <code>MyRLSString2</code> object.
     */
    private RLSString2Bulk convert(RLSString2 obj){
        return new MyRLSString2Bulk(0,obj.s1,obj.s2);
    }

    /**
     * Logs to the logger object.
     *
     * @param message the message to be logged.
     * @param level   the logger level at which the message is to be logged.
     */
    private void log(String message,int level){
        message = "{LRC " + mLRCURL + "} " + message;
        mLogger.log(message,level);
    }

    /**
     * Logs to the logger object.
     *
     * @param message the message to be logged.
     * @param e       the exception that occured.
     * @param level   the logger level at which the message is to be logged.
     */
    private void log(String message,Exception e,int level){
        message = "{LRC " + mLRCURL + "} " +  message;
        mLogger.log(message,e,level);
    }



    /**
     * The comparator that is used to group the <code>RLSString2</code> objects by the
     * value in the s1 field.  This comparator should only  be used for grouping
     * purposes not in Sets or Maps etc.
     */
    private class RLSString2Comparator implements Comparator {

        /**
         * Compares this object with the specified object for order. Returns a
         * negative integer, zero, or a positive integer if the first argument is
         * less than, equal to, or greater than the specified object. The
         * RLSString2 are compared by their s1 field.
         *
         * @param o1 is the first object to be compared.
         * @param o2 is the second object to be compared.
         *
         * @return a negative number, zero, or a positive number, if the
         * object compared against is less than, equals or greater than
         * this object.
         * @exception ClassCastException if the specified object's type
         * prevents it from being compared to this Object.
         */
        public int compare(Object o1, Object o2) {
            if (o1 instanceof RLSString2 && o2 instanceof RLSString2) {
                return ( (RLSString2) o1).s1.compareTo( ( (RLSString2)
                    o2).s1);
            }
            else {
                throw new ClassCastException("object is not RLSString2");
            }
        }

    }

    /**
     * The comparator that is used to group the RLSString2Bulk objects by the
     * value in the s1 field.  This comparator should only  be used for grouping
     * purposes not in Sets or Maps etc.
     */
    private class RLSString2BulkComparator implements Comparator {

        /**
         * Compares this object with the specified object for order. Returns a
         * negative integer, zero, or a positive integer if the first argument is
         * less than, equal to, or greater than the specified object. The
         * RLSString2Bulk are compared by their s1 field.
         *
         * @param o1 is the first object to be compared.
         * @param o2 is the second object to be compared.
         *
         * @return a negative number, zero, or a positive number, if the
         * object compared against is less than, equals or greater than
         * this object.
         * @exception ClassCastException if the specified object's type
         * prevents it from being compared to this Object.
         */
        public int compare(Object o1, Object o2) {
            if (o1 instanceof RLSString2Bulk && o2 instanceof RLSString2Bulk) {
                return ( (RLSString2Bulk) o1).s1.compareTo( ( (RLSString2Bulk)
                    o2).s1);
            }
            else {
                throw new ClassCastException("object is not RLSString2Bulk");
            }
        }

    }

    /**
     * The comparator that is used to group the RLSAttributeObject objects by the
     * value in the key field.  This comparator should only  be used for grouping
     * purposes not in Sets or Maps etc.
     */
    private class RLSAttributeComparator implements Comparator {

        /**
         * Compares this object with the specified object for order. Returns a
         * negative integer, zero, or a positive integer if the first argument is
         * less than, equal to, or greater than the specified object. The
         * RLSAttributeObject are compared by their s1 field.
         *
         * @param o1 is the first object to be compared.
         * @param o2 is the second object to be compared.
         *
         * @return a negative number, zero, or a positive number, if the
         * object compared against is less than, equals or greater than
         * this object.
         * @exception ClassCastException if the specified object's type
         * prevents it from being compared to this Object.
         */
        public int compare(Object o1, Object o2) {
            if (o1 instanceof RLSAttributeObject && o2 instanceof RLSAttributeObject) {
                return ( (RLSAttributeObject) o1).key.compareTo( ( (
                    RLSAttributeObject) o2).key);
            }
            else {
                throw new ClassCastException("object is not RLSAttributeObject");
            }
        }

    }

    /**
     * The class that extends RLSString2Bulk and adds on the equals method,
     * that allows me to do the set operations
     */
    private class MyRLSString2Bulk extends RLSString2Bulk{


        /**
         * The overloaded constructor.
         *
         * @param rca  the rls exitcode
         * @param s1a  the String object usually containing the lfn
         */
        public MyRLSString2Bulk(int rca, java.lang.String s1a){
            super(rca,s1a);
        }


        /**
         * The overloaded constructor.
         *
         * @param rca  the rls exitcode.
         * @param s1a  the String object usually containing the lfn.
         * @param s2a  the String object usually containing the pfn.
         */
        public MyRLSString2Bulk(int rca, java.lang.String s1a, java.lang.String s2a){
            super(rca,s1a,s2a);
        }

        /**
         * Indicates whether some other object is "equal to" this one.
         *
         * An object is considered equal if it is of the same type and
         * all the fields s1 and s2 match.
         *
         * @return boolean whether the object is equal or not.
         */
        public boolean equals(Object obj){
            if(obj instanceof MyRLSString2Bulk){
                MyRLSString2Bulk sec = (MyRLSString2Bulk)obj;
                return this.s1.equals(sec.s1) && this.s2.equals(sec.s2);
            }
            return false;
        }

        /**
         * Returns a string representation of the object.
         *
         * @return the String representation.
         */
        public String toString(){
            StringBuffer sb = new StringBuffer(10);
            sb.append("(").append(s1).append("->").append(s2).
            append(",").append(rc).append(")");
            return sb.toString();
        }
    }

    /**
     * Testing function.
     */
    public static void main(String[] args){
        LRC lrc = new LRC();
        lrc.connect("rls://sukhna.isi.edu");
        String lfn = "test";
        LogManagerFactory.loadSingletonInstance().setLevel(LogManager.DEBUG_MESSAGE_LEVEL);

        /*
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://sukhna.isi.edu/tmp/test");
        rce.addAttribute("name","karan");
        lrc.insert("test",rce);
        lrc.insert("test","gsiftp://sukhna.isi.edu/tmp/test1","isi");
        lrc.insert("test","gsiftp://sukhna.isi.edu/constraint/testvahi","isi");
        lrc.insert("vahi.f.a","file:///tmp/vahi.f.a","isi");
        lrc.insert("testvahi.f.a","file:///tmp/testvahi.f.a","isi");

        rce = new ReplicaCatalogEntry("gsiftp://sukhna.isi.edu/tmp/testX");
        rce.addAttribute("name","karan");
        rce.addAttribute("pool","isi");
        lrc.insert("testX",rce);
        */

       /*
        System.out.println("Getting list of lfns");
        System.out.println("\t" + lrc.list());



        Set s = new HashSet();
        s.add("test");s.add("vahi.f.a");s.add("testX");
        s.add("unknown");


        System.out.println("\nQuerying for complete RCE for site  " + s );
        System.out.println(lrc.lookup(s));
        */

        /*
        System.out.println("\n Deleting " + lfn);
        System.out.println(lrc.deleteByResource(lfn,"isi"));


        System.out.println("\nQuerying for PFN for site" + s );
        System.out.println(lrc.lookupNoAttributes(s,"isi"));

        System.out.println("\nRemoving lfns " + s);
        //System.out.println(lrc.remove(s));
        System.out.println(lrc.removeByAttribute("isi"));


       System.out.println("\n\nClearing catalog " + lrc.clear());
       */

       //System.out.println("Getting list of lfns");
       //System.out.println("\t" + lrc.listLFNPFN("*vahi*",false));

       /*
       System.out.println("Removing lfns in set " + s + " ");
       System.out.println(lrc.removeByAttribute("isi"));

       Map m = new HashMap();
       //m.put("pfn","*vahi*");
       m.put("lfn","test*");
       System.out.println("Getting lfns matching constraint");
       System.out.println("\t" + lrc.lookup(m));
       */


       //test bulk insert
       System.out.println("Clearing the database");
       //lrc.clear();
       Map inserts = new HashMap();
       Collection c1 = new ArrayList();
       c1.add(new ReplicaCatalogEntry("gsiftp://test/f.a","isi"));
       Collection c2 = new ArrayList();
       c2.add(new ReplicaCatalogEntry("gsiftp://test/f.b","isi"));
       Collection c3 = new ArrayList();
       c3.add(new ReplicaCatalogEntry("gsiftp://test/f.c","isi1"));
       inserts.put("f.a",c1);
       inserts.put("f.b",c2);
       inserts.put("f.c",c3);
       System.out.println("Doing bulk inserts");
       try{
           System.out.println("Inserted " + lrc.insert(inserts) + " entries");
       }
       catch(ReplicaCatalogException rce){
           do {
               System.out.println(rce.getMessage());
               rce = (ReplicaCatalogException) rce.getNextException();
           } while ( rce != null );

       }

       lrc.close();
    }


} //end of  class LRC
