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

import org.griphyn.common.util.Version;
import org.globus.replica.rls.RLSClient;
import org.globus.replica.rls.RLSException;
import org.globus.replica.rls.RLSAttribute;
import org.globus.replica.rls.RLSAttributeObject;
import org.globus.replica.rls.RLSLRCInfo;
import org.globus.replica.rls.RLSString2Bulk;
import org.globus.replica.rls.RLSString2;

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
import java.util.LinkedHashSet;
import java.util.Iterator;

/**
 * This class implements the VDS replica catalog interface on top of RLI API.
 * A thing to take care of is that all delete and remove operations are
 * propoagated to all the Local Replica Catalogs (LRCs) reporting to the RLI.
 * Hence,
 * you should be careful while deleting LFNs, as deletions can cascade to
 * multiple LRCs. If you want to delete or remove an LFN from a particular LRC,
 * use the LRC implementation to connect to that LRC and call the corresponding
 * delete functions on that.
 * There is no transaction support in the implementation. The implementation
 * is best effort. Inconsistencies can occur if one of the LRCs goes offline,
 * or an operation fails for whatsoever reason.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 */
public class RLI implements ReplicaCatalog {

    /**
     * The number of entries searched in each bulk query to RLS.
     */
    public static final int RLS_BULK_QUERY_SIZE = 1000;

    /**
     * The default timeout in seconds to be used while querying the RLI.
     */
    public static final String DEFAULT_RLI_TIMEOUT = "30";

    /**
     * The key that is used to get hold of the timeout value from the properties
     * object.
     */
    public static final String RLS_TIMEOUT_KEY = "rls.timeout";

    /**
     * The key that is used to get hold of the timeout value from the properties
     * object.
     */
    public static final String RLI_TIMEOUT_KEY = "rli.timeout";

    /**
     * The key that is used to designate the LRC whose results are to be
     * ignored.
     */
    public static final String LRC_IGNORE_KEY = "lrc.ignore";

    /**
     * The key that is used to designate the LRC whose results are to be
     * restricted.
     */
    public static final String LRC_RESTRICT_KEY = "lrc.restrict";

    /**
     * The attribute in RLS that maps to a site handle.
     */
    public static final String SITE_ATTRIBUTE = "pool";

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
     * The error message for not connected to RLI.
     */
    public static final String RLI_NOT_CONNECTED_MSG = "Not connected to RLI ";

    /**
     * The error message for not connected to LRC.
     */
    public static final String LRC_NOT_CONNECTED_MSG = "Unable to connect to LRC ";


    /**
     * The LRC query state indicating that LRC needs to queried fully. The LRC
     * returns all PFNs irrespective of whether they have a site attribute or
     * not.
     */
    public static final int LRC_QUERY_NORMAL = 0;

    /**
     * The LRC query state indicating that LRC has to be restricted query.
     * LRC should return only PFNs with site attributes tagged.
     */
    public static final int LRC_QUERY_RESTRICT = 1;

    /**
     * The LRC query state indicating that LRC has to be ignored.
     */
    public static final int LRC_QUERY_IGNORE = 2;


    /**
     * The handle to the client that allows access to both the RLI and the LRC
     * running at the url specified while connecting.
     */
    private RLSClient mRLS;

    /**
     * The handle to the client that allows access to the LRC running at the
     * url specified while connecting.
     */
    private RLSClient.RLI mRLI;

    /**
     * The url to the RLI to which this instance implementation talks to.
     */
    private String mRLIURL;

    /**
     * A String array contains the LRC URLs that have to be ignored for querying.
     */
    private String[] mLRCIgnoreList;

    /**
     * A String array contains the LRC URLs that have to be restricted for querying.
     * Only those entries are returned that have a site attribute associated
     * with them.
     */
    private String[] mLRCRestrictList;


    /**
     * The handle to the logging object. Should be log4j soon.
     */
    private LogManager mLogger;

    /**
     * The string holding the message that is logged in the logger.
     */
    private String mLogMsg;


    /**
     * The properties object containing all the properties, that are required
     * to connect to a RLS.
     */
    private Properties mConnectProps;

    /**
     * The batch size while querying the RLI in the bulk mode.
     */
    private int mBatchSize;

    /**
     * The timeout in seconds to be applied while querying the RLI.
     */
    private int mTimeout;
    
    /**
     * The default constructor, that creates an object which is not linked with
     * any RLS. Use the connect method to connect to the RLS.
     *
     * @see #connect(Properties).
     */
    public RLI() {
        mRLS = null;
        mLogger =  LogManagerFactory.loadSingletonInstance();
        mConnectProps = new Properties();
        mBatchSize = this.RLS_BULK_QUERY_SIZE;
        mTimeout   = Integer.parseInt(DEFAULT_RLI_TIMEOUT);

    }

   /**
    * Establishes a connection to the RLI, picking up the proxy from the default
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
     * Establishes a connection to the RLI.
     *
     * @param props contains all necessary data to establish the link.
     *
     * @return true if connected now, or false to indicate a failure.
     */
    public boolean connect(Properties props) {
        boolean con = false;
        Object obj = props.remove(URL_KEY);
        mRLIURL = (obj == null) ? null : (String) obj;

        if (mRLIURL == null) {
            //nothing to connect to.
            mLogger.log("The RLI url is not specified",
                        LogManager.ERROR_MESSAGE_LEVEL);
            return con;
        }

        //try to see if a proxy cert has been specified or not
        String proxy = props.getProperty(PROXY_KEY);
        mConnectProps = props;//??
        
        mLogger.log( "[RLI-RC] Connection properties passed are " + props,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        mLRCIgnoreList   = this.getRLSLRCIgnoreURLs( props );
        mLRCRestrictList = this.getRLSLRCRestrictURLs( props );


        //determine timeout
        mTimeout = getTimeout(props);

        //set the batch size for queries
        setBatchSize(props);

        return connect(mRLIURL, proxy);
    }

    /**
     * Establishes a connection to the RLI.
     *
     * @param url    the url to lrc to connect to.
     * @param proxy  the path to the proxy file to be picked up. null denotes
     *               default location.
     *
     * @return true if connected now, or false to indicate a failure.
     */
    public boolean connect(String url, String proxy) {
        mRLIURL = url;
        //push it into the internal properties object
        mConnectProps.setProperty(URL_KEY,url);
        if(proxy != null){
            mConnectProps.setProperty(PROXY_KEY, proxy);
        }
        try {
            mRLS = (proxy == null) ?
                new RLSClient(url) : //proxy is picked up from default loc /tmp
                new RLSClient(url, proxy);

            //set RLI timeout
            mRLS.SetTimeout(mTimeout);

            //connect is only successful if we have
            //successfully connected to the LRC
            mRLI = mRLS.getRLI();

        }
        catch (RLSException e) {
            mLogger.log("RLS Exception", e,LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }
        return true;
    }


    /**
     * Gets a handle to the RLI that is associated with the RLS running at
     * url.
     *
     * @return <code>RLSClient.RLI</code> that points to the RLI that is
     *         running , or null in case connect method not being called.
     * @see #mRLIURL
     */
    public RLSClient.RLI getRLI() {
        return (this.isClosed()) ? null: mRLS.getRLI() ;
    }

    /**
     * Gets a handle to the LRC that is associated with the RLS running at
     * url.
     *
     * @return <code>RLSClient.LRC</code> that points to the RLI that is
     *         running , or null in case connect method not being called.
     * @see #mRLIURL
     */
    public RLSClient.LRC getLRC() {
        return (this.isClosed()) ? null : mRLS.getLRC();
    }

    /**
     * Retrieves the entry for a given filename and resource handle from
     * the RLS.
     *
     * @param lfn is the logical filename to obtain information for.
     * @param handle is the resource handle to obtain entries for.
     *
     * @return the (first) matching physical filename, or
     * <code>null</code> if no match was found.
     */
    public String lookup(String lfn, String handle) {
        //sanity check
        if (this.isClosed()) {
            //probably an exception should be thrown here!!
            throw new RuntimeException(RLI_NOT_CONNECTED_MSG + this.mRLIURL);
        }
        String pfn = null;
        ArrayList lrcList = null;
        try {
            lrcList = mRLI.getLRC(lfn);
            for (Iterator it = lrcList.iterator(); it.hasNext(); ) {
                //connect to an lrc
                String lrcURL = ( (RLSString2) it.next()).s2;
                //push the lrcURL to the properties object
                mConnectProps.setProperty(this.URL_KEY,lrcURL);
                LRC lrc = new LRC();
                if(!lrc.connect(mConnectProps)){
                    //log an error/warning message
                    mLogger.log("Unable to connect to LRC " + lrcURL,
                                LogManager.ERROR_MESSAGE_LEVEL);
                    continue;
                }

                //query the lrc
                try{
                    pfn = lrc.lookup(lfn,handle);
                    if(pfn != null)
                        return pfn;
                }
                catch(Exception ex){
                    mLogger.log("lookup(String,String)",ex,
                                LogManager.ERROR_MESSAGE_LEVEL);
                }
                finally{
                    //disconnect
                    lrc.close();
                }
            }
        }
        catch (RLSException ex) {
            mLogger.log("lookup(String,String)",ex,
                        LogManager.ERROR_MESSAGE_LEVEL);
        }

        return null;
    }
    
    /**
     * Retrieves all entries for a given LFN from the replica catalog.
     * Each entry in the result set is a tuple of a PFN and all its
     * attributes.
     *
     * @param lfn is the logical filename to obtain information for.
     *
     * @return a collection of replica catalog entries,  or null in case of
     *         unable to connect to RLS.
     *
     * @see ReplicaCatalogEntry
     */
    public Collection lookup(String lfn) {
        Set lfns = new HashSet();
        lfns.add( lfn );
        
        Map<String, Collection<ReplicaCatalogEntry>> result = this.lookup( lfns );

        if( result == null ){
            return null;
        }
        else{
            Collection values = result.get( lfn );
            if( values == null ){
                //JIRA PM-74
                values = new ArrayList();
            }
            return values;
        }
        
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
        Set lfns = new HashSet();
        lfns.add( lfn );
        
        Map<String, Set<String>> result = this.lookupNoAttributes( lfns );

        if( result == null ){
            return null;
        }
        else{
            Set values = result.get( lfn );
            if( values == null ){
                //JIRA PM-74
                values = new HashSet();
            }
            return values;
        }
     
    }
  
    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete LRC. It uses the bulk query api to the LRC to query for stuff.
     * Bulk query has been in RLS since version 2.0.8. Internally, the bulk
     * queries are done is sizes specified by variable mBatchSize.
     *
     * @param lfns is a set of logical filename strings to look up.
     *
     * @return a map indexed by the LFN. Each value is a collection
     * of replica catalog entries for the LFN.
     *
     * @see ReplicaCatalogEntry
     * @see #getBatchSize()
     */
    public Map lookup(Set lfns) {
        //Map indexed by lrc url and each value a collection
        //of lfns that the RLI says are present in it.
        Map lrc2lfn  = this.getLRC2LFNS(lfns);

        if(lrc2lfn == null){
            //probably RLI is not connected!!
            return null;
        }

        // now query the LRCs with the LFNs that they are responsible for
        // and aggregate stuff.
        String key = null;
        Map result = new HashMap(lfns.size());
        String message;
        for(Iterator it = lrc2lfn.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry)it.next();
            key = (String)entry.getKey();
            message = "Querying LRC " + key;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,key);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + key,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }

            //query the lrc
            try{
                Map m = lrc.lookup((Set)entry.getValue());

                //figure out if we need to restrict our queries or not.
                //restrict means only include results if they have a site
                //handle associated
                boolean restrict = ( this.determineQueryType(key) == this.LRC_QUERY_RESTRICT );

                for(Iterator mit = m.entrySet().iterator();mit.hasNext();){
                    entry = (Map.Entry)mit.next();
                    List pfns = (( List )entry.getValue());
                    if ( restrict ){
                        //traverse through all the PFN's and check for resource handle
                        for ( Iterator pfnIterator = pfns.iterator(); pfnIterator.hasNext();  ){
                            ReplicaCatalogEntry pfn = (ReplicaCatalogEntry) pfnIterator.next();
                            if ( pfn.getResourceHandle() == null ){
                                //do not include in the results if the entry does not have
                                //a pool attribute associated with it.
                                mLogger.log("Ignoring entry " + entry.getValue() +
                                            " from  LRC " + key,
                                            LogManager.DEBUG_MESSAGE_LEVEL);
                                pfnIterator.remove();
                            }
                        }

                    }

                    //if pfns are empty which could be due to
                    //restriction case taking away all pfns
                    //do not merge in result
                    if( pfns.isEmpty() ){ continue; }

                    //merge the entries into the main result
                    key   = (String)entry.getKey(); //the lfn
                    if( result.containsKey(key) ){
                        //right now no merging of RCE being done on basis
                        //on them having same pfns. duplicate might occur.
                        ((List)result.get(key)).addAll( pfns );
                    }
                    else{
                        result.put( key, pfns );
                    }
                }
            }
            catch(Exception ex){
                mLogger.log("lookup(Set)",ex,LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                //disconnect
                lrc.close();
            }


            mLogger.log( message  + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
        }
        return result;
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog.
     * Each entry in the result set is just a PFN string. Duplicates
     * are reduced through the set paradigm.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a collection
     * of PFN's for the LFN.
     */
    public Map lookupNoAttributes(Set lfns) {
        //Map indexed by lrc url and each value a collection
        //of lfns that the RLI says are present in it.
        Map lrc2lfn  = this.getLRC2LFNS(lfns);
        if(lrc2lfn == null){
            //probably RLI is not connected!!
            return null;
        }

        // now query the LRCs with the LFNs that they are responsible for
        // and aggregate stuff.
        String key = null;
        String message;
        Map result = new HashMap(lfns.size());
        for(Iterator it = lrc2lfn.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry)it.next();
            key = (String)entry.getKey();
            message = "Querying LRC " + key;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,key);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + key,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }

            //query the lrc
            try{
                Map m = lrc.lookupNoAttributes((Set)entry.getValue());
                for(Iterator mit = m.entrySet().iterator();mit.hasNext();){
                    entry = (Map.Entry)mit.next();
                    //merge the entries into the main result
                    key   = (String)entry.getKey(); //the lfn
                    if(result.containsKey(key)){
                        //right now no merging of RCE being done on basis
                        //on them having same pfns. duplicate might occur.
                        ((Set)result.get(key)).addAll((Set)entry.getValue());
                    }
                    else{
                        result.put(key,entry.getValue());
                    }
                }
            }
            catch(Exception ex){
                mLogger.log("lookup(Set)",ex,
                            LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                //disconnect
                lrc.close();
            }


            mLogger.log(message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
        }
        return result;

    }

    /**
     * Retrieves multiple entries for a given logical filenames, up to the
     * complete catalog. Retrieving full catalogs should be harmful, but
     * may be helpful in online display or portal.<p>
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     *
     * @return a map indexed by the LFN. Each value is a collection
     * of replica catalog entries (all attributes).
     *
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Set lfns, String handle) {
        //Map indexed by lrc url and each value a collection
        //of lfns that the RLI says are present in it.
        Map lrc2lfn  = this.getLRC2LFNS(lfns);
        if(lrc2lfn == null){
            //probably RLI is not connected!!
            return null;
        }

        // now query the LRCs with the LFNs they are responsible for
        // and aggregate stuff.
        String key = null,message = null;
        Map result = new HashMap(lfns.size());
        for(Iterator it = lrc2lfn.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry)it.next();
            key = (String)entry.getKey();
            message = "Querying LRC " + key;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,key);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + key,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }

            //query the lrc
            try{
                Map m = lrc.lookup((Set)entry.getValue(),handle);
                for(Iterator mit = m.entrySet().iterator();mit.hasNext();){
                    entry = (Map.Entry)mit.next();
                    //merge the entries into the main result
                    key   = (String)entry.getKey(); //the lfn
                    if(result.containsKey(key)){
                        //right now no merging of RCE being done on basis
                        //on them having same pfns. duplicate might occur.
                        ((Set)result.get(key)).addAll((Set)entry.getValue());
                    }
                    else{
                        result.put(key,entry.getValue());
                    }
                }
            }
            catch(Exception ex){
                mLogger.log("lookup(Set,String)",ex,
                            LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                //disconnect
                lrc.close();
            }


            mLogger.log(message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
        }
        return result;


    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog. Retrieving full catalogs should be harmful, but
     * may be helpful in online display or portal.<p>
     *
     * The <code>noAttributes</code> flag is missing on purpose, because
     * due to the resource handle, attribute lookups are already required.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     *
     * @return a map indexed by the LFN. Each value is a set of
     * physical filenames.
     */
    public Map lookupNoAttributes( Set lfns, String handle ){
        //Map indexed by lrc url and each value a collection
        //of lfns that the RLI says are present in it.
        Map lrc2lfn  = this.getLRC2LFNS(lfns);
        if(lrc2lfn == null){
            //probably RLI is not connected!!
            return null;
        }

        // now query the LRCs with the LFNs that they are responsible for
        // and aggregate stuff.
        String key = null,message = null;
        Map result = new HashMap(lfns.size());
        for(Iterator it = lrc2lfn.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry)it.next();
            key = (String)entry.getKey();
            message = "Querying LRC " + key;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,key);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + key,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }

            //query the lrc
            try{
                Map m = lrc.lookupNoAttributes((Set)entry.getValue(),handle);
                for(Iterator mit = m.entrySet().iterator();mit.hasNext();){
                    entry = (Map.Entry)mit.next();
                    //merge the entries into the main result
                    key   = (String)entry.getKey(); //the lfn
                    if(result.containsKey(key)){
                        //right now no merging of RCE being done on basis
                        //on them having same pfns. duplicate might occur.
                        ((Set)result.get(key)).addAll((Set)entry.getValue());
                    }
                    else{
                        result.put(key,entry.getValue());
                    }
                }
            }
            catch(Exception ex){
                mLogger.log("lookup(Set,String):",ex,
                            LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                //disconnect
                lrc.close();
            }


            mLogger.log(message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
        }
        return result;

    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the
     * complete catalog. Retrieving full catalogs should be harmful, but
     * may be helpful in online display or portal.<p>
     *
     *
     * At present it DOES NOT SUPPORT ATTRIBUTE MATCHING.
     *
     * @param constraints is mapping of keys 'lfn', 'pfn' to a string that
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
    public Map lookup(Map constraints) {
        //sanity check
        if (this.isClosed()) {
            //probably an exception should be thrown here!!
            throw new RuntimeException(RLI_NOT_CONNECTED_MSG + this.mRLIURL);
        }
        Map result = new HashMap();
        String url = null,message = null;
        //we need to get hold of all the LRC
        //that report to the RLI and call the
        //list() method on each of them
        for(Iterator it = this.getReportingLRC().iterator();it.hasNext();){
            url = (String)it.next();
            message = "Querying LRC " + url;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,url);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + url,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }
            try{
                Map m = lrc.lookup(constraints);
                for(Iterator mit = m.entrySet().iterator();mit.hasNext();){
                    Map.Entry entry = (Map.Entry)mit.next();
                    //merge the entries into the main result
                    String key   = (String)entry.getKey(); //the lfn
                    if(result.containsKey(key)){
                        //right now no merging of RCE being done on basis
                        //on them having same pfns. duplicate might occur.
                        ((List)result.get(key)).addAll((List)entry.getValue());
                    }
                    else{
                        result.put(key,entry.getValue());
                    }
                }

            }
            catch(Exception e){
                mLogger.log("list(String)",e,LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                lrc.close();
            }
        }

        return result;

    }

    /**
     * Lists all logical filenames in the catalog.
     *
     * @return A set of all logical filenames known to the catalog.
     */
    public Set list() {
        //sanity check
        if (this.isClosed()) {
            //probably an exception should be thrown here!!
            throw new RuntimeException(RLI_NOT_CONNECTED_MSG + this.mRLIURL);
        }
        Set result = new HashSet();
        String url = null,message = null;
        //we need to get hold of all the LRC
        //that report to the RLI and call the
        //list() method on each of them
        for(Iterator it = this.getReportingLRC().iterator();it.hasNext();){
            url = (String)it.next();
            message = "Querying LRC " + url;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,url);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + url,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }
            try{
                result.addAll(lrc.list());
            }
            catch(Exception e){
                mLogger.log("list()",e,LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                lrc.close();
            }
            mLogger.log(message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
        }

        return result;
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
        //sanity check
        if (this.isClosed()) {
            //probably an exception should be thrown here!!
            throw new RuntimeException(RLI_NOT_CONNECTED_MSG + this.mRLIURL);
        }
        Set result = new HashSet();
        String url = null,message = null;
        //we need to get hold of all the LRC
        //that report to the RLI and call the
        //list() method on each of them
        for(Iterator it = this.getReportingLRC().iterator();it.hasNext();){
            url = (String)it.next();
            message = "Querying LRC " + url;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,url);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + url,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }
            try{
                result.addAll(lrc.list(constraint));
            }
            catch(Exception e){
                mLogger.log("list(String)",e,LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                lrc.close();
            }
            mLogger.log(message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
        }

        return result;
    }

    /**
     * Inserts a new mapping into the LRC running at the URL, where the RLI
     * is running.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param tuple is the physical filename and associated PFN attributes.
     *
     * @return number of insertions, should always be 1. On failure,
     * throws a RuntimeException.
     */
    public int insert(String lfn, ReplicaCatalogEntry tuple) {
        //get hold of the LRC if that is running
        LRC lrc     = new LRC();
        int result  = 1;
        if(!lrc.connect(mConnectProps)){
            //log an error/warning message
            throw new RuntimeException(LRC_NOT_CONNECTED_MSG +
                                    mConnectProps.getProperty(URL_KEY));
        }
        result = lrc.insert(lfn,tuple);
        //better to keep a handle to the running LRC
        //as a member variable, and close it in
        //RLI.close()
        lrc.close();
        return result;

    }

    /**
     * Inserts a new mapping into the LRC running at the URL, where the RLI
     * is running.
     * This is a convenience function exposing the resource handle. Internally,
     * the <code>ReplicaCatalogEntry</code> element will be contructed, and passed to
     * the appropriate insert function.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param pfn is the physical filename associated with it.
     * @param handle is a resource handle where the PFN resides.
     *
     * @return number of insertions, should always be 1. On failure,
     * throws a RuntimeException.
     *
     * @see #insert( String, ReplicaCatalogEntry )
     * @see ReplicaCatalogEntry
     */
    public int insert(String lfn, String pfn, String handle) {
        //get hold of the LRC if that is running
        LRC lrc     = new LRC();
        int result  = 1;
        if(!lrc.connect(mConnectProps)){
            //log an error/warning message
            throw new RuntimeException(LRC_NOT_CONNECTED_MSG +
                                    mConnectProps.getProperty(URL_KEY));
        }
        result = lrc.insert(lfn,pfn,handle);
        //better to keep a handle to the running LRC
        //as a member variable, and close it in
        //RLI.close()
        lrc.close();
        return result;

    }

    /**
     * Inserts multiple mappings into the replica catalog. The input is a
     * map indexed by the LFN. The value for each LFN key is a collection
     * of replica catalog entries.
     *
     * @param x is a map from logical filename string to list of replica
     * catalog entries.
     *
     * @return the number of insertions.
     * @see ReplicaCatalogEntry
     */
     public int insert(Map x) {
         //get hold of the LRC if that is running
        LRC lrc     = new LRC();
        int result  = 1;
        if(!lrc.connect(mConnectProps)){
            //log an error/warning message
            throw new RuntimeException(LRC_NOT_CONNECTED_MSG +
                                    mConnectProps.getProperty(URL_KEY));
        }
        result = lrc.insert(x);
        //better to keep a handle to the running LRC
        //as a member variable, and close it in
        //RLI.close()
        lrc.close();
        return result;

     }

     /**
      * Deletes a specific mapping from the replica catalog. We don't care
      * about the resource handle. More than one entry could theoretically
      * be removed. Upon removal of an entry, all attributes associated
      * with the PFN also evaporate (cascading deletion).
      *
      * It can result in a deletion of more than one entry, and from more
      * than one local replica catalog that might be reporting to the RLI.
      *
      * @param lfn is the logical filename in the tuple.
      * @param pfn is the physical filename in the tuple.
      *
      * @return the number of removed entries.
      */
     public int delete(String lfn, String pfn) {
         ReplicaCatalogEntry rce = new ReplicaCatalogEntry(pfn);
         return delete(lfn,rce);
     }

     /**
      * Deletes a very specific mapping from the replica catalog. The LFN
      * must be matches, the PFN, and all PFN attributes specified in the
      * replica catalog entry. More than one entry could theoretically be
      * removed. Upon removal of an entry, all attributes associated with
      * the PFN also evaporate (cascading deletion).
      * It can result in a deletion of more than one entry, and from more
      * than one local replica catalog that might be reporting to the RLI.
      *
      * @param lfn is the logical filename in the tuple.
      * @param tuple is a description of the PFN and its attributes.
      *
      * @return the number of removed entries, either 0 or 1.
      */
     public int delete(String lfn, ReplicaCatalogEntry tuple) {
         //Map indexed by lrc url and each value a collection
         //of lfns that the RLI says are present in it.
         Set lfns     = new HashSet(1);
         lfns.add(lfn);
         Map lrc2lfn  = this.getLRC2LFNS(lfns);
         int result = 0;

         if(lrc2lfn == null){
             //probably RLI is not connected!!
             return 0;
         }

         // call the delete function on the individual
         // LRCs where the mapping resides
         String key = null,message = null;
         for(Iterator it = lrc2lfn.entrySet().iterator();it.hasNext();){
             Map.Entry entry = (Map.Entry)it.next();
             key = (String)entry.getKey();
             message = "Querying LRC " + key;
             mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

             //push the lrcURL to the properties object
             mConnectProps.setProperty(this.URL_KEY,key);
             LRC lrc     = new LRC();
             if(!lrc.connect(mConnectProps)){
                 //log an error/warning message
                 mLogger.log("Unable to connect to LRC " + key,
                             LogManager.ERROR_MESSAGE_LEVEL);
                 continue;
             }

             //delete from the LRC
             try{
                 result += lrc.delete(lfn,tuple);
             }
             catch(Exception ex){
                 mLogger.log("delete(String, ReplicaCatalogEntry)",ex,
                             LogManager.ERROR_MESSAGE_LEVEL);
             }
             finally{
                 //disconnect
                 lrc.close();
             }
             mLogger.log(message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
        }

        return result;

     }

     /**
      * Deletes all PFN entries for a given LFN from the replica catalog
      * where the PFN attribute is found, and matches exactly the object
      * value. This method may be useful to remove all replica entries that
      * have a certain MD5 sum associated with them. It may also be harmful
      * overkill.
      * It can result in a deletion of more than one entry, and from more
      * than one local replica catalog that might be reporting to the RLI.
      *
      * @param lfn is the logical filename to look for.
      * @param name is the PFN attribute name to look for.
      * @param value is an exact match of the attribute value to match.
      *
      * @return the number of removed entries.
      */
     public int delete(String lfn, String name, Object value) {
         //Map indexed by lrc url and each value a collection
         //of lfns that the RLI says are present in it.
         Set lfns     = new HashSet(1);
         lfns.add(lfn);
         Map lrc2lfn  = this.getLRC2LFNS(lfns);
         int result = 0;

         if(lrc2lfn == null){
             //probably RLI is not connected!!
             return 0;
         }

         // call the delete function on the individual
         // LRCs where the mapping resides
         String key = null,message = null;
         for(Iterator it = lrc2lfn.entrySet().iterator();it.hasNext();){
             Map.Entry entry = (Map.Entry)it.next();
             key = (String)entry.getKey();
             message = "Deleting from LRC " + key;
             mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

             //push the lrcURL to the properties object
             mConnectProps.setProperty(this.URL_KEY,key);
             LRC lrc     = new LRC();
             if(!lrc.connect(mConnectProps)){
                 //log an error/warning message
                 mLogger.log("Unable to connect to LRC " + key,
                             LogManager.ERROR_MESSAGE_LEVEL);
                 continue;
             }

             //delete from the LRC
             try{
                 result += lrc.delete(lfn,name,value);
             }
             catch(Exception ex){
                 mLogger.log("delete(String, String, Object)",
                             ex,LogManager.ERROR_MESSAGE_LEVEL);
             }
             finally{
                 //disconnect
                 lrc.close();
             }
             mLogger.log(message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
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
      * It can result in a deletion of more than one entry, and from more
      * than one local replica catalog that might be reporting to the RLI.
      *
      * @param lfn is the logical filename to look for.
      * @param handle is the resource handle
      *
      * @return the number of entries removed.
      */
    public int deleteByResource(String lfn, String handle) {
        return delete(lfn,SITE_ATTRIBUTE,handle);
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
        //Map indexed by lrc url and each value a collection
        //of lfns that the RLI says are present in it.
        Set lfns     = new HashSet(x.size());
        for(Iterator it = x.keySet().iterator();it.hasNext();){
            lfns.add( (String)it.next());
        }
        Map lrc2lfn  = this.getLRC2LFNS(lfns);
        int result = 0;

        if(lrc2lfn == null){
            //probably RLI is not connected!!
            return 0;
        }

        //compose an exception that might need to be thrown
        CatalogException exception = new ReplicaCatalogException();

        // call the delete function on the individual
        // LRCs where the mapping resides
        String key = null,message = null;
        String lfn;
        for(Iterator it = lrc2lfn.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry)it.next();
            key = ( String )entry.getKey();
            lfns = ( Set )entry.getValue();
            message = "Deleting from LRC " + key;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,key);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + key,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }

            //compose the map to delete for a particular LRC
            Map lmap = new HashMap(lfns.size());
            for(Iterator lfnIt = lfns.iterator();lfnIt.hasNext();){
                lfn = (String)lfnIt.next();
                lmap.put(lfn,x.get(lfn));
            }

             //delete from the LRC
             try{
                 result += lrc.delete(x,matchAttributes);
             }
             catch(ReplicaCatalogException e){
                 exception.setNextException(e);
             }
             catch(Exception ex){
                 mLogger.log("delete(Map,boolean)",
                             ex,LogManager.ERROR_MESSAGE_LEVEL);
             }
             finally{
                 //disconnect
                 lrc.close();
             }
             mLogger.log(message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
        }


        //throw an exception only if a nested exception
        if( (exception = exception.getNextException()) != null) throw exception;

        return result;

    }

    /**
     * Removes all mappings for an LFN from the replica catalog.
     * It can result in a deletion of more than one entry, and from more
     * than one local replica catalog that might be reporting to the RLI.
     *
     * @param lfn is the logical filename to remove all mappings for.
     *
     * @return the number of removed entries.
     */
    public int remove(String lfn) {
        //Map indexed by lrc url and each value a collection
        //of lfns that the RLI says are present in it.
        Set lfns     = new HashSet(1);
        lfns.add(lfn);
        Map lrc2lfn  = this.getLRC2LFNS(lfns);
        int result = 0;

        if(lrc2lfn == null){
            //probably RLI is not connected!!
            return 0;
        }

        // call the delete function on the individual
        // LRCs where the mapping resides
        String key = null,message = null;
        for(Iterator it = lrc2lfn.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry)it.next();
            key = (String)entry.getKey();
            message = "Deleting from LRC " + key;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,key);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + key,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }

            //delete from the LRC
            try{
                result += lrc.remove(lfn);
            }
            catch(Exception ex){
                mLogger.log("remove(String):",ex,
                            LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                //disconnect
                lrc.close();
            }
            mLogger.log(message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
       }

       return result;

    }

    /**
     * Removes all mappings for a set of LFNs.
     * It can result in a deletion of more than one entry, and from more
     * than one local replica catalog that might be reporting to the RLI.
     *
     * @param lfns is a set of logical filename to remove all mappings for.
     *
     * @return the number of removed entries.
     */
    public int remove(Set lfns) {
        //Map indexed by lrc url and each value a collection
        //of lfns that the RLI says are present in it.
        Map lrc2lfn  = this.getLRC2LFNS(lfns);
        int result = 0;
        Set s = null;

        if(lrc2lfn == null){
            //probably RLI is not connected!!
            return 0;
        }

        // call the delete function on the individual
        // LRCs where the mapping resides
        String key = null,message = null;
        for(Iterator it = lrc2lfn.entrySet().iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry)it.next();
            key = (String)entry.getKey();
            message = "Deleting from LRC " + key;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,key);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + key,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }

            //delete from the LRC
            try{
                s = (Set)entry.getValue();
                mLogger.log("\tDeleting the following lfns " + s,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                result += lrc.remove((Set)entry.getValue());
            }
            catch(Exception ex){
                mLogger.log("remove(Set)",ex,LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                //disconnect
                lrc.close();
            }
            mLogger.log(message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
       }

       return result;

    }

    /**
     * Removes all entries from the replica catalog where the PFN attribute
     * is found, and matches exactly the object value.
     * It can result in a deletion of more than one entry, and from more
     * than one local replica catalog that might be reporting to the RLI.
     *
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     *
     * @return the number of removed entries.
     */
    public int removeByAttribute(String name, Object value) {
        //sanity check
        if (this.isClosed()) {
            //probably an exception should be thrown here!!
            throw new RuntimeException(RLI_NOT_CONNECTED_MSG + this.mRLIURL);
        }
        int result = 0;
        String url = null;
        //we need to get hold of all the LRC
        //that report to the RLI and call the
        //list() method on each of them
        for(Iterator it = this.getReportingLRC().iterator();it.hasNext();){
            url = (String)it.next();

            mLogger.log("Removing from LRC " + url,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,url);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + url,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }
            try{
                result += lrc.removeByAttribute(name,value);
            }
            catch(Exception e){
                mLogger.log("removeByAttribute(String,Object)",e,
                            LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                lrc.close();
            }
            mLogger.log( "Removing from LRC " + url + LogManager.MESSAGE_DONE_PREFIX,
                          LogManager.DEBUG_MESSAGE_LEVEL);
        }

        return result;

    }

    /**
     * Removes all entries associated with a particular resource handle.
     * This is useful, if a site goes offline. It is a convenience method,
     * which calls the generic <code>removeByAttribute</code> method.
     * It can result in a deletion of more than one entry, and from more
     * than one local replica catalog that might be reporting to the RLI.
     *
     * @param handle is the site handle to remove all entries for.
     *
     * @return the number of removed entries.
     * @see #removeByAttribute( String, Object )
     */
    public int removeByAttribute(String handle) {
            return removeByAttribute(SITE_ATTRIBUTE,handle);
    }

    /**
     * Removes everything from all the LRCs that report to this RLI.
     *  Use with caution!
     *
     * @return the number of removed entries.
     */
    public int clear() {
        //sanity check
        if (this.isClosed()) {
            //probably an exception should be thrown here!!
            throw new RuntimeException(RLI_NOT_CONNECTED_MSG + this.mRLIURL);
        }
        int result = 0;
        String url = null,message = null;
        //we need to get hold of all the LRC
        //that report to the RLI and call the
        //list() method on each of them
        for(Iterator it = this.getReportingLRC().iterator();it.hasNext();){
            url = (String)it.next();
            message = "Querying LRC " + url;
            mLogger.log(message,LogManager.DEBUG_MESSAGE_LEVEL);

            //push the lrcURL to the properties object
            mConnectProps.setProperty(this.URL_KEY,url);
            LRC lrc     = new LRC();
            if(!lrc.connect(mConnectProps)){
                //log an error/warning message
                mLogger.log("Unable to connect to LRC " + url,
                            LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }
            try{
                result += lrc.clear();
            }
            catch(Exception e){
                mLogger.log("list(String)",e,
                            LogManager.ERROR_MESSAGE_LEVEL);
            }
            finally{
                lrc.close();
            }
            mLogger.log( message + LogManager.MESSAGE_DONE_PREFIX,LogManager.DEBUG_MESSAGE_LEVEL);
        }

        return result;
    }

    /**
     * Explicitely free resources before the garbage collection hits.
     */
    public void close() {
        try{
            if (mRLS != null)
                mRLS.Close();
        }
        catch(RLSException e){
            //ignore
        }
        finally{
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
     * no activity from the RLI.
     *
     * Referred to by the "rli.timeout" property.
     *
     * @param properties   the properties passed in the connect method.
     *
     * @return the timeout value if specified else,
     *         the value specified by "rls.timeout" property, else
     *         DEFAULT_RLI_TIMEOUT.
     *
     * @see #DEFAULT_RLI_TIMEOUT
     */
    public int getTimeout(Properties properties) {
        String prop = properties.getProperty( this.RLI_TIMEOUT_KEY);

        //if prop is null get rls timeout,
        prop = (prop == null)? properties.getProperty(this.RLS_TIMEOUT_KEY):prop;

        int val = 0;
        try {
            val = Integer.parseInt( prop );
        } catch ( Exception e ) {
            val = Integer.parseInt( DEFAULT_RLI_TIMEOUT );
        }
        return val;

    }



    /**
     * Sets the number of lfns in each batch while querying the lrc in the
     * bulk mode.
     *
     * @param properties  the properties passed while connecting.
     *
     */
    protected void setBatchSize(Properties properties) {
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
    protected int getBatchSize() {
        return mBatchSize;
    }


    /**
     * Returns a map indexed by lrc urls. Each value is a set of
     * String objects referring to the logical filenames whose mappings reside
     * at a particular lrc amongst the set of logical filenames passed.
     *
     * @param lfns  the set of lfns queried to the RLI.
     *
     * @return Map indexed by lrc urls. Each value is a set of lfn strings.
     *         null in case the connection to RLI is closed or error.
     */
    private Map getLRC2LFNS(Set lfns){
        int batch = lfns.size() > mBatchSize ? mBatchSize:lfns.size();
        //sanity check
        if (this.isClosed()) {
            //probably an exception should be thrown here!!
            throw new RuntimeException(RLI_NOT_CONNECTED_MSG + this.mRLIURL);
        }

        Map lrc2lfn = new HashMap();//indexed by lrc url and each value a collection
                                    //of lfns that the RLI says are present init.
                                    //get a handle to the rli

        //we need to query the RLI in batches
        for (Iterator it = lfns.iterator(); it.hasNext(); ) {
            ArrayList l = new ArrayList(batch);
            for (int j = 0; (j < batch) && (it.hasNext()); j++) {
                l.add(it.next());
            }

            //query the RLI for one batch
            List res = null;
            try{
                res = mRLI.getLRCBulk(l);
            }
            catch(RLSException ex){
                mLogger.log("getLRC2LFNS(Set)",ex,
                            LogManager.ERROR_MESSAGE_LEVEL);
                //or throw a runtime exception
                return null;
            }
            //iterate through the results and put them in the map
            String lrc = null;
            String lfn = null;
            for(Iterator lit = res.iterator();lit.hasNext();){
                RLSString2Bulk s2b = (RLSString2Bulk) lit.next();
                lfn = s2b.s1;//s1 is the lfn
                lrc = s2b.s2;//s2 denotes the lrc which contains the mapping

                //rc is the exit status returned by the RLI
                if (s2b.rc == RLSClient.RLS_SUCCESS) {
                    //we are really only concerned with success
                    //and do not care about other exit codes
                    Object val = null;
                    Set s      = null;
                    s = ( (val = lrc2lfn.get(lrc)) == null) ?
                        new LinkedHashSet():
                        (LinkedHashSet)val;
                    s.add(lfn);
                    if(val == null)
                        lrc2lfn.put(lrc,s);

                }
            }
        }

        //match LRC's just once against ingore and restrict lists
        for( Iterator it = lrc2lfn.keySet().iterator(); it.hasNext(); ){
            String lrc = ( String ) it.next();
            int state = this.determineQueryType(lrc);

            //do the query on the basis of the state
            if (state == LRC_QUERY_IGNORE) {
                mLogger.log("Skipping LRC " + lrc,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                it.remove();
            }
        }



        return lrc2lfn;
    }


    /**
     * Returns a tri state indicating what type of query needs to be done to
     * a particular LRC.
     *
     * @param url   the LRC url.
     *
     * @return tristate
     */
    private int determineQueryType(String url){
        int type = RLI.LRC_QUERY_NORMAL;

        if(mLRCRestrictList != null){
            for ( int j = 0; j < mLRCRestrictList.length; j++ ) {
                if ( url.indexOf( mLRCRestrictList[ j ] ) != -1 ) {
                    type = RLI.LRC_QUERY_RESTRICT;
                    break;
                }
            }
        }
        if(mLRCIgnoreList != null){
            for ( int j = 0; j < mLRCIgnoreList.length; j++ ) {
                if ( url.indexOf( mLRCIgnoreList[ j ] ) != -1 ) {
                    type = RLI.LRC_QUERY_IGNORE;
                    break;
                }
            }
        }


        return type;
    }


    /**
     * Returns the rls LRC urls to ignore for querying (requested by LIGO).
     *
     * Referred to by the "pegasus.catalog.replica.lrc.ignore" property.
     *
     * @param properties  the properties passed in the connect method.
     *
     * @return String[] if a comma separated list supplied as the property value,
     *         else null
     */
    protected String[] getRLSLRCIgnoreURLs( Properties properties ) {
        String urls =  properties.getProperty( this.LRC_IGNORE_KEY,
                                               null );
        if ( urls != null ) {
            String[] urllist = urls.split( "," );
            return urllist;
        } else {
            return null;
        }
    }

    /**
     * Returns the rls LRC urls to restrict for querying (requested by LIGO).
     *
     * Referred to by the "pegasus.catalog.replica.lrc.restrict" property.
     *
     * @param properties  the properties passed in the connect method.
     *
     * @return String[] if a comma separated list supplied as the property value,
     *         else null
     */
    protected String[] getRLSLRCRestrictURLs( Properties properties ) {
        String urls = properties.getProperty( this.LRC_RESTRICT_KEY,
                                              null );
        if ( urls != null ) {
            String[] urllist = urls.split( "," );
            return urllist;
        } else {
            return null;
        }
    }


    /**
     * Retrieves the URLs of all the LRCs that report to the RLI.
     *
     * @return a Set containing the URLs to all the LRCs that report to the
     *         RLI.
     */
    private Set getReportingLRC(){
        //sanity check
        if (this.isClosed()) {
            //probably an exception should be thrown here!!
            throw new RuntimeException(RLI_NOT_CONNECTED_MSG + this.mRLIURL);
        }
        Set result = new HashSet();
        Collection c = null;

        try{
            c = mRLI.lrcList();
        }
        catch(RLSException e){
            mLogger.log("getReportingLRC(Set)",e,LogManager.ERROR_MESSAGE_LEVEL);
        }

        for(Iterator it = c.iterator(); it.hasNext();){
            RLSLRCInfo lrc = (RLSLRCInfo)it.next();
            result.add(lrc.url);
        }

        return result;
    }




    /**
     * Populates the mapping table by querying the LRC in the mLRCList. At
     * present it searches for all the files in the original DAG. At this point
     * it should be all the files in the Reduced Dag but not doing so in order to
     * conserve memory.
     *
     * @param allInCache  indicates whether all input file entries were found in
     *                    cache or not.
     *
     * @return List
     */
     /*
    private List populateMapTable( boolean allInCache ) {
        String lrcURL = null;
        List list = null;
        RLSQuery client = null;
        ReplicaLocation rl = null;
        List pfnList = null;

        mTable = new HashMap( mSearchFiles.size() );

        int size = mLRCMap.size();
        mLogger.log("Number of LRCs that will be queried: "+size,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        for ( Iterator iter = mLRCMap.keySet().iterator(); iter.hasNext(); ) {
            lrcURL = ( String ) iter.next();
            int state = this.determineQueryType(lrcURL);

            //do the query on the basis of the state
            if ( state == LRC_QUERY_IGNORE ) {
                mLogger.log( "Skipping LRC " + lrcURL,
                             LogManager.DEBUG_MESSAGE_LEVEL);
            }
            else{
                mLogger.log( "Querying LRC " + lrcURL,
                             LogManager.DEBUG_MESSAGE_LEVEL);
                list = ( ArrayList ) mLRCMap.get( lrcURL );
                try {
                    client = new RLSQuery( lrcURL );
                    boolean restrict = (state == LRC_QUERY_RESTRICT);
                    client.bulkQueryLRC( list, RLSQuery.RLS_BULK_QUERY_SIZE,
                                         mTable,restrict);
                    client.close();
                } catch ( Exception e ) {
                    mLogMsg =
                        "RLSEngine.java: While getting connection to LRC " +
                        lrcURL + " " + e;
                    mLogger.log( mLogMsg, LogManager.ERROR_MESSAGE_LEVEL );
                    size--;

                    //do a hard fail only if the RLS exitmode is set to error
                    //or  we could not query to all the LRCs
                    //    and we could not find all the entries in the cache
                    mLogger.log("RLS exit mode is " + mProps.getRLSExitMode(),
                                 LogManager.DEBUG_MESSAGE_LEVEL);
                    boolean exitOnError = mProps.getRLSExitMode().equalsIgnoreCase( "error" );
                    if (  exitOnError || ( size == 0 && !allInCache )) {
                        mLogMsg = ( exitOnError ) ?
                                  "Unable to access LRC " + lrcURL :
                                  "Unable to query any LRC and not all input files are in cache!";
                        throw new RuntimeException( mLogMsg );
                    }
                }
                mLogger.logCompletion("Querying LRC " + lrcURL,
                                      LogManager.DEBUG_MESSAGE_LEVEL);

            }
        }
        return new ArrayList( mTable.keySet() );

    }

*/
    /**
     * The main program, for some unit testing.
     *
     * @param args String[]
     */
    public static void main(String[] args) {
        //setup the logger for the default streams.
        LogManager logger = LogManagerFactory.loadSingletonInstance(  );
        logger.logEventStart( "event.pegasus.catalog.replica.RLI", "planner.version", Version.instance().toString() );

        RLI rli = new RLI();
        Properties props = new Properties();
        props.setProperty( RLI.URL_KEY, "rls://dataserver.phy.syr.edu" );
        props.setProperty( RLI.LRC_IGNORE_KEY, "rls://ldas-cit.ligo.caltech.edu:39281" );
        rli.connect(props);
        System.out.println( "Complete Lookup "  + rli.lookup("H-H1_RDS_C03_L2-847608132-128.gwf" ) );
        System.out.println( "Lookup without attributes "  + rli.lookupNoAttributes("H-H1_RDS_C03_L2-847608132-128.gwf" ) );
        rli.close();
        
        
        //RLI rli = new RLI();
        String lfn = "test";
        Set s = new HashSet();
        s.add(lfn);s.add("testX");s.add("vahi.f.a");
        System.out.println("Connecting " + rli.connect("rls://sukhna"));
        boolean insert = false;
        
        
        if(insert){
            ReplicaCatalogEntry rce = new ReplicaCatalogEntry(
                "gsiftp://sukhna.isi.edu/tmp/test");
            rce.addAttribute("name", "karan");
            LRC sukhna = new LRC();
            sukhna.connect("rls://sukhna");
            sukhna.insert("test", rce);
            sukhna.insert("test", "gsiftp://sukhna.isi.edu/tmp/test1", "isi");
            sukhna.insert("vahi.f.a", "file:///tmp/vahi.f.a", "isi");
            sukhna.insert("testX", "gsiftp://sukhna.isi.edu/tmp/testX", "isi");
            sukhna.insert("testX", "gsiftp://sukhna.isi.edu/tmp/testXvahi", "isi");
            sukhna.close();

            LRC smarty = new LRC();
            ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry(
                "gsiftp://smarty.isi.edu/tmp/test");
            rce1.addAttribute("name", "gaurang");

            smarty.connect("rlsn://smarty");
            smarty.insert("test", rce1);
            smarty.insert("test", "gsiftp://smarty.isi.edu/tmp/test1", "isi");
            smarty.insert("vahi.f.a", "file:///tmp-smarty/vahi.f.a", "isi");
            smarty.insert("testX", "gsiftp://smarty.isi.edu/tmp/testX", "isi");
            smarty.close();
        }

        System.out.println("\n Searching for lfn " + lfn);
        System.out.println(rli.lookup(lfn));

        System.out.println("\n Searching for lfn w/o attributes " + lfn);
        System.out.println(rli.lookupNoAttributes(lfn));

        System.out.println("\nSearching for a set of lfn " + s);
        System.out.println(rli.lookup(s));

        System.out.println("\nSearching for a set of lfn with handle matching" + s);
        System.out.println(rli.lookup(s,"isi"));

        System.out.println("\nSearching for a set of lfn with handle matching "+
                           " returning only pfns" + s);
        System.out.println(rli.lookupNoAttributes(s,"isi"));

        System.out.println("\nListing all the lfns tracked in RLI");
        System.out.println(rli.list("*").size());

        //System.out.println("\n Removing entry for lfn " + lfn);
        //System.out.println(rli.remove(lfn));

        //System.out.println("\n Removing entry for lfns " + s);
        //System.out.println(rli.remove(s));

        //System.out.println("\n Removing entry for lfn by handle matching " + lfn);
        //System.out.println(rli.deleteByResource(lfn,"isi"));

        //System.out.println("\nSearching for a set of lfn " + s);
        //System.out.println(rli.lookup(s));
        Map m = new HashMap();
        m.put("lfn","test*");
        m.put("pfn","*vahi*");
        System.out.println("\nDoing a constraint lookup " + rli.lookup(m));
        rli.close();
    }


}//end of main class

