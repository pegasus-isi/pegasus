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
package edu.isi.pegasus.planner.classes;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.catalog.replica.impl.SimpleFile;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A data class that is used to track the various files placed by the mapper on the staging sites
 * for the workflow.
 *
 * <p>The url's are stored into a memory based Replica Catalog instance, dependant upon type ( get |
 * put URL ).
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PlannerCache extends Data implements Cloneable {

    /** The name of the source key for Replica Catalog Implementer that serves as cache */
    public static final String PLANNER_CACHE_REPLICA_CATALOG_KEY = "file";

    /** The name of the Replica Catalog Implementer that serves as the source for cache files. */
    public static final String PLANNER_CACHE_REPLICA_CATALOG_IMPLEMENTER = "SimpleFile";

    /** The cache storing the GET urls for the files in the workflow */
    private ReplicaCatalog mGetRCCache;

    /** The cache storing the PUT urls for the files in the workflow */
    private ReplicaCatalog mPutRCCache;

    /** The planner options */
    private PlannerOptions mPOptions;

    /** The PegasusProperties */
    private PegasusProperties mProps;

    /** @return */
    public String toString() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /** The default constructor. */
    public PlannerCache() {}

    /**
     * Initialize the replica catalog instances that make up the cache.
     *
     * @param bag
     * @param dag
     */
    public void initialize(PegasusBag bag, ADag dag) {
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        mPOptions = bag.getPlannerOptions();

        mGetRCCache = this.intializeRCAsCache(dag, OPERATION.get);
        mPutRCCache = this.intializeRCAsCache(dag, OPERATION.put);
    }

    /**
     * Inserts a new entry into the cache.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param pfn is the physical filename associated with it.
     * @param handle is a resource handle where the PFN resides.
     * @param type the type of URL.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     */
    public int insert(String lfn, String pfn, String handle, OPERATION type) {

        if (type == OPERATION.get) {
            return mGetRCCache.insert(lfn, pfn, handle);
        } else if (type == OPERATION.put) {
            return mPutRCCache.insert(lfn, pfn, handle);
        } else {
            throw new RuntimeException("Unsupported operation type for planner cache " + type);
        }
    }

    /**
     * Inserts a new entry into the cache.
     *
     * @param lfn is the logical filename under which to book the entry
     * @param rce ReplicaCatalogEntry
     * @param type the type of URL.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     */
    public int insert(String lfn, ReplicaCatalogEntry rce, OPERATION type) {

        if (type == OPERATION.get) {
            return mGetRCCache.insert(lfn, rce);
        } else if (type == OPERATION.put) {
            return mPutRCCache.insert(lfn, rce);
        } else {
            throw new RuntimeException("Unsupported operation type for planner cache " + type);
        }
    }

    /**
     * Retrieves a single entry for a given LFN from the replica catalog. Each entry in the result
     * set is a tuple of a PFN and all its attributes.
     *
     * @param lfn is the logical filename to obtain information for.
     * @param type the type of URL.
     * @return the first matching entry
     * @see ReplicaCatalogEntry
     */
    public ReplicaCatalogEntry lookup(String lfn, OPERATION type) {
        Collection<ReplicaCatalogEntry> results = null;
        ReplicaCatalogEntry result = null;
        if (type == OPERATION.get) {
            results = mGetRCCache.lookup(lfn);
        } else if (type == OPERATION.put) {
            results = mPutRCCache.lookup(lfn);
        } else {
            throw new RuntimeException("Unsupported operation type for planner cache " + type);
        }

        // we return the first entry
        for (ReplicaCatalogEntry entry : results) {
            result = entry;
            break;
        }
        return result;
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog. Each entry in the result set
     * is a tuple of a PFN and all its attributes.
     *
     * @param lfn is the logical filename to obtain information for.
     * @param handle the site handle
     * @param type the type of URL.
     * @return all the entries else empty collection
     * @see Collection<ReplicaCatalogEntry>
     */
    public Collection<ReplicaCatalogEntry> lookupAllEntries(
            String lfn, String handle, OPERATION type) {
        Map<Set<String>, Collection<ReplicaCatalogEntry>> results = null;
        Set<String> lfns = new HashSet();
        lfns.add(lfn);

        if (type == OPERATION.get) {
            results = mGetRCCache.lookup(lfns, handle);
        } else if (type == OPERATION.put) {
            results = mPutRCCache.lookup(lfns, handle);
        } else {
            throw new RuntimeException("Unsupported operation type for planner cache " + type);
        }

        Collection<ReplicaCatalogEntry> result = results.get(lfn);
        return (result == null) ? new LinkedList() : result;
    }

    /**
     * Retrieves the entry for a given filename and resource handle from the replica catalog.
     *
     * @param lfn is the logical filename to obtain information for.
     * @param handle is the resource handle to obtain entries for.
     * @param type the type of URL.
     * @return the (first) matching physical filename, or <code>null</code> if no match was found.
     */
    public String lookup(String lfn, String handle, OPERATION type) {

        if (type == OPERATION.get) {
            return mGetRCCache.lookup(lfn, handle);
        } else if (type == OPERATION.put) {
            return mPutRCCache.lookup(lfn, handle);
        } else {
            throw new RuntimeException("Unsupported operation type for planner cache " + type);
        }
    }

    /** Explicitely free resources before the garbage collection hits. */
    public void close() {
        if (mGetRCCache != null) {
            mGetRCCache.close();
        }
        if (mPutRCCache != null) {
            mPutRCCache.close();
        }
    }

    /**
     * Initializes the transient replica catalog and returns a handle to it.
     *
     * @param dag the workflow being planned
     * @param type the url type that will be stored
     * @return handle to transient catalog
     */
    private ReplicaCatalog intializeRCAsCache(ADag dag, OPERATION type) {
        ReplicaCatalog rc = null;
        mLogger.log(
                "Initialising  Replica Catalog for Planner Cache", LogManager.DEBUG_MESSAGE_LEVEL);

        Properties cacheProps =
                mProps.getVDSProperties().matchingSubset(ReplicaCatalog.c_prefix, false);
        String file =
                mPOptions.getSubmitDirectory() + File.separatorChar + getCacheFileName(dag, type);

        // set the appropriate property to designate path to file
        cacheProps.setProperty(PlannerCache.PLANNER_CACHE_REPLICA_CATALOG_KEY, file);

        // the planner cache is to be never written out
        // PM-677
        cacheProps.setProperty(SimpleFile.READ_ONLY_KEY, "true");
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        try {
            rc =
                    ReplicaFactory.loadInstance(
                            PLANNER_CACHE_REPLICA_CATALOG_IMPLEMENTER, bag, cacheProps);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to initialize the replica catalog that acts as planner cache" + file,
                    e);
        }
        return rc;
    }

    /**
     * Constructs the basename to the cache file that is to be used to log the transient files. The
     * basename is dependant on whether the basename prefix has been specified at runtime or not.
     *
     * @param adag the ADag object containing the workflow that is being concretized.
     * @return the name of the cache file
     */
    private String getCacheFileName(ADag adag, OPERATION operation) {
        StringBuffer sb = new StringBuffer();
        String bprefix = mPOptions.getBasenamePrefix();

        if (bprefix != null) {
            // the prefix is not null using it
            sb.append(bprefix);
        } else {
            // generate the prefix from the name of the dag
            sb.append(adag.getLabel()).append("-").append(adag.getIndex());
        }

        // PM-677 deliberately a put cache to make sure it is never
        // it does not overwrite the workflow cache written out in
        // Transfer Engine. We do explicilty set read only flag to true
        // This is a failsafe.
        sb.append(".").append(operation).append("cache");

        return sb.toString();
    }
}
