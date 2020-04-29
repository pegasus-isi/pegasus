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
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.Mapper;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import edu.isi.pegasus.planner.mapper.SubmitMapper;
import edu.isi.pegasus.planner.partitioner.graph.Bag;
import java.io.File;
import java.util.Map;

/**
 * A bag of objects that needs to be passed to various refiners. It contains handles to the various
 * catalogs, the properties and the planner options.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PegasusBag implements Bag {

    /** Array storing the names of the attributes that are stored with the site. */
    public static final String PEGASUS_INFO[] = {
        "pegasus-properties", "planner-options", "replica-catalog", "site-catalog",
        "transformation-catalog", "transformation-mapper", "pegasus-logger", "site-store",
        "planner-cache", "worker-package-map", "uses-pmc", "planner-metrics",
        "submit-mapper", "staging-mapper", "planner-directory"
    };

    /** The constant to be passed to the accessor functions to get or set the PegasusProperties. */
    public static final Integer PEGASUS_PROPERTIES = new Integer(0);

    /**
     * The constant to be passed to the accessor functions to get or set the options passed to the
     * planner.
     */
    public static final Integer PLANNER_OPTIONS = new Integer(1);

    /**
     * The constant to be passed to the accessor functions to get the file source for replica
     * catalog
     */
    public static final Integer REPLICA_CATALOG_FILE_SOURCE = new Integer(2);

    /**
     * The constant to be passed to the accessor functions to get or set the handle to the site
     * catalog.
     */
    public static final Integer SITE_CATALOG = new Integer(3);

    /**
     * The constant to be passed to the accessor functions to get or set the handle to the
     * transformation catalog.
     */
    public static final Integer TRANSFORMATION_CATALOG = new Integer(4);

    /**
     * The constant to be passed to the accessor functions to get or set the handle to the
     * Transformation Mapper.
     */
    public static final Integer TRANSFORMATION_MAPPER = new Integer(5);

    /**
     * The constant to be passed to the accessor functions to get or set the handle to the Logging
     * manager
     */
    public static final Integer PEGASUS_LOGMANAGER = new Integer(6);

    /**
     * The constant to be passed to the accessor functions to get or set the handle to the Site
     * Store
     */
    public static final Integer SITE_STORE = new Integer(7);

    /**
     * The constant to be passed to the accessor functions to get or set the handle to the planner
     * cache for planning purposes.
     */
    public static final Integer PLANNER_CACHE = new Integer(8);

    /**
     * The constant to be passed to the accessor functions to get or set the handle to the worker
     * package maps
     */
    public static final Integer WORKER_PACKAGE_MAP = new Integer(9);

    /**
     * The constant to be passed to the accessor functions to get or set the whether the planner
     * used PMC or not
     */
    public static final Integer USES_PMC = new Integer(10);

    /**
     * The constant to be passed to the accessor functions to get or set the the planner metrics
     * that are logged during the planning purpose
     */
    public static final Integer PLANNER_METRICS = new Integer(11);

    /**
     * The handle to the file factory that is used to create the relative submit directories in the
     * deep hierarchy structure.
     */
    public static final Integer PEGASUS_SUBMIT_MAPPER = new Integer(12);

    /**
     * The handle to the file factory that is used to create the relative directories on the staging
     * site
     */
    public static final Integer PEGASUS_STAGING_MAPPER = new Integer(13);

    /** The directory from which the planner is invoked */
    public static final Integer PLANNER_DIRECTORY = new Integer(14);

    /** The handle to the <code>PegasusProperties</code>. */
    private PegasusProperties mProps;

    /** The options passed to the planner. */
    private PlannerOptions mPOptions;

    /** The handle to the replica catalog. */
    private File mReplicaFileSource;

    /** The handle to the site catalog. */
    // private PoolInfoProvider mSCHandle;

    /** The handle to the transformation catalog. */
    private TransformationCatalog mTCHandle;

    /** The handle to the Transformation Mapper. */
    private Mapper mTCMapper;

    /** The handle to the LogManager. */
    private LogManager mLogger;

    /** The site store containing the sites that need to be used. */
    private SiteStore mSiteStore;

    /**
     * The transient replica catalog that tracks the files created or transferred during the
     * workflow
     */
    private PlannerCache mPlannerCache;

    /**
     * Worker Package Map, that indexes execution site with the location of the corresponding worker
     * package in the submit directory
     */
    private Map<String, String> mWorkerPackageMap;

    /** A boolean indicating whether we use PMC or not */
    private boolean mUsesPMC;

    /** The planner metrics to use. */
    private PlannerMetrics mPMetrics;

    /** The handle to the submit mapper */
    private SubmitMapper mSubmitMapper;

    /** The handle to the staging mapper */
    private StagingMapper mStagingMapper;

    /** the directory from which the planner is invoked */
    private File mPlannerDirectory;

    /** The default constructor. */
    public PegasusBag() {
        // by default uses PMC is set to false
        mUsesPMC = false;
        mPlannerDirectory = new File(System.getProperty("user.dir"));
    }

    /**
     * Adds an object to the underlying bag corresponding to a particular key.
     *
     * @param key the key with which the value has to be associated.
     * @param value the value to be associated with the key.
     * @return boolean indicating if insertion was successful.
     */
    public boolean add(Object key, Object value) {
        // to denote if object is of valid type or not.
        boolean valid = true;
        int k = getIntValue(key);

        switch (k) {
            case 0: // PEGASUS_PROPERTIES
                if (value != null && value instanceof PegasusProperties)
                    mProps = (PegasusProperties) value;
                else valid = false;
                break;

            case 1: // PLANNER_OPTIONS
                if (value != null && value instanceof PlannerOptions)
                    mPOptions = (PlannerOptions) value;
                else valid = false;
                break;

            case 2: // REPLICA_CATALOG:
                if (value != null && value instanceof File) mReplicaFileSource = (File) value;
                else valid = false;
                break;

            case 3: // SITE_CATALOG:
                valid = false;
                break;

            case 4: // TRANSFORMATION_CATALOG:
                if (value != null && value instanceof TransformationCatalog)
                    mTCHandle = (TransformationCatalog) value;
                else valid = false;
                break;

            case 5: // TRANSFORMATION_MAPPER
                if (value != null && value instanceof Mapper) mTCMapper = (Mapper) value;
                else valid = false;
                break;

            case 6: // PEGASUS_LOGGER
                if (value != null && value instanceof LogManager) mLogger = (LogManager) value;
                else valid = false;
                break;

            case 7: // SITE_STORE
                if (value != null && value instanceof SiteStore) mSiteStore = (SiteStore) value;
                else valid = false;
                break;

            case 8: // PLANNER_CACHE
                if (value != null && value instanceof PlannerCache)
                    mPlannerCache = (PlannerCache) value;
                else valid = false;
                break;

            case 9: // WORKER_PACKAGE_MAP
                if (value != null && value instanceof Map) mWorkerPackageMap = (Map) value;
                else valid = false;
                break;

            case 10: // uses PMC
                if (value != null && value instanceof Boolean) mUsesPMC = (Boolean) value;
                else valid = false;
                break;

            case 11: // Planner metrics
                if (value != null && value instanceof PlannerMetrics)
                    mPMetrics = (PlannerMetrics) value;
                else valid = false;
                break;

            case 12: // Submit Mapper
                if (value != null && value instanceof SubmitMapper)
                    mSubmitMapper = (SubmitMapper) value;
                else valid = false;
                break;

            case 13: // Staging Mapper
                if (value != null && value instanceof StagingMapper)
                    mStagingMapper = (StagingMapper) value;
                else valid = false;
                break;

            case 14: // Planner Directory
                if (value != null && value instanceof File) mPlannerDirectory = (File) value;
                else valid = false;
                break;

            default:
                throw new RuntimeException(
                        " Wrong Pegasus Bag key. Please use one of the predefined Integer key types");
        }

        // if object is not null , and valid == false
        // throw exception
        if (!valid && value != null) {
            throw new RuntimeException(
                    "Invalid object passed for key " + PEGASUS_INFO[k] + " Passed " + value);
        }

        return valid;
    }

    /**
     * Returns true if the namespace contains a mapping for the specified key.
     *
     * @param key The key that you want to search for in the bag.
     * @return boolean
     */
    public boolean containsKey(Object key) {

        int k = -1;
        try {
            k = ((Integer) key).intValue();
        } catch (Exception e) {
        }

        return (k >= PegasusBag.PEGASUS_PROPERTIES.intValue()
                && k <= PegasusBag.PLANNER_METRICS.intValue());
    }

    /**
     * Returns an objects corresponding to the key passed.
     *
     * @param key the key corresponding to which the objects need to be returned.
     * @return the object that is found corresponding to the key or null.
     */
    public Object get(Object key) {
        int k = getIntValue(key);

        switch (k) {
            case 0:
                return this.mProps;

            case 1:
                return this.mPOptions;

            case 2:
                return this.mReplicaFileSource;

                /*
                case 3:
                   return this.mSCHandle;
                */

            case 4:
                return this.mTCHandle;

            case 5: // TRANSFORMATION_MAPPER
                return this.mTCMapper;

            case 6: // PEGASUS_LOGMANAGER
                return this.mLogger;

            case 7: // SITE_STORE
                return this.mSiteStore;

            case 8: // TRANSIENT_RC
                return this.mPlannerCache;

            case 9: // WORKER PACKAGE MAP
                return this.mWorkerPackageMap;

            case 10: // USES PMC
                return this.mUsesPMC;

            case 11: // PLANNER METRICS
                return this.mPMetrics;

            case 12: // Submit Mapper
                return this.mSubmitMapper;

            case 13: // Staging Mapper
                return this.mStagingMapper;

            case 14: // Staging Mapper
                return this.mPlannerDirectory;

            default:
                throw new RuntimeException(
                        " Wrong Pegasus Bag key. Please use one of the predefined Integer key types");
        }
    }

    /**
     * A convenience method to get PlannerOptions
     *
     * @return the handle to options passed to the planner.
     */
    public PlannerOptions getPlannerOptions() {
        return (PlannerOptions) get(PegasusBag.PLANNER_OPTIONS);
    }

    /**
     * A convenience method to get PegasusProperties
     *
     * @return the handle to the properties.
     */
    public PegasusProperties getPegasusProperties() {
        return (PegasusProperties) get(PegasusBag.PEGASUS_PROPERTIES);
    }

    /**
     * A convenience method to get Logger/
     *
     * @return the handle to the logger.
     */
    public LogManager getLogger() {
        return (LogManager) get(PegasusBag.PEGASUS_LOGMANAGER);
    }

    /**
     * A convenience method to get the handle to the site catalog.
     *
     * @return the handle to site catalog
     */
    /*
    public PoolInfoProvider getHandleToSiteCatalog(){
        return ( PoolInfoProvider )get( PegasusBag.SITE_CATALOG );
    }*/

    /**
     * A convenience method to get the handle to the site store
     *
     * @return the handle to site store
     */
    public SiteStore getHandleToSiteStore() {
        return (SiteStore) get(PegasusBag.SITE_STORE);
    }

    /**
     * A convenience method to get the handle to the planner cache
     *
     * @return the handle to transient replica catalog
     */
    public PlannerCache getHandleToPlannerCache() {
        return (PlannerCache) get(PegasusBag.PLANNER_CACHE);
    }

    /**
     * A convenience method to get the handle to the transformation catalog.
     *
     * @return the handle to transformation catalog
     */
    public TransformationCatalog getHandleToTransformationCatalog() {
        return (TransformationCatalog) get(PegasusBag.TRANSFORMATION_CATALOG);
    }

    /**
     * A convenience method to return the replica catalog file source
     *
     * @return file source else null
     */
    public File getReplicaCatalogFileSource() {
        return (File) get(PegasusBag.REPLICA_CATALOG_FILE_SOURCE);
    }

    /**
     * A convenience method to get the handle to the transformation mapper.
     *
     * @return the handle to transformation catalog
     */
    public Mapper getHandleToTransformationMapper() {
        return (Mapper) get(PegasusBag.TRANSFORMATION_MAPPER);
    }

    /**
     * A convenience method to get the worker package
     *
     * @return the handle to worker package map
     */
    public Map<String, String> getWorkerPackageMap() {
        return (Map) get(PegasusBag.WORKER_PACKAGE_MAP);
    }

    /**
     * A convenience method to return whether the planner used PMC or not
     *
     * @return boolean indicating whether PMC was used or not
     */
    public boolean plannerUsesPMC() {
        return (Boolean) get(PegasusBag.USES_PMC);
    }

    /**
     * A convenience method to return the File Factory used
     *
     * @return file factory
     */
    public SubmitMapper getSubmitMapper() {
        return (SubmitMapper) get(PegasusBag.PEGASUS_SUBMIT_MAPPER);
    }

    /**
     * A convenience method to return the Staging Mapper
     *
     * @return staging mapper
     */
    public StagingMapper getStagingMapper() {
        return (StagingMapper) get(PegasusBag.PEGASUS_STAGING_MAPPER);
    }

    /**
     * A convenience method to get the planner directory
     *
     * @return the handle to directory from which planner was invoked
     */
    public File getPlannerDirectory() {
        return (File) get(PegasusBag.PLANNER_DIRECTORY);
    }

    /**
     * A convenience method to get the intValue for the object passed.
     *
     * @param key the key to be converted
     * @return the int value if object an integer, else -1
     */
    private int getIntValue(Object key) {

        int k = -1;
        try {
            k = ((Integer) key).intValue();
        } catch (Exception e) {
        }

        return k;
    }
}
