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
package edu.isi.pegasus.planner.catalog.transformation;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.common.util.FileDetector;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.catalog.transformation.impl.Directory;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Properties;

/**
 * A factory class to load the appropriate implementation of Transformation Catalog as specified by
 * properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TransformationFactory {

    /** The default package where all the implementations reside. */
    public static final String DEFAULT_PACKAGE_NAME =
            "edu.isi.pegasus.planner.catalog.transformation.impl";

    public static final String DEFAULT_CATALOG_IMPLEMENTOR =
            edu.isi.pegasus.planner.catalog.transformation.impl.YAML.class.getCanonicalName();

    public static final String YAML_CATALOG_IMPLEMENTOR =
            edu.isi.pegasus.planner.catalog.transformation.impl.YAML.class.getCanonicalName();

    public static final String TEXT_CATALOG_IMPLEMENTOR =
            edu.isi.pegasus.planner.catalog.transformation.impl.Text.class.getCanonicalName();

    public static final String DIRECTORY_CATALOG_IMPLEMENTOR =
            edu.isi.pegasus.planner.catalog.transformation.impl.Directory.class.getCanonicalName();

    /** The default basename of the yaml transformation catalog file. */
    public static final String DEFAULT_YAML_TRANSFORMATION_CATALOG_BASENAME = "transformations.yml";

    /**
     * The default directory from which transformations are picked up if using directory backend.
     */
    public static final String DEFAULT_TRANSFORMATION_CATALOG_DIRECTORY = "transformations";

    /** The default basename of the transformation catalog file. */
    public static final String DEFAULT_TEXT_TRANSFORMATION_CATALOG_BASENAME = "tc.txt";

    /**
     * the basename of the property key that indicates the transformation catalog loaded is
     * transient
     */
    public static final String TRANSIENT_PROPERTY_KEY = "transient";

    /**
     * Connects the interface with the transformation catalog implementation. The choice of backend
     * is configured through properties. This method uses default properties from the property
     * singleton.
     *
     * @return handle to the Transformation Catalog.
     * @throws TransformationFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static TransformationCatalog loadInstance() throws TransformationFactoryException {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, LogManagerFactory.loadSingletonInstance());
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());

        return loadInstance(bag);
    }

    /**
     * Loads the transformation catalog, and also merges in entries from the workflow transformation
     * catalog section and any transformations specified in the directory either passed on the
     * command line or the default transformations directory, into the catalog instance.
     *
     * @param bag bag of initalization objects
     * @param dag the workflow
     * @return Transformation Catalog
     * @throws RuntimeException encountered while loading TC only if the daxStore is null or empty,
     *     or no transformations are loaded from the default directory from which the planner is run
     */
    public static final TransformationCatalog loadInstanceWithStores(PegasusBag bag, ADag dag) {
        TransformationStore directoriesTransformationStore =
                TransformationFactory.loadTransformationStoreFromDirectories(bag, dag);
        TransformationCatalog catalog =
                TransformationFactory.loadInstanceWithStores(
                        bag, dag, directoriesTransformationStore);
        LogManager logger = bag.getLogger();

        // linked list to preserve order
        LinkedList<TransformationStore> stores = new LinkedList();

        // we iterate through the DAX Transformation Store and update
        // the transformation catalog with any transformation specified.
        stores.add(dag.getTransformationStore());
        // PM-1926 the transformations loaded from directories via command line have highest
        // precedence
        stores.add(directoriesTransformationStore);

        for (TransformationStore store : stores) {
            for (TransformationCatalogEntry entry : store.getAllEntries()) {
                try {
                    // insert an entry into the transformation catalog
                    // for the mapper to pick up later on
                    logger.log(
                            "Addding entry into transformation catalog " + entry,
                            LogManager.DEBUG_MESSAGE_LEVEL);

                    if (catalog.insert(entry, false) != 1) {
                        logger.log(
                                "Unable to add entry to transformation catalog " + entry,
                                LogManager.WARNING_MESSAGE_LEVEL);
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(
                            "Exception while inserting into TC in Interpool Engine " + ex);
                }
            }
        }

        return catalog;
    }

    /**
     * Loads the transformation catalog.
     *
     * @param bag bag of initalization objects
     * @param dag the workflow
     * @param directoriesTXStore the store of transformations loaded from directory
     * @return Transformation Catalog
     * @throws RuntimeException encountered while loading TC only if the daxStore is null or empty,
     *     or no transformations are loaded from the default directory from which the planner is run
     */
    private static final TransformationCatalog loadInstanceWithStores(
            PegasusBag bag, ADag dag, TransformationStore directoriesTXStore) {

        TransformationCatalog catalog = null;
        TransformationStore daxStore = dag.getTransformationStore();
        try {
            catalog = TransformationFactory.loadInstance(bag);
        } catch (TransformationFactoryException e) {
            if ((daxStore == null || daxStore.isEmpty())
                    &&
                    // PM-1926 we should consider any transformations loaded from the
                    // default directory before throwing an error
                    (directoriesTXStore == null || directoriesTXStore.isEmpty())
                    && dag.getWorkflowMetrics().getTaskCount(Job.COMPUTE_JOB)
                            != 0) { // pure hierarchal workflows with no compute jobs should not
                // throw error
                throw e;
            }
            // log the error nevertheless
            bag.getLogger()
                    .log(
                            "Ignoring error encountered while loading Transformation Catalog "
                                    + e.convertException(),
                            LogManager.DEBUG_MESSAGE_LEVEL);
        }
        // create a temp file as a TC backend for planning purposes
        if (catalog == null) {
            File f = null;
            try {
                f = File.createTempFile("tc.", ".txt");
                bag.getLogger()
                        .log(
                                "Created a temporary transformation catalog backend " + f,
                                LogManager.DEBUG_MESSAGE_LEVEL);
            } catch (IOException ex) {
                throw new RuntimeException(
                        "Unable to create a temporary transformation catalog backend " + f, ex);
            }
            PegasusBag b = new PegasusBag();
            b.add(PegasusBag.PEGASUS_LOGMANAGER, bag.getLogger());
            PegasusProperties props = PegasusProperties.nonSingletonInstance();
            props.setProperty(
                    PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_PROPERTY,
                    TransformationFactory.TEXT_CATALOG_IMPLEMENTOR);
            props.setProperty(
                    PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
                    f.getAbsolutePath());
            // PM-1947
            props.setProperty(
                    PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_PROPERTY
                            + "."
                            + TransformationFactory.TRANSIENT_PROPERTY_KEY,
                    "true");
            b.add(PegasusBag.PEGASUS_PROPERTIES, props);
            return loadInstanceWithStores(b, dag, directoriesTXStore);
        }
        return catalog;
    }

    /**
     * Connects the interface with the transformation catalog implementation. The choice of backend
     * is configured through properties. This class is useful for non-singleton instances that may
     * require changing properties.
     *
     * @param bag is bag of initialization objects
     * @return handle to the Transformation Catalog.
     * @throws TransformationFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static TransformationCatalog loadInstance(PegasusBag bag)
            throws TransformationFactoryException {

        PegasusProperties properties = bag.getPegasusProperties();
        if (properties == null) {
            throw new TransformationFactoryException("Invalid NULL properties passed");
        }
        File dir = bag.getPlannerDirectory();
        if (dir == null) {
            throw new TransformationFactoryException("Invalid Directory passed");
        }
        if (bag.getLogger() == null) {
            throw new TransformationFactoryException("Invalid Logger passed");
        }

        /* get the implementor from properties */
        String catalogImplementor = bag.getPegasusProperties().getTCMode();

        Properties props =
                bag.getPegasusProperties()
                        .matchingSubset(
                                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_PROPERTY, false);
        if (catalogImplementor == null) {
            // check if file is specified in properties
            if (props.containsKey("file")) {
                // PM-1518 check for type of file
                if (FileDetector.isTypeYAML(props.getProperty("file"))) {
                    catalogImplementor = YAML_CATALOG_IMPLEMENTOR;
                } else {
                    catalogImplementor = TEXT_CATALOG_IMPLEMENTOR;
                }
            } else {
                // catalogImplementor = DEFAULT_CATALOG_IMPLEMENTOR;
                // PM-1486 check for default files
                File defaultYAML =
                        new File(
                                dir,
                                TransformationFactory.DEFAULT_YAML_TRANSFORMATION_CATALOG_BASENAME);
                File defaultText =
                        new File(
                                dir,
                                TransformationFactory.DEFAULT_TEXT_TRANSFORMATION_CATALOG_BASENAME);
                if (exists(defaultYAML)) {
                    catalogImplementor = TransformationFactory.YAML_CATALOG_IMPLEMENTOR;
                    props.setProperty("file", defaultYAML.getAbsolutePath());
                } else if (exists(defaultText)) {
                    catalogImplementor = TransformationFactory.TEXT_CATALOG_IMPLEMENTOR;
                    props.setProperty("file", defaultText.getAbsolutePath());
                } else {
                    // then just set to default implementor and let the implementing class load
                    catalogImplementor = TransformationFactory.DEFAULT_CATALOG_IMPLEMENTOR;
                }
            }
        }

        /* prepend the package name if required */
        catalogImplementor =
                (catalogImplementor.indexOf('.') == -1)
                        ?
                        // pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + catalogImplementor
                        :
                        // load directly
                        catalogImplementor;

        TransformationCatalog catalog;

        /* try loading the catalog implementation dynamically */
        try {
            DynamicLoader dl = new DynamicLoader(catalogImplementor);
            catalog = (TransformationCatalog) dl.instantiate(new Object[0]);

            if (catalog == null) {
                throw new RuntimeException("Unable to load " + catalogImplementor);
            }
            catalog.initialize(bag);
            if (!catalog.connect(props)) {
                throw new TransformationFactoryException(
                        " Unable to connect to Transformation Catalog with properties" + props,
                        catalogImplementor);
            }
        } catch (Exception e) {
            // e.printStackTrace();
            throw new TransformationFactoryException(
                    " Unable to instantiate Transformation Catalog ", catalogImplementor, e);
        }
        if (catalog == null) {
            throw new TransformationFactoryException(
                    " Unable to instantiate Transformation Catalog ", catalogImplementor);
        }
        return catalog;
    }

    /**
     * Returns whether a file exists or not
     *
     * @param file
     * @return
     */
    private static boolean exists(File file) {
        return file == null ? false : file.exists() && file.canRead();
    }

    /**
     * Loads the mappings from the input directory
     *
     * @param bag the bag of Pegasus initialization objects
     * @param workflow the workflow
     */
    private static TransformationStore loadTransformationStoreFromDirectories(
            PegasusBag bag, ADag workflow) {
        PegasusProperties properties = bag.getPegasusProperties();
        PegasusProperties connectProperties = PegasusProperties.nonSingletonInstance();
        connectProperties.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_PROPERTY,
                TransformationFactory.DIRECTORY_CATALOG_IMPLEMENTOR);
        for (String key :
                properties
                        .getVDSProperties()
                        .matchingSubset(TransformationCatalog.c_prefix, true)
                        .stringPropertyNames()) {
            connectProperties.setProperty(key, properties.getProperty(key));
        }
        PegasusBag b = new PegasusBag();
        b.add(PegasusBag.PEGASUS_LOGMANAGER, bag.getLogger());
        b.add(PegasusBag.PEGASUS_PROPERTIES, connectProperties);
        b.add(PegasusBag.PLANNER_OPTIONS, bag.getPlannerOptions());
        b.add(PegasusBag.SITE_STORE, bag.getHandleToSiteStore());

        Collection<String> directories = new HashSet();
        // null is fine, as the transformation factory will default to
        // the default directory transformations to pick up the
        // excutables from
        directories.add(bag.getPlannerOptions().getTransformationsDirectory());

        return TransformationFactory.loadTransformationStoreFromDirectories(
                b, workflow, directories);
    }

    /**
     * Loads the mappings from the input directory
     *
     * @param bag the bag of initialization objects
     * @param workflow the workflow
     * @param directories set of directories to load from
     */
    private static TransformationStore loadTransformationStoreFromDirectories(
            PegasusBag bag, ADag workflow, Collection<String> directories) {
        TransformationStore store = new TransformationStore();

        PegasusProperties properties = bag.getPegasusProperties();
        LogManager logger = bag.getLogger();
        for (String directory : directories) {
            logger.logEventStart(
                    LoggingKeys.EVENT_PEGASUS_LOAD_DIRECTORY_CACHE,
                    LoggingKeys.DAX_ID,
                    workflow.getAbstractWorkflowName());

            TransformationCatalog catalog = null;

            // set the appropriate property to designate path to directory.
            boolean defaultDirectoryUsed = false;
            if (directory == null) {
                defaultDirectoryUsed = true;
                directory = TransformationFactory.DEFAULT_TRANSFORMATION_CATALOG_DIRECTORY;
            }
            directory = new File(directory).getAbsolutePath();
            // need to set with the complete prefix as we are passing
            // Pegasus properties not java properties
            properties.setProperty(
                    TransformationCatalog.c_prefix + "." + Directory.DIRECTORY_PROPERTY_KEY,
                    directory);

            try {
                catalog = TransformationFactory.loadInstance(bag);
                for (TransformationCatalogEntry entry : catalog.getContents()) {
                    store.addEntry(entry);
                }

            } catch (Exception e) {
                int logLevel =
                        defaultDirectoryUsed
                                ? LogManager.DEBUG_MESSAGE_LEVEL
                                : LogManager.ERROR_MESSAGE_LEVEL;
                logger.log(
                        "Unable to load transformations from directory  " + directory, e, logLevel);
            } finally {
                if (catalog != null) {
                    catalog.close();
                }
            }
            logger.logEventCompletion();
        }
        return store;
    }
}
