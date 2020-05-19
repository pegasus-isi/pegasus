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
package edu.isi.pegasus.planner.catalog.replica;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.common.util.FileDetector;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.*;
import java.util.Enumeration;
import java.util.Properties;

/**
 * This factory loads a replica catalog, as specified by the properties. Each invocation of the
 * factory will result in a new instance of a connection to the replica catalog.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @version $Revision$
 * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalog
 * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
 * @see edu.isi.pegasus.planner.catalog.replica.impl.JDBCRC
 */
public class ReplicaFactory {

    /** Package to prefix "just" class names with. */
    public static final String DEFAULT_PACKAGE = "edu.isi.pegasus.planner.catalog.replica.impl";

    public static final String DEFAULT_CATALOG_IMPLEMENTOR =
            edu.isi.pegasus.planner.catalog.replica.impl.YAML.class.getCanonicalName();

    public static final String YAML_CATALOG_IMPLEMENTOR =
            edu.isi.pegasus.planner.catalog.replica.impl.YAML.class.getCanonicalName();

    public static final String FILE_CATALOG_IMPLEMENTOR =
            edu.isi.pegasus.planner.catalog.replica.impl.SimpleFile.class.getCanonicalName();

    /** The default basename of the yaml transformation catalog file. */
    public static final String DEFAULT_YAML_REPLICA_CATALOG_BASENAME = "replicas.yml";

    /** The default basename of the transformation catalog file. */
    public static final String DEFAULT_FILE_REPLICA_CATALOG_BASENAME = "rc.txt";

    /**
     * Connects the interface with the replica catalog implementation. The choice of backend is
     * configured through properties. This class is useful for non-singleton instances that may
     * require changing properties.
     *
     * @param bag bag of Pegasus initialization objects
     * @exception ClassNotFoundException if the schema for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the schema's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the schema class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the schema class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the schema throws an exception
     *     while being dynamically loaded.
     * @see org.griphyn.common.util.CommonProperties
     * @see #loadInstance()
     */
    public static ReplicaCatalog loadInstance(PegasusBag bag)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {

        return loadInstance(bag, bag.getPegasusProperties().getPropertiesInSubmitDirectory());
    }

    /**
     * Connects the interface with the replica catalog implementation. The choice of backend is
     * configured through properties. This class is useful for non-singleton instances that may
     * require changing properties.
     *
     * @param bag bag of Pegasus initialization objects
     * @param propFile the physical location of the property propFile
     * @exception ClassNotFoundException if the schema for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the schema's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the schema class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the schema class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the schema throws an exception
     *     while being dynamically loaded.
     * @see org.griphyn.common.util.CommonProperties
     * @see #loadInstance()
     */
    public static ReplicaCatalog loadInstance(PegasusBag bag, String propFile)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {

        // sanity check
        PegasusProperties properties = bag.getPegasusProperties();
        if (properties == null) {
            throw new NullPointerException("Invalid NULL properties passed");
        }
        File dir = bag.getPlannerDirectory();
        if (dir == null) {
            throw new NullPointerException("Invalid Directory passed");
        }
        LogManager logger = bag.getLogger();
        if (logger == null) {
            throw new NullPointerException("Invalid Logger passed");
        }

        Properties connectProps = properties.matchingSubset(ReplicaCatalog.c_prefix, false);
        logger.log(
                "[Replica Factory] Connect properties detected " + connectProps,
                LogManager.DEBUG_MESSAGE_LEVEL);

        // get the default db driver properties in first pegasus.catalog.*.db.driver.*
        Properties db = properties.matchingSubset(ReplicaCatalog.DB_ALL_PREFIX, false);
        // now overload with the work catalog specific db properties.
        // pegasus.catalog.work.db.driver.*
        db.putAll(properties.matchingSubset(ReplicaCatalog.DB_PREFIX, false));

        // PM-778 properties propFile location requried for pegasus-db-admin
        if (propFile != null) {
            connectProps.put("properties.file", propFile);
        }

        // to make sure that no confusion happens.
        // add the db prefix to all the db properties
        for (Enumeration e = db.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            connectProps.put("db." + key, db.getProperty(key));
        }

        // determine the class that implements the work catalog
        String catalogImplementor = properties.getProperty(ReplicaCatalog.c_prefix);
        if (catalogImplementor == null) {
            // check if file is specified in properties
            if (connectProps.containsKey("file")) {
                // PM-1518 check for type of file
                if (FileDetector.isTypeYAML(connectProps.getProperty("file"))) {
                    catalogImplementor = YAML_CATALOG_IMPLEMENTOR;
                } else {
                    catalogImplementor = FILE_CATALOG_IMPLEMENTOR;
                }
            } else {
                // catalogImplementor = DEFAULT_CATALOG_IMPLEMENTOR;
                // PM-1486 check for default files
                File defaultYAML =
                        new File(dir, ReplicaFactory.DEFAULT_YAML_REPLICA_CATALOG_BASENAME);
                File defaultText =
                        new File(dir, ReplicaFactory.DEFAULT_FILE_REPLICA_CATALOG_BASENAME);
                if (exists(defaultYAML)) {
                    catalogImplementor = ReplicaFactory.YAML_CATALOG_IMPLEMENTOR;
                    connectProps.setProperty("file", defaultYAML.getAbsolutePath());
                } else if (exists(defaultText)) {
                    catalogImplementor = ReplicaFactory.FILE_CATALOG_IMPLEMENTOR;
                    connectProps.setProperty("file", defaultText.getAbsolutePath());
                } else {
                    // then just set to default implementor and let the implementing class load
                    catalogImplementor = ReplicaFactory.DEFAULT_CATALOG_IMPLEMENTOR;
                }
            }
        }
        return loadInstance(catalogImplementor, bag, connectProps);
    }

    /**
     * Connects the interface with the replica catalog implementation.The choice of backend is
     * configured through properties.This class is useful for non-singleton instances that may
     * require changing properties.
     *
     * @param catalogImplementor the catalog implementor to invoke
     * @param bag bag of Pegasus initialization objects
     * @param connectProps
     * @exception ClassNotFoundException if the schema for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the schema's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the schema class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the schema class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the schema throws an exception
     *     while being dynamically loaded.
     * @see org.griphyn.common.util.CommonProperties
     * @see #loadInstance()
     */
    public static ReplicaCatalog loadInstance(
            String catalogImplementor, PegasusBag bag, Properties connectProps)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {
        ReplicaCatalog result = null;

        if (catalogImplementor == null) {
            throw new NullPointerException("Invalid catalog implementor passed");
        }

        LogManager logger = bag.getLogger();
        if (logger == null) {
            throw new NullPointerException("Invalid Logger passed");
        }
        logger.log(
                "[Replica Factory] Connect properties detected for implementor "
                        + catalogImplementor
                        + " -> "
                        + connectProps,
                LogManager.DEBUG_MESSAGE_LEVEL);

        // File also means SimpleFile
        if (catalogImplementor.equalsIgnoreCase("File")) {
            catalogImplementor = FILE_CATALOG_IMPLEMENTOR;
        }

        // syntactic sugar adds absolute class prefix
        if (catalogImplementor.indexOf('.') == -1)
            catalogImplementor = DEFAULT_PACKAGE + "." + catalogImplementor;
        // POSTCONDITION: we have now a fully-qualified classname

        DynamicLoader dl = new DynamicLoader(catalogImplementor);
        result = (ReplicaCatalog) dl.instantiate(new Object[0]);
        if (result == null) throw new RuntimeException("Unable to load " + catalogImplementor);

        if (!result.connect(connectProps))
            throw new RuntimeException(
                    "Unable to connect to replica catalog implementation "
                            + catalogImplementor
                            + " with props "
                            + connectProps);

        // done
        return result;
    }

    public static ReplicaCatalog loadInstance(CommonProperties props) {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools |
        // Templates.
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
}
