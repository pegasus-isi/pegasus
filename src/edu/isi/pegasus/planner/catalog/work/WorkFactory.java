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
package edu.isi.pegasus.planner.catalog.work;

import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.catalog.WorkCatalog;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.Enumeration;
import java.util.Properties;

/**
 * This factory loads a work catalog, as specified by the properties. Each invocation of the factory
 * will result in a new instance of a connection to the replica catalog.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @version $Revision: 50 $
 * @see org.griphyn.common.catalog.WorkCatalog
 */
public class WorkFactory {

    /** Package to prefix "just" class names with. */
    public static final String DEFAULT_PACKAGE = "edu.isi.pegasus.planner.catalog.work";

    /**
     * Connects the interface with the work catalog implementation. The choice of backend is
     * configured through properties. This class is useful for non-singleton instances that may
     * require changing properties.
     *
     * @param props is an instance of properties to use.
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
     */
    public static WorkCatalog loadInstance(PegasusProperties props) throws WorkFactoryException {

        return loadInstance(props.getVDSProperties());
    }

    /**
     * Connects the interface with the work catalog implementation. The choice of backend is
     * configured through properties. This class is useful for non-singleton instances that may
     * require changing properties.
     *
     * @param props is an instance of properties to use.
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
     */
    public static WorkCatalog loadInstance(CommonProperties props) throws WorkFactoryException {
        // sanity check
        if (props == null) throw new NullPointerException("invalid properties");

        Properties connect = props.matchingSubset(WorkCatalog.c_prefix, false);

        // get the default db driver properties in first pegasus.catalog.*.db.driver.*
        Properties db = props.matchingSubset(WorkCatalog.DB_ALL_PREFIX, false);
        // now overload with the work catalog specific db properties.
        // pegasus.catalog.work.db.driver.*
        db.putAll(props.matchingSubset(WorkCatalog.DB_PREFIX, false));

        // to make sure that no confusion happens.
        // add the db prefix to all the db properties
        for (Enumeration e = db.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            connect.put("db." + key, db.getProperty(key));
        }

        // put the driver property back into the DB property
        //       String driver = props.getProperty( WorkCatalog.DBDRIVER_PREFIX );
        //       if( driver == null ){ driver = props.getProperty( WorkCatalog.DBDRIVER_ALL_PREFIX
        // ); }
        //       connect.put( "db.driver", driver );

        // determine the class that implements the work catalog
        return loadInstance(props.getProperty(WorkCatalog.c_prefix), connect);
    }

    /**
     * Connects the interface with the work catalog implementation. The choice of backend is
     * configured through properties. This class is useful for non-singleton instances that may
     * require changing properties.
     *
     * @param props is an instance of properties to use.
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
     */
    private static WorkCatalog loadInstance(String catalogImplementor, Properties props)
            throws WorkFactoryException {
        WorkCatalog result = null;

        try {
            if (catalogImplementor == null)
                throw new RuntimeException(
                        "You need to specify the " + WorkCatalog.c_prefix + " property");

            // syntactic sugar adds absolute class prefix
            if (catalogImplementor.indexOf('.') == -1)
                catalogImplementor = DEFAULT_PACKAGE + "." + catalogImplementor;
            // POSTCONDITION: we have now a fully-qualified classname

            DynamicLoader dl = new DynamicLoader(catalogImplementor);
            result = (WorkCatalog) dl.instantiate(new Object[0]);

            if (!result.connect(props))
                throw new RuntimeException("Unable to connect to work catalog implementation");
        } catch (Exception e) {
            throw new WorkFactoryException(
                    " Unable to instantiate Work Catalog ", catalogImplementor, e);
        }

        // done
        return result;
    }
}
