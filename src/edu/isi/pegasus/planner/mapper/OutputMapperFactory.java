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
package edu.isi.pegasus.planner.mapper;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;

/**
 * The factory class that loads an appropriate Transfer OutputMapper class, as specified by the
 * properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class OutputMapperFactory {

    /** The default package where the implementations reside, which this factory loads. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.planner.mapper.output";

    /** The prefix for the property subset for connecting to the individual catalogs. */
    public static final String PROPERTY_KEY = "pegasus.dir.storage.mapper";

    /** The default mapper implementation that is picked up. */
    public static final String DEFAULT_OUTPUT_MAPPER_IMPLEMENTATION = "Flat";

    /** The Hashed mapper implementation to be used. */
    public static final String HASHED_OUTPUT_MAPPER_IMPLEMENTATION = "Hashed";

    /**
     * Loads the implementing class corresponding to the value specified in the properties. If the
     * package name is not specified with the class, then class is assumed to be in the
     * DEFAULT_PACKAGE. The properties object passed should not be null.
     *
     * <p>In addition it ends up loading the appropriate Transfer Implementation that is required by
     * the mapper.
     *
     * @param dag the workflow that is being refined.
     * @param bag the bag of initialization objects
     * @return the instance of the class implementing this interface.
     * @exception TransferOutputMapperException that nests any error that might occur during the
     *     instantiation.
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static OutputMapper loadInstance(ADag dag, PegasusBag bag)
            throws OutputMapperFactoryException {

        PegasusProperties props = bag.getPegasusProperties();
        if (props == null) {
            throw new OutputMapperFactoryException("Null Properties passed in the bag ");
        }

        String className = props.getProperty(PROPERTY_KEY); // rely on the new mapper property

        if (className == null && props.useDeepStorageDirectoryStructure()) {
            // if the legacy property is specified, use that to set to HashedMapper
            className = HASHED_OUTPUT_MAPPER_IMPLEMENTATION;
        }

        // fall back to default if not determined from properties
        className = (className == null) ? DEFAULT_OUTPUT_MAPPER_IMPLEMENTATION : className;

        return loadInstance(className, bag, dag);
    }

    /**
     * Loads the implementing class corresponding to the class. If the package name is not specified
     * with the class, then class is assumed to be in the DEFAULT_PACKAGE. The properties object
     * passed should not be null. In addition it ends up loading the appropriate Transfer
     * Implementation that is required by the mapper.
     *
     * @param className the name of the class that implements the mode.It can or cannot be with the
     *     package name.
     * @param bag the bag of initialization objects
     * @param dag the workflow that is being refined.
     * @return the instance of the class implementing this interface.
     * @exception OutputMapperFactoryException that nests any error that might occur during the
     *     instantiation.
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static OutputMapper loadInstance(String className, PegasusBag bag, ADag dag)
            throws OutputMapperFactoryException {

        OutputMapper mapper = null;
        try {
            // sanity check
            if (bag.getPegasusProperties() == null) {
                throw new RuntimeException("Invalid properties passed");
            }
            if (dag == null) {
                throw new RuntimeException("Invalid workflow passed");
            }

            // set the mapper to default if required
            if (className == null) {
                className = OutputMapperFactory.DEFAULT_OUTPUT_MAPPER_IMPLEMENTATION;
            }

            // prepend the package name
            className =
                    (className.indexOf('.') == -1)
                            ?
                            // pick up from the default package
                            DEFAULT_PACKAGE_NAME + "." + className
                            :
                            // load directly
                            className;

            // try loading the class dynamically
            DynamicLoader dl = new DynamicLoader(className);
            mapper = (OutputMapper) dl.instantiate(new Object[0]);

            // we got the mapper try to load the appropriate
            // transfer implementation also
            mapper.initialize(bag, dag);
        } catch (Exception e) {
            throw new OutputMapperFactoryException("Instantiating Output Mapper", className, e);
        }

        return mapper;
    }
}
