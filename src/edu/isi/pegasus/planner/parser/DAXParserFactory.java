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
package edu.isi.pegasus.planner.parser;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.dax.*;

import java.util.Map;

/**
 * A factory class to load the appropriate DAX XMLParser and Callback implementations that need to
 * be passed to the DAX XMLParser.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DAXParserFactory {

    /** The default callback for label partitioning. */
    public static String LABEL_CALLBACK_CLASS = "DAX2LabelGraph";

    /** The default callback for planner use. */
    public static String DEFAULT_CALLBACK_CLASS = "DAX2CDAG";

    /** Package to prefix "just" class names with. */
    public static final String DEFAULT_PARSER_PACKAGE_NAME = "edu.isi.pegasus.planner.parser.dax";

    /** Package to prefix "just" class names with. */
    public static final String DEFAULT_CALLBACK_PACKAGE_NAME = "edu.isi.pegasus.planner.parser.dax";

    /** The YAML_DAX_PARSER_CLASS classname */
    public static final String YAML_DAX_PARSER_CLASS = "DAXParser5";

    /**
     * Loads the appropriate DAXParser looking at the dax schema that is specified by the user.
     *
     * @param bag bag of Pegasus initialization objects
     * @param callbackClass the dax callback class
     * @param daxFile
     * @return the DAXParser loaded.
     * @exception DAXParserFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     */
    public static DAXParser loadDAXParser(PegasusBag bag, String callbackClass, String daxFile)
            throws DAXParserFactoryException {

        PegasusProperties properties = bag.getPegasusProperties();

        // sanity check
        if (properties == null) {
            throw new RuntimeException("Invalid properties passed");
        }
        return loadDAXParser(
                bag, DAXParserFactory.loadDAXParserCallback(bag, daxFile, callbackClass), daxFile);
    }

    /**
     * Loads the appropriate DAXParser looking at the dax schema that is specified by the user.
     *
     * @param bag bag of Pegasus initialization objects
     * @param cb the dax callback class
     * @param daxFile
     * @return the DAXParser loaded.
     * @exception DAXParserFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     */
    public static DAXParser loadDAXParser(PegasusBag bag, Callback cb, String daxFile)
            throws DAXParserFactoryException {

        PegasusProperties properties = bag.getPegasusProperties();

        // sanity check
        if (properties == null) {
            throw new RuntimeException("Invalid properties passed");
        }

        return DAXParserFactory.loadDAXParser(YAML_DAX_PARSER_CLASS, "5.0", bag, cb);
    }

    /**
     * Loads the appropriate DAXParser looking at the dax schema that is specified by the user.
     *
     * @param classname the classname of the parser class that needs to be loaded
     * @param schemaVersion the schema version as determined from the DAX
     * @param bag bag of Pegasus initialization objects
     * @param c the DAX Callback to use
     * @return the DAXParser loaded.
     * @exception DAXParserFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     */
    private static final DAXParser loadDAXParser(
            String classname, String schemaVersion, PegasusBag bag, Callback c) {
        DAXParser daxParser = null;
        try {
            // load the DAX XMLParser class
            // prepend the package name
            String daxClass =
                    (classname.indexOf('.') == -1)
                            ?
                            // pick up from the default package
                            DEFAULT_PARSER_PACKAGE_NAME + "." + classname
                            :
                            // load directly
                            classname;

            DynamicLoader dl = new DynamicLoader(daxClass);
            Object argList[] = new Object[2];
            argList[0] = bag;
            argList[1] = schemaVersion;
            daxParser = (DAXParser) dl.instantiate(argList);

            // set the callback for the DAX XMLParser
            ((DAXParser) daxParser).setDAXCallback(c);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DAXParserFactoryException("Instantiating DAXParser ", classname, e);
        }
        return daxParser;
    }

    /**
     * Returns the metadata stored in the root adag element in the DAX
     *
     * @param bag the bag of initialization objects
     * @param dax the dax file.
     * @return Map containing the metadata, else an empty map
     */
    public static Map getDAXMetadata(PegasusBag bag, String dax) {
        return YAMLDAX2Metadata.getMetadata(bag, dax);
    }

    /**
     * Loads the implementing class corresponding to the type specified by the user. The properties
     * object passed should not be null. The callback that is loaded, is the one referred to by the
     * className parameter passed.
     *
     * @param bag the bag of initialization objects containing the logger and the properties handler
     * @param dax the path to the DAX file that has to be parsed.
     * @param className the name of the implementing class.
     * @return the instance of the class implementing this interface.
     * @exception DAXParserFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     */
    public static Callback loadDAXParserCallback(PegasusBag bag, String dax, String className)
            throws DAXParserFactoryException {

        // try loading the class dynamically
        Callback callback = null;

        try {
            // sanity check
            if (bag == null) {
                throw new RuntimeException("Invalid PegasusBag passed");
            }
            PegasusProperties properties = bag.getPegasusProperties();
            if (properties == null) {
                throw new RuntimeException("Invalid properties passed");
            }
            if (className == null) {
                throw new RuntimeException("Invalid class specified to load");
            }

            // prepend the package name
            className =
                    (className.indexOf('.') == -1)
                            ?
                            // pick up from the default package
                            DEFAULT_CALLBACK_PACKAGE_NAME + "." + className
                            :
                            // load directly
                            className;

            DynamicLoader dl = new DynamicLoader(className);
            Object argList[] = new Object[0];
            callback = (Callback) dl.instantiate(argList);
            callback.initialize(bag, dax);
        } catch (Exception e) {
            throw new DAXParserFactoryException("Instantiating DAXCallback ", className, e);
        }
        return callback;
    }
}
