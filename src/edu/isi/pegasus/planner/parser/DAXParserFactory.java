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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.CondorVersion;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.dax.*;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * A factory class to load the appropriate DAX Parser and Callback implementations that need to be
 * passed to the DAX Parser.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DAXParserFactory {

    /** The default callback for label partitioning. */
    public static String LABEL_CALLBACK_CLASS = "DAX2LabelGraph";

    /** Package to prefix "just" class names with. */
    public static final String DEFAULT_PARSER_PACKAGE_NAME = "edu.isi.pegasus.planner.parser.dax";

    /** Package to prefix "just" class names with. */
    public static final String DEFAULT_CALLBACK_PACKAGE_NAME = "edu.isi.pegasus.planner.parser.dax";

    /*
     * Predefined Constant for DAX version 3.2.0
     */
    public static final long DAX_VERSION_3_2_0 = CondorVersion.numericValue("3.2.0");

    /** The default DAXParser classname */
    public static final String DEFAULT_DAX_PARSER_CLASS = "DAXParser3";

    /** The DAXParser3 classname */
    public static final String DAX_PARSER2_CLASS = "DAXParser2";

    /** The DAXParser3 classname */
    public static final String DAX_PARSER3_CLASS = "DAXParser3";

    /**
     * Loads the appropriate DAXParser looking at the dax schema that is specified by the user.
     *
     * @param bag bag of Pegasus intialization objects
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

        // load the callback
        Callback c = DAXParserFactory.loadDAXParserCallback(bag, daxFile, callbackClass);
        return DAXParserFactory.loadXMLDAXParser(bag, c, daxFile);
    }

    /**
     * Loads the appropriate DAXParser looking at the dax schema that is specified in the DAX file.
     *
     * @param bag bag of Pegasus intialization objects
     * @param c the dax callback.
     * @param daxFile the dax file to parser
     * @return the DAXParser loaded.
     * @exception DAXParserFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     */
    public static DAXParser loadXMLDAXParser(PegasusBag bag, Callback c, String daxFile)
            throws DAXParserFactoryException {

        String daxClass = DAXParserFactory.DEFAULT_DAX_PARSER_CLASS;
        LogManager logger = bag.getLogger();
        PegasusProperties properties = bag.getPegasusProperties();
        String daxSchema = properties.getDAXSchemaLocation();

        // sanity check
        if (properties == null) {
            throw new RuntimeException("Invalid properties passed");
        }
        if (logger == null) {
            throw new RuntimeException("Invalid logger passed");
        }

        // try to figure out the schema version by parsing the dax file
        String schemaVersion = null;

        try {
            if (daxFile != null && !daxFile.isEmpty()) {
                Map m = getDAXMetadata(bag, daxFile);
                if (m.containsKey("version")
                        && (schemaVersion = (String) m.get("version")) != null) {

                    logger.log(
                            "DAX Version as determined from DAX file " + schemaVersion,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                }
            }

            // try to figure out the schema from the schema in properties
            // in case unable to determine from the dax file
            if (schemaVersion == null && daxSchema != null) {
                // try to determin the version of dax schema
                daxSchema = new File(daxSchema).getName();
                if (daxSchema.startsWith("dax-") && daxSchema.endsWith(".xsd")) {
                    schemaVersion =
                            daxSchema.substring(
                                    daxSchema.indexOf("dax-") + 4, daxSchema.lastIndexOf(".xsd"));

                    logger.log(
                            "DAX Version as determined from schema property " + schemaVersion,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                }
            }

            if (schemaVersion == null) {
                throw new DAXParserFactoryException(
                        "Unable to determine the DAX version from the DAX " + daxFile);
            }

            // append .0 to the version number
            // to be able to convert to numberic value
            if (CondorVersion.numericValue(schemaVersion + ".0")
                    < DAXParserFactory.DAX_VERSION_3_2_0) {
                daxClass = DAXParserFactory.DAX_PARSER2_CLASS;
            } else {
                daxClass = DAXParserFactory.DAX_PARSER3_CLASS;
            }

        } catch (Exception e) {
            logger.log(
                    "Problem while determining the version of dax",
                    e,
                    LogManager.ERROR_MESSAGE_LEVEL);
        }
        logger.log("DAX Parser Class to be loaded is " + daxClass, LogManager.CONFIG_MESSAGE_LEVEL);

        return loadDAXParser(daxClass, schemaVersion, bag, c);
    }

    /**
     * Loads the appropriate DAXParser looking at the dax schema that is specified by the user.
     *
     * @param classname the classname of the parser class that needs to be loaded
     * @param schemaVersion the schema version as determined from the DAX
     * @param bag bag of Pegasus intialization objects
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
            // load the DAX Parser class
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

            // set the callback for the DAX Parser
            ((DAXParser) daxParser).setDAXCallback(c);
        } catch (Exception e) {
            throw new DAXParserFactoryException("Instantiating DAXParser ", classname, e);
        }
        return daxParser;
    }

    /**
     * Loads the implementing class corresponding to the type specified by the user. The properties
     * object passed should not be null. The callback that is loaded, is the one referred to in the
     * properties by the user, unless the type of partitioning is label. In that case DAX2LabelGraph
     * is loaded always.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     * @param type the type of partitioning the user specified.
     * @param dax the path to the DAX file that has to be parsed.
     * @return the instance of the class implementing this interface.
     * @exception DAXParserFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_CALLBACK_PACKAGE_NAME
     * @see edu.isi.pegasus.planner.common.PegasusProperties#getPartitionerDAXCallback()
     */
    public static Callback loadDAXParserCallback(String type, PegasusBag bag, String dax)
            throws DAXParserFactoryException {

        String callbackClass = null;
        PegasusProperties properties = bag.getPegasusProperties();

        // for type label always load DAX2LabelGraph
        if (type.equalsIgnoreCase("label")) {
            callbackClass = LABEL_CALLBACK_CLASS; // graph with labels populated
        } else {
            // pick up the value passed in properties
            callbackClass = properties.getPartitionerDAXCallback();
        }

        return loadDAXParserCallback(bag, dax, callbackClass);
    }

    /**
     * Returns the metadata stored in the root adag element in the DAX
     *
     * @param bag the bag of initialization objects
     * @param dax the dax file.
     * @return Map containing the metadata, else an empty map
     */
    public static Map getDAXMetadata(PegasusBag bag, String dax) {
        Callback cb = DAXParserFactory.loadDAXParserCallback(bag, dax, "DAX2Metadata");

        LogManager logger = bag.getLogger();
        if (logger != null) {
            logger.log(
                    "Retrieving Metadata from the DAX file " + dax, LogManager.DEBUG_MESSAGE_LEVEL);
        }

        try {
            Parser p =
                    (Parser)
                            DAXParserFactory.loadDAXParser(
                                    DAXParserFactory.DAX_PARSER2_CLASS, "2.0", bag, cb);

            // while determining the metadata we are just parsing adag element
            // we want the parser validation to be turned off.
            p.setParserFeature("http://xml.org/sax/features/validation", false);
            p.setParserFeature("http://apache.org/xml/features/validation/schema", false);
            p.startParser(dax);
        } catch (RuntimeException e) {
            // check explicity for file not found exception
            if (e.getCause() != null && e.getCause() instanceof java.io.IOException) {
                // rethrow
                throw e;
            }
        }

        Map result = (Map) cb.getConstructedObject();
        return (result == null) ? new HashMap() : result;
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
