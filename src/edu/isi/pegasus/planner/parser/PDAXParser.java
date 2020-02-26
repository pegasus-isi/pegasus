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
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.pdax.Callback;
import edu.isi.pegasus.planner.partitioner.Partition;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * This is a parser class for the parsing the pdax that contain the jobs in the various partitions
 * and the relations between the partitions.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PDAXParser extends Parser {

    /** The "not-so-official" location URL of the DAX schema definition. */
    public static final String SCHEMA_LOCATION = "http://pegasus.isi.edu/schema/pdax-2.0.xsd";

    /** URI namespace */
    public static final String SCHEMA_NAMESPACE = "http://pegasus.isi.edu/schema/PDAX";

    /** The object holding the contents of one partition as indicated in the pdax. */
    private Partition mPartition;

    /** The current depth of parsing through the xml structure. */
    private int mCurrentDepth;

    /** The current child. */
    private String mChild;

    /** List of parents for a particular child. */
    private List mParents;

    /**
     * The callback handler to which the callbacks are sent during designated points of parsing the
     * pdax.
     */
    private Callback mCallback;

    /**
     * The default constructor.
     *
     * @param properties the <code>PegasusProperties</code> to be used.
     */
    public PDAXParser(PegasusProperties properties) {
        super(properties);
        // intialize to null every member variable
        mPartition = null;
        mCurrentDepth = 0;
        mCallback = null;
    }

    /**
     * The constructor initialises the parser, and turns on the validation feature in Xerces.
     *
     * @param fileName the file which one has to parse using the parser.
     * @param properties the <code>PegasusProperties</code> to be used.
     */
    public PDAXParser(String fileName, PegasusProperties properties) {
        super(properties);
        mCurrentDepth = 0;

        try {
            this.testForFile(fileName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        mCallback = null;

        // set the schema location against which
        // to validate.
        String schemaLoc = getSchemaLocation();
        mLogger.log("Picking schema " + schemaLoc, LogManager.CONFIG_MESSAGE_LEVEL);
        String list = PDAXParser.SCHEMA_NAMESPACE + " " + schemaLoc;
        setSchemaLocations(list);
    }

    /**
     * Returns the XML schema namespace that a document being parsed conforms to.
     *
     * @return the schema namespace
     */
    public String getSchemaNamespace() {
        return PDAXParser.SCHEMA_NAMESPACE;
    }

    /** Sets the callback handler for this parsing instance. */
    public void setCallback(Callback callback) {
        mCallback = callback;
    }

    /**
     * Ends up starting the parsing of the file , by the underlying parser.
     *
     * @param file the path/url to the file that needs to be parsed.
     */
    public void startParser(String file) {
        try {
            mParser.parse(file);
        } catch (FactoryException fe) {
            // throw it as it is for time being
            throw fe;
        } catch (Exception e) {
            String message;
            // if a locator error then
            if (mLocator != null) {
                message =
                        "Parsing Error in "
                                + mLocator.getSystemId()
                                + " at line "
                                + mLocator.getLineNumber()
                                + " at column "
                                + mLocator.getColumnNumber()
                                + " : ";

            } else {
                message = "Parsing the PDAX file ";
                mLogger.log(message, LogManager.ERROR_MESSAGE_LEVEL);
            }
            throw new RuntimeException(message, e);
        }
    }

    /**
     * An empty implementation is provided by DefaultHandler of ContentHandler. This method receives
     * the notification from the sacks parser when start tag of an element comes. Any parser class
     * must implement this method.
     */
    public void startElement(String uri, String local, String raw, Attributes attrs)
            throws SAXException {

        String key;
        String value;
        int i = 0;

        // new element increment the depth
        mCurrentDepth++;

        if (local.equals("pdag")) {
            HashMap mp = new HashMap();

            for (i = 0; i < attrs.getLength(); i++) {
                key = attrs.getLocalName(i);
                value = attrs.getValue(i);
                // should probably check for valid attributes before setting
                mp.put(key, value);
                // System.out.println(key + " --> " + value);
            }
            // call the callback interface
            mCallback.cbDocument(mp);

            return;
        } else if (local.equals("partition")) {
            mPartition = new Partition();
            for (i = 0; i < attrs.getLength(); i++) {
                key = attrs.getLocalName(i);
                value = attrs.getValue(i);

                // check for valid attributes before setting
                if (key.equals("name")) {
                    mPartition.setName(value);
                } else if (key.equals("id")) {
                    mPartition.setID(value);
                } else if (key.equals("index")) {
                    int index = -1;
                    // try convert the String to int
                    try {
                        index = Integer.parseInt(value);
                    } catch (Exception e) {
                        invalidValue(local, key, value);
                    }
                    mPartition.setIndex(index);
                } else {
                    invalidAttribute(local, key, value);
                }
                // System.out.println(key + " --> " + value);
            }
            return;
        } else if (local.equals("job")) {
            String name = null;
            String id = null;
            GraphNode job;

            for (i = 0; i < attrs.getLength(); i++) {
                key = attrs.getLocalName(i);
                value = attrs.getValue(i);

                // check for valid attributes before setting
                if (key.equals("name")) {
                    name = value;
                } else if (key.equals("id")) {
                    id = value;
                } else {
                    // complain about invalid key
                    invalidAttribute(local, key, value);
                }
            }
            job = new GraphNode(id, name);
            // add it to the partition
            mPartition.addNode(job);
            return;
        } else if (local.equals("child")) {
            // we do not know how many parents it has
            mParents = new java.util.LinkedList();
            for (i = 0; i < attrs.getLength(); i++) {
                key = attrs.getLocalName(i);
                value = attrs.getValue(i);
                if (key.equals("ref")) {
                    mChild = value;
                } else {
                    invalidAttribute(local, key, value);
                }
            }

            return;
        } else if (local.equals("parent")) {
            for (i = 0; i < attrs.getLength(); i++) {
                key = attrs.getLocalName(i);
                value = attrs.getValue(i);
                if (key.equals("ref")) {
                    mParents.add(value);
                } else {
                    invalidAttribute(local, key, value);
                }
            }

            return;

        } else {
            mLogger.log("No implementation for element " + local, LogManager.ERROR_MESSAGE_LEVEL);

            throw new RuntimeException("No implementation for element " + local);
        }
    }

    /**
     * An empty implementation is provided by DefaultHandler class. This method is called
     * automatically by the Sax parser when the end tag of an element comes in the xml file. Any
     * parser class should implement this method
     */
    public void endElement(String uri, String local, String qName) {
        // decrement the depth of parsing
        mCurrentDepth--;

        if (local.equals("pdag")) {
            // call the callback interface
            return;
        } else if (local.equals("partition")) {
            // call the callback interface
            mCallback.cbPartition(mPartition);
            // cleanup the object
            mPartition = null;
        } else if (local.equals("child")) {
            // check if it was nested in partition element
            // or the pdag element
            if (mCurrentDepth == 2) {
                // means the put the child and parents in partition
                mPartition.addParents(mChild, mParents);
            } else if (mCurrentDepth == 1) {
                // need to call the callback interface
                mCallback.cbParents(mChild, mParents);
            } else {
                throw new RuntimeException("Wrongly formed xml");
            }
        } else if (local.equals("parent") || local.equals("job")) {
            // do nothing
            return;
        } else {
            // end of invalid element.
            // non reachable line???
            mLogMsg = "End of invalid element reached " + local;
            mLogMsg =
                    (mLocator == null)
                            ? mLogMsg
                            :
                            // append the locator information
                            mLogMsg
                                    + " at line "
                                    + mLocator.getLineNumber()
                                    + " at column "
                                    + mLocator.getColumnNumber();

            throw new RuntimeException(mLogMsg);
        }
    }

    /** This is called automatically when the end of the XML file is reached. */
    public void endDocument() {
        // do a sanity check
        if (mCurrentDepth != 0) {
            mLogger.log(
                    "It seems that the xml was not well formed!!", LogManager.ERROR_MESSAGE_LEVEL);
        }
        // call the callback interface
        mCallback.cbDone();
    }

    /**
     * Helps the load database to locate the PDAX XML schema, if available. Please note that the
     * schema location URL in the instance document is only a hint, and may be overriden by the
     * findings of this method.
     *
     * @return a location pointing to a definition document of the XML schema that can read PDAX.
     *     Result may be null, if such a document is unknown or unspecified.
     */
    public String getSchemaLocation() {
        String child = new File(this.SCHEMA_LOCATION).getName();
        File pdax = // create a pointer to the default local position
                new File(this.mProps.getSysConfDir(), child);

        // System.out.println("\nDefault Location of PDAX is " + pdax.getAbsolutePath());

        // Nota bene: vds.schema.dax may be a networked URI...
        return this.mProps.getPDAXSchemaLocation(pdax.getAbsolutePath());
    }

    /**
     * Logs a message if an unknown key is come across, while parsing the xml document.
     *
     * @param element the xml element in which the invalid key was come across.
     * @param key the key that is construed to be invalid.
     * @param value the value associated with the key.
     */
    private void invalidAttribute(String element, String key, String value) {
        String message =
                "Invalid attribute " + key + "found in " + element + " with value " + value;
        message =
                (mLocator == null)
                        ? message
                        :
                        // append the locator information
                        message
                                + " at line "
                                + mLocator.getLineNumber()
                                + " at column "
                                + mLocator.getColumnNumber();

        mLogger.log(message, LogManager.WARNING_MESSAGE_LEVEL);
    }

    /**
     * Logs a message if an unknown value is come across, while parsing the xml document.
     *
     * @param element the xml element in which the invalid key was come across.
     * @param key the key that is construed to be invalid.
     * @param value the value associated with the key.
     */
    private void invalidValue(String element, String key, String value) {
        String message =
                "Invalid value " + value + "found in " + element + " for attribute " + value;
        message =
                (mLocator == null)
                        ? message
                        :
                        // append the locator information
                        message
                                + " at line "
                                + mLocator.getLineNumber()
                                + " at column "
                                + mLocator.getColumnNumber();

        mLogger.log(message, LogManager.WARNING_MESSAGE_LEVEL);
    }
}
