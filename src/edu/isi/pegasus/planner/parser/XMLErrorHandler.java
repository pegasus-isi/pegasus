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
import edu.isi.pegasus.common.logging.LogManagerFactory;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * This class handles the errors which occur while enforcing validation against the XML Schema. Same
 * as the VDLErrorHandler.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *     <p>$Revision$
 */
public class XMLErrorHandler implements ErrorHandler {

    /** The handle to the logging object that is used to log the messages. */
    private LogManager mLogger;

    /** The default constructor. Initializes the logger object. */
    public XMLErrorHandler() {
        mLogger = LogManagerFactory.loadSingletonInstance();
    }

    /**
     * Logs the warning messages in the SAX parser generates while validating the XML file against
     * an XML Schema.
     *
     * @param e the execption that is being caught.
     */
    public void warning(SAXParseException e) throws SAXException {
        mLogger.log(
                "**Parsing **  " + " Line: " + e.getLineNumber() + "\n" + "[" + e + "]\n",
                LogManager.WARNING_MESSAGE_LEVEL);
    }

    /**
     * Logs the error messages which SAX parser generates while validating the XML file against an
     * XML Schema
     *
     * @param e the exception that is being caught.
     */
    public void error(SAXParseException e) throws SAXException {
        mLogger.log(
                "**Parsing **  " + " Line: " + e.getLineNumber() + "\n" + "[" + e + "]\n",
                LogManager.ERROR_MESSAGE_LEVEL);
    }

    /**
     * Logs the the fatal messages which SAX parser generates while validating the XML file against
     * an XML Schema
     *
     * @param e the exception that is being caught
     */
    public void fatalError(SAXParseException e) throws SAXException {
        mLogger.log(
                "\n** Parsing **  " + " Line: " + e.getLineNumber() + "\n" + "[" + e + "]\n",
                LogManager.FATAL_MESSAGE_LEVEL);
    }
}
