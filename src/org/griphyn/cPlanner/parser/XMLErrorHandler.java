/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.cPlanner.parser;

import org.griphyn.cPlanner.common.LogManager;

import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * This class handles the errors which occur while enforcing validation against
 * the XML Schema. Same as the VDLErrorHandler.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * $Revision$
 */

public class XMLErrorHandler implements ErrorHandler{

    /**
     * The handle to the logging object that is used to log the messages.
     */
    private LogManager mLogger ;

    /**
     * The default constructor.
     * Initializes the logger object.
     */
    public XMLErrorHandler(){
        mLogger = LogManager.getInstance();
    }

    /**
     * Logs the warning messages in the SAX parser generates while
     * validating the XML file against an XML Schema.
     *
     * @param e  the execption that is being caught.
     */
    public void warning (SAXParseException e) throws SAXException{
	  mLogger.log("**Parsing **  " +
                      " Line: " +
                      e.getLineNumber() + "\n" +
                      "[" + e + "]\n",LogManager.WARNING_MESSAGE_LEVEL);
      }


      /**
       * Logs the error messages which SAX parser generates while
       * validating the XML file against an XML Schema
       *
       * @param e the exception that is being caught.
       */
      public void error(SAXParseException e) throws SAXException {
	  mLogger.log("**Parsing **  " +
                      " Line: " +
                      e.getLineNumber() + "\n" +
                      "[" + e + "]\n",LogManager.ERROR_MESSAGE_LEVEL);
      }

      /**
       * Logs the the fatal messages which SAX parser generates while
       * validating the XML file against an XML Schema
       *
       * @param e the exception that is being caught
       */
      public void fatalError(SAXParseException e) throws SAXException{
	  mLogger.log("\n** Parsing **  " +
                      " Line: " +
                      e.getLineNumber() + "\n" +
                      "[" + e + "]\n",LogManager.FATAL_MESSAGE_LEVEL);
       }
}