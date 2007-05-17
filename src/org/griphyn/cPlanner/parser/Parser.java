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

import java.io.File;
import java.io.FileNotFoundException;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * This is the base class which all the xml parsing classes extend.
 * It initializes the xml parser namely Xerces, sets it's various features
 * like turning on validation against schema etc, plus the namespace resolution.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 *
 */

public abstract class Parser extends DefaultHandler{

    /**
     * Default parser name. Using Xerces at present.
     */
    protected final String DEFAULT_PARSER_NAME = "org.apache.xerces.parsers.SAXParser";

    /**
     * Locator object to determine on which line in the xml has the error
     * occured.
     */
    protected Locator mLocator;

    /**
     * Holds the text in an element (text between start and final tags if any).
     * Used in case of elements of mixed type.
     */
    protected StringBuffer mTextContent ;

    /**
     * The LogManager object which logs the Pegasus messages.
     */
    protected LogManager mLogger;

    /**
     * The String which contains the messages to be logged.
     */
    protected String mLogMsg;

    /**
     * The object which is used to parse the dax. This reads the XML document
     * and sends it to the event handlers.
     */
    protected XMLReader mParser = null;


    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;


    /**
     * A String that holds the contents of data passed as text. The string
     * should only be trimmed when the appropriate end tag of the element is
     * invoked. At this point, a whitespace is added if there are whitespaces in
     * at the ends.
     */
    protected String mTextString;


    /**
     * Boolean flag to ensure that two adjacent filenames are separated by a
     * whitespace.
     */
    protected boolean mAdjFName;

    /**
     * Intialises the parser. Sets the various features. However the parsing is
     * done in the implementing class, by call mParser.parse(filename).
     *
     * @param properties the <code>PegasusProperties</code> to be used.
     */
    public Parser( PegasusProperties properties ) {
        mTextContent  = new StringBuffer();
        mLogMsg       = new String();
        mLogger       = LogManager.getInstance();
        mProps        = properties;
        mTextString   = new String();
        mAdjFName     = false;
        mTextContent.setLength(0);
        createParserInstance();
    }


    /**
     * An empty implementation is provided by DefaultHandler of ContentHandler.
     * This method receives the notification from the sacks parser when start
     * tag of an element comes. Any parser class must implement this method.
     */
    public abstract void startElement(String uri, String local, String raw,
                            Attributes attrs) throws SAXException ;

    /**
     * An empty implementation is provided by DefaultHandler class. This method
     * is called automatically by the Sax parser when the end tag of an element
     * comes in the xml file. Any parser class should implement this method
     */
    public abstract void endElement(String uri,String localName,String qName);


    /**
     * This is called automatically when the end of the XML file is reached.
     */
    public abstract void endDocument();


    /**
     * Start the parser. This starts the parsing of the file by the parser.
     *
     * @param file  the path to the XML file you want to parse.
     */
    public abstract void startParser(String file);


    /**
     * Helps the load database to locate the VDLx XML schema, if available.
     * Please note that the schema location URL in the instance document
     * is only a hint, and may be overriden by the findings of this method.
     *
     * @return a location pointing to a definition document of the XML
     * schema that can read VDLx. Result may be null, if such a document
     * is unknown or unspecified.
     */
    public abstract String getSchemaLocation();



    /**
     * Sets the list of external real locations where the XML schema may be found.
     * Since this list can be determined at run-time through properties etc., we
     * expect this function to be called between instantiating the parser, and
     * using the parser
     *
     * @param list is a list of strings representing schema locations. The content
     * exists in pairs, one of the namespace URI, one of the location URL.
     *
     *
     */
    public void setSchemaLocations( String list ){
        /*
            // default place to add
            list += "http://www.griphyn.org/working_groups/VDS/vdl-1.19.xsd " +
              "http://www.griphyn.org/working_groups/VDS/vdl-1.19.xsd";
        */

       // schema location handling
       try {
           mParser.setProperty(
               "http://apache.org/xml/properties/schema/external-schemaLocation",
               list);
       }
       catch (SAXException se) {
           mLogger.log("The SAXParser reported an error: " + se.getMessage(),
                       LogManager.ERROR_MESSAGE_LEVEL);
       }
   }


    /**
     * This is used to store the character data that is in xml. An implementation
     * of the interface for the Sacks parser.
     */
    public void characters(char[] chars,int start,int length){

        //appending the buffer with chars. We use this way bec sacks parser can
        //parse internally the data any way they like

        //Very IMPORTANT
        String temp = new String(chars,start,length);

        /*if(temp.trim().length() > 0){
            mTextContent.append(temp);
        }*/
        temp = this.ignoreWhitespace(temp);
        mTextContent.append(temp);
        //set the adjacent flag to false
        mAdjFName     = false;


    }

    /**
     * Our own implementation for ignorable whitespace. A String that holds the
     * contents of data passed as text by the underlying parser. The whitespaces
     * at the end are replaced by one whitespace.
     *
     * @param str   The string that contains whitespaces.
     *
     * @return  String corresponding to the trimmed version.
     *
     */
    public String ignoreWhitespace(String str){
        boolean st = false;
        boolean end = false;
        int length = str.length();
        if(length > 0){
            //check for whitespace in the
            //starting
            if(str.charAt(0) == ' ' || str.charAt(0) == '\t' || str.charAt(0) == '\n'){
                st = true;
            }
            //check for whitespace in the end
            if(str.length() > 1 &&
               (str.charAt(length -1) == ' ' ||
                str.charAt(length -1) == '\t' ||
                str.charAt(length -1) == '\n')){

                end = true;
            }
            //trim the string and add a single whitespace accordingly
            str = str.trim();
            str = st == true ? ' ' + str:str;
            str = end == true ? str + ' ':str;
        }

        return str;
    }


    /**
     * Overrides the empty implementation provided by Default Handler and sets
     * the locator variable for the locator.
     *
     * @param loc  the Locator object which keeps the track as to the line
     *             numbers of the line being parsed.
    */
    public void setDocumentLocator(Locator loc) {
        this.mLocator = loc;
    }

    /**
     * Tests whether the file exists or not.
     */
    public void testForFile(String file) throws FileNotFoundException{

        File f = new File(file);
        if (!f.exists()){
            mLogMsg = "The file (" + file +" ) specified does not exist";
            throw new FileNotFoundException( mLogMsg );
        }
    }


    /**
     * Creates an instance of the parser, and sets the various options to it.
     */
    private void createParserInstance(){
        //creating a parser
        try{
            mParser = XMLReaderFactory.createXMLReader(DEFAULT_PARSER_NAME);
        }
        catch(Exception e){
            throw new RuntimeException( "Unable to create XMLReader" + e.getMessage() , e );
        }

        //setting the handlers The class extend DefaultHandler which provides
        //for a empty implemetnation of the four handlers
        mParser.setContentHandler(this);
        mParser.setErrorHandler(new XMLErrorHandler());

        try{
            //setting the feature that xml should be validated against the
            //xml schema specified in it

            setParserFeature("http://xml.org/sax/features/validation", true);
            setParserFeature("http://apache.org/xml/features/validation/schema", true);

            //should be set only for debugging purposes
            //setParserFeature("http://apache.org/xml/features/validation/schema-full-checking", true);

            setParserFeature("http://apache.org/xml/features/validation/dynamic", true);
            setParserFeature("http://apache.org/xml/features/validation/warn-on-duplicate-attdef", true);

            //fails with the new xerces
            //setParserFeature("http://apache.org/xml/features/validation/warn-on-undeclared-elemdef", true);

            setParserFeature("http://apache.org/xml/features/warn-on-duplicate-entitydef", true);
            setParserFeature("http://apache.org/xml/features/validation/schema/element-default", true);

        }
        catch (Exception e){
            //if a locator error then
            if(mLocator != null){
                String message = "Error in " + mLocator.getSystemId() + " at line " +
                                  mLocator.getLineNumber() + " at column " +
                                  mLocator.getColumnNumber() + e.getMessage();
                mLogger.log(message,LogManager.ERROR_MESSAGE_LEVEL );

            }
            mLogger.log(e.getMessage(),LogManager.ERROR_MESSAGE_LEVEL);
        }

    }



    /**
     * Sets a parser feature, and fails here enabling us to set all the following
     * features.
     *
     * @param uri is the feature's URI to modify
     * @param flag is the new value to set.
     * @return true if the feature could be set, else false for
     *         an exception.
     */
    public boolean setParserFeature( String uri, boolean flag){
        boolean result = false;
        try{
            this.mParser.setFeature(uri,flag);
            result = true;
        }
        catch (SAXException se){
            mLogger.log("Unable to set parser feature " + uri + " :" +
                        se.getMessage(),LogManager.ERROR_MESSAGE_LEVEL);
        }
        return result;
    }

}
