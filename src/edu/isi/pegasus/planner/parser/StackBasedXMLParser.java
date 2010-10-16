/*
 * 
 *   Copyright 2007-2008 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */

package edu.isi.pegasus.planner.parser;


import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;


import java.util.List;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * An abstract base class that XML Parsers can use if they use stack internally
 * to store the elements encountered while parsing XML documents using SAX
 * 
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public abstract class StackBasedXMLParser extends Parser {


    /**
    * Count the depths of elements in the document
    */
    protected int mDepth;
    
    /**
     * The stack of objects kept around.
     */
    protected Stack mStack;


    /**
     * A boolean indicating that parsing is done.
     */
    protected boolean mParsingDone;


    /**
     * The default Constructor.
     * 
     *
     */
    public StackBasedXMLParser(  ) {
        this( PegasusProperties.nonSingletonInstance() );
    }
    
    
    /**
     * The overloaded constructor.
     *
     * @param properties the <code>PegasusProperties</code> to be used.
     */
    public StackBasedXMLParser( PegasusProperties properties  ) {
        super( properties );
        mStack = new Stack();
        mDepth = 0;
        
       
        //setting the schema Locations
        String schemaLoc = getSchemaLocation();
        mLogger.log( "Picking schema for site catalog" + schemaLoc,
                     LogManager.CONFIG_MESSAGE_LEVEL);
        String list = getSchemaNamespace( ) + " " + schemaLoc;
        setSchemaLocations( list );
    }




    /**
     * 
     */
    public void endDocument() {
        mParsingDone = true;
    }

    

    
     /**
      * This method defines the action to take when the parser begins to parse
      * an element.
      *
      * @param namespaceURI is the URI of the namespace for the element
      * @param localName is the element name without namespace
      * @param qName is the element name as it appears in the docment
      * @param atts has the names and values of all the attributes
      */
    public void startElement( String namespaceURI,
                              String localName,
                              String qName,
                              Attributes atts ) throws SAXException{
        
       

        //one more element level
        mDepth++;

        List names = new java.util.ArrayList();
        List values = new java.util.ArrayList();
        for ( int i=0; i < atts.getLength(); ++i ) {
            String name = new String( atts.getLocalName(i) );
            String value = new String( atts.getValue(i) );            
            names.add(name);
            values.add(value);
        }

        //System.out.println( "QNAME " + qName + " NAME " + names + "\t Values" + values );

        Object object = createObject( qName, names, values );
        if ( object != null ){
            mStack.push( new ParserStackElement( qName, object ) );
        }
        else{
            mLogger.log(
                    "Unknown element in xml :" + namespaceURI + ":" +
                    localName + ":" + qName, LogManager.ERROR_MESSAGE_LEVEL );
            
            throw new SAXException( "Unknown or Empty element while parsing" );
        }
    }

    /**
     * The parser is at the end of an element. Triggers the association of
     * the child elements with the appropriate parent elements.
     *
     * @param namespaceURI is the URI of the namespace for the element
     * @param localName is the element name without namespace
     * @param qName is the element name as it appears in the docment
     */  
    public void endElement( String namespaceURI,
                            String localName,
                            String qName )   throws SAXException{

        // that's it for this level
        mDepth--;
        mLogger.log( "</" +  localName + "> at " +
                     this.mLocator.getLineNumber() + ":" +
                     mLocator.getColumnNumber() , LogManager.DEBUG_MESSAGE_LEVEL );

        ParserStackElement tos = ( ParserStackElement ) mStack.pop();
        if ( ! qName.equals( tos.getElementName() ) ) {
            String error = "Top of Stack " + tos.getElementName() + " does not mactch " + qName;
            mLogger.log( error,
                         LogManager.FATAL_MESSAGE_LEVEL );
            throw new SAXException( error );
        }

        if ( ! mStack.empty() ) {
            // add pieces to lower levels
            ParserStackElement peek = ( ParserStackElement ) mStack.peek();
            
            if ( !setElementRelation( tos.getElementName(), peek.getElementObject(), tos.getElementObject() )){
                    mLogger.log( "Element " + tos.getElementName() +
                     		  " does not fit into element " + peek.getElementName(),
                                  LogManager.DEBUG_MESSAGE_LEVEL );
            }
            
        } else {
          // run finalizer, if available
          mLogger.log( "End of last element reached ",
                        LogManager.DEBUG_MESSAGE_LEVEL );
        }
        //reinitialize our cdata handler at end of each element
        mTextContent.setLength( 0 );    
  }
   


    /**
     * 
     * @param element
     * @param attribute
     * @param value
     */
    public void log( String element, String attribute, String value) {
        //to be enabled when logging per queue.
        mLogger.log( "For element " + element + " found " + attribute + " -> " + value,
                     LogManager.DEBUG_MESSAGE_LEVEL );
    }
    
    /**
     * This is called when an attribute is encountered for an element that is invalid
     * from the schema context and is not supported.
     *
     * @param element    the element name
     * @param attribute  the attribute name
     * @param value      the attribute value
     */
    public void complain(String element, String attribute, String value) {
        mLogger.log( "For element " + element + " invalid attribute found " + attribute + " -> " + value,
                     LogManager.ERROR_MESSAGE_LEVEL );
    }

    /**
     * This is called when an attribute is encountered for an element that is valid
     * in the schema context but not supported right now.
     *
     * @param element    the element name
     * @param attribute  the attribute name
     * @param value      the attribute value
     */
    public void attributeNotSupported(String element, String attribute, String value) {
        mLogger.log( "For element " + element + " attribute currently not supported " + attribute + " -> " + value,
                     LogManager.WARNING_MESSAGE_LEVEL );
    }

    /**
     * Called when certain element nesting is allowed in the XML schema
     * but is not supported in the code as yet.
     *
     * @param parent  parent element
     * @param child   child element
     */
    public void unSupportedNestingOfElements(String parent, String child ) {
        StringBuffer sb = new StringBuffer();
        sb.append( "Support for element " ).append( child ).
           append( " in parent element " ).append( parent ).
           append( " not supported " );
        mLogger.log( sb.toString(), LogManager.WARNING_MESSAGE_LEVEL  );
    }


}

