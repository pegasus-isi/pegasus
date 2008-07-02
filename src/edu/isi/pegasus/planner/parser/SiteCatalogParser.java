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

import edu.isi.pegasus.planner.catalog.classes.Architecture;
import edu.isi.pegasus.planner.catalog.classes.OS;

import edu.isi.pegasus.planner.catalog.site.classes.SiteData;
import edu.isi.pegasus.planner.catalog.site.classes.Connection;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.HeadNodeFS;
import edu.isi.pegasus.planner.catalog.site.classes.HeadNodeScratch;
import edu.isi.pegasus.planner.catalog.site.classes.HeadNodeStorage;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.LocalDirectory;
import edu.isi.pegasus.planner.catalog.site.classes.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SharedDirectory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.WorkerNodeFS;
import edu.isi.pegasus.planner.catalog.site.classes.WorkerSharedDirectory;
import edu.isi.pegasus.planner.catalog.site.classes.WorkerNodeStorage;
import edu.isi.pegasus.planner.catalog.site.classes.WorkerNodeScratch;

import org.griphyn.cPlanner.namespace.Namespace;
import org.griphyn.cPlanner.parser.Parser;


import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import java.io.File;
import java.io.IOException;

import java.util.List;
import java.util.Stack;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * The parser for parsing the new Site Catalog schema ( v3.0 )
 * 
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public class SiteCatalogParser extends Parser {

    /**
     * The "not-so-official" location URL of the Site Catalog Schema.
     */
    public static final String SCHEMA_LOCATION =
                                        "http://pegasus.isi.edu/schema/sc-3.0.xsd";

    /**
     * uri namespace
     */
    public static final String SCHEMA_NAMESPACE =
                                        "http://pegasus.isi.edu/schema/sitecatalog";

    /**
    * Count the depths of elements in the document
    */
    private int mDepth;
    
    /**
     * The stack of objects kept around.
     */
    private Stack mStack;

    /**
     * Default Class Constructor.
     *
     * @param properties the <code>PegasusProperties</code> to be used.
     */
    public SiteCatalogParser( PegasusProperties properties ) {
        super( properties );
        mStack = new Stack();
        mDepth = 0;
    }

    /**
     * Class Constructor intializes the parser and turns on validation.
     *
     * @param configFileName The file which you want to parse
     * @param properties the <code>PegasusProperties</code> to be used.
     */
    public SiteCatalogParser( String configFileName, PegasusProperties properties ) {
        this( properties );

        mLogger.log("Parsing the site catalog " + configFileName, LogManager.INFO_MESSAGE_LEVEL);

        //setting the schema Locations
        String schemaLoc = getSchemaLocation();
        mLogger.log( "Picking schema for site catalog" + schemaLoc,
                     LogManager.CONFIG_MESSAGE_LEVEL);
        String list = SiteCatalogParser.SCHEMA_NAMESPACE + " " + schemaLoc;
        setSchemaLocations( list );
        startParser( configFileName );
        mLogger.logCompletion("Parsing the site catalog",LogManager.INFO_MESSAGE_LEVEL);

    }

    /**
     * The main method that starts the parsing.
     * 
     * @param file   the XML file to be parsed.
     */
    public void startParser( String file ) {
        try {
            this.testForFile( file );
            mParser.parse( file );
        } catch ( IOException ioe ) {
            mLogger.log( "IO Error :" + ioe.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL );
        } catch ( SAXException se ) {

            if ( mLocator != null ) {
                mLogger.log( "Error in " + mLocator.getSystemId() +
                    " at line " + mLocator.getLineNumber() +
                    "at column " + mLocator.getColumnNumber() + " :" +
                    se.getMessage() , LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
    }

    /**
     * 
     */
    public void endDocument() {

    }

    public void endElement( String uri, String localName, String qName ) {
            if ( true ){
            } else {
                mLogger.log(
                    "Unkown element end reached :" + uri + ":" +
                    localName + ":" + qName + "-******" + mTextContent +
                    "***********", LogManager.ERROR_MESSAGE_LEVEL );
                mTextContent.setLength( 0 );
            }

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
        
       /* to be added later when logging is fixed.
        mLogger.log( "parser", 3,
	       "<" + map(namespaceURI) + localName + "> at " +
	       m_location.getLineNumber() + ":" +
	       m_location.getColumnNumber() );
         */

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

        System.out.println( "QNAME " + qName + " NAME " + names + "\t Values" + values );

        Object object = createObject( qName, names, values );
        if ( object != null ){
            //mStack.push( new IVSElement( qName, object ) );
        }
        else{
            mLogger.log(
                    "Unknown element in xml :" + namespaceURI + ":" +
                    localName + ":" + qName, LogManager.ERROR_MESSAGE_LEVEL );
            
            throw new SAXException( "Unknown or Empty element while parsing" );
        }
    }

   
    
    /**
     * Composes the  <code>SiteData</code> object corresponding to the element
     * name in the XML document.
     * 
     * @param element the element name encountered while parsing.
     * @param names   is a list of attribute names, as strings.
     * @param values  is a list of attribute values, to match the key list.
     * 
     * @return the relevant SiteData object, else null if unable to construct.
     * 
     * @exception IllegalArgumentException if the element name is too short.
     */
    private Object createObject(String element, List names, List values) {
        if ( element == null || element.length() < 1 ){
            throw new IllegalArgumentException("illegal element length");
        }
        
        SiteData object = null;
        
        switch ( element.charAt(0) ) {
            // a alias
            case 'a':
                if ( element.equals( "alias" ) ) {
                    String alias = null;
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "name" ) ) {
                            alias = value;
                	    this.log( element, name, value );                              
                        } else {
                	    this.complain( element, name, value );
                        }
                    } 
                    return alias;
                }
                else{
                    return null;
                }
                
            //c connection
            case 'c':
                if ( element.equals( "connection" ) ) {
                    Connection c = new Connection();
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "key" ) ) {
                            c.setKey( value );
                	    this.log( element, name, value );                              
                        } else {
                	    this.complain( element, name, value );
                        }
                    } 
                    return c;
                }
                else{
                    return null;
                }
                
            //f
            case 'f':
                if( element.equals( "file-server" ) ){
                    FileServer fs = new FileServer();
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "protocol" ) ) {
                            fs.setProtocol( value );
                 	    this.log( element, name, value );                              
                        }
                        else if ( name.equals( "url" ) ) {
                            fs.setURLPrefix( value );
                 	    this.log( element, name, value );                              
                        }                        
                        else if ( name.equals( "mount-point" ) ) {
                            fs.setMountPoint( value );
                 	    this.log( element, name, value );                              
                        }
                        else {
                	      this.complain( element, name, value );
                        }
                        return fs;
                    } 
                }
                else{
                    return null;
                }
                
            //g  grid
            case 'g':
                if( element.equals( "grid" ) ){
                    GridGateway gw = new GridGateway();
                    for ( int i=0; i<names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "arch") ){
                            gw.setArchitecture( Architecture.valueOf( value ));
                 	    this.log( element, name, value );                              
                        }
                        else if ( name.equals( "type" ) ) {
                            gw.setType( GridGateway.TYPE.valueOf( value ) );
                 	    this.log( element, name, value );                              
                        }
                        else if ( name.equals( "contact" ) ) {
                            gw.setContact( value );
                 	    this.log( element, name, value );                              
                        }                        
                        else if ( name.equals( "scheduler" ) ) {
                            gw.setScheduler( GridGateway.SCHEDULER_TYPE.valueOf( value ));
                 	    this.log( element, name, value );                              
                        }                                    
                        else if ( name.equals( "job-type" ) ) {
                            gw.setJobType( GridGateway.JOB_TYPE.valueOf( value ));
                 	    this.log( element, name, value );                              
                        }
                        else if ( name.equals( "os") ){
                            gw.setOS( OS.valueOf( value ) );
                            this.log( element, name, value );                              
                        }
                        else if ( name.equals( "osrelease") ){
                            gw.setOSRelease( value );
                            this.log( element, name, value );                              
                        }
                        else if ( name.equals( "osversion") ){                            
                            gw.setOSVersion( value  );
                            this.log( element, name, value );                              
                        }
                        else if ( name.equals( "glibc") ){
                            gw.setGlibc( value );                            
                            this.log( element, name, value );                              
                        }
                        else {
                	      this.complain( element, name, value );
                        }
                        return gw;
                    } 
                }
                else{
                    return null;
                }
                
            //h head-fs
            case 'h':
                if( element.equals( "head-fs" ) ){
                    return new HeadNodeFS();
                }
                else{
                    return null;
                }
            
            //i  internal-mount-point
            case 'i':
                if( element.equals( "internal-mount-point" ) ){
                    InternalMountPoint imt = new InternalMountPoint();
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "mount-point" ) ) {
                            imt.setMountPoint( value );
                 	    this.log( element, name, value );                              
                        }
                        else if ( name.equals( "free-size" ) ) {
                            imt.setFreeSize( value );
                 	    this.log( element, name, value );                              
                        }
                        else if ( name.equals( "total-size" ) ) {
                            imt.setTotalSize( value );
                 	    this.log( element, name, value );                              
                        }                        
                        else {
                	      this.complain( element, name, value );
                        }
                    }
                    
                }
                else{
                    return null;
                }
                
            //l local                 
            case 'l':
                if( element.equals( "local" ) ){
                    return new LocalDirectory();
                }
                else{
                    return null;
                }
                
            //w worker-fs
            case 'w':
                if( element.equals( "worker-fs" ) ){
                    return new WorkerNodeFS();
                }
                else{
                    return null;
                }
                
           
        }
        
        return object;
    }

    /**
     * Returns the local path to the XML schema against which to validate.
     * 
     * @return path to the schema
     */
    public String getSchemaLocation() {
        // treat URI as File, yes, I know - I need the basename
        File uri = new File( SiteCatalogParser.SCHEMA_LOCATION );
        // create a pointer to the default local position
        File poolconfig = new File( this.mProps.getSysConfDir(),  uri.getName() );

        return this.mProps.getPoolSchemaLocation( poolconfig.getAbsolutePath() );

    }

    /**
     * 
     * @param element
     * @param attribute
     * @param value
     */
    private void log( String element, String attribute, String value) {
        //to be enabled when logging per queue.
        mLogger.log( "For element " + element + " found " + attribute + " -> " + value,
                     LogManager.DEBUG_MESSAGE_LEVEL );
    }
    
    /**
     * 
     * @param element
     * @param attribute
     * @param value
     */
    private void complain(String element, String attribute, String value) {
        mLogger.log( "For element " + element + " invalid attribute found " + attribute + " -> " + value,
                     LogManager.ERROR_MESSAGE_LEVEL );
    }
}

