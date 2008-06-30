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

package edu.isi.pegasus.planner.catalog.site.classes;


import org.griphyn.common.util.Currently;


import java.io.IOException;
import java.io.Writer;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 * The site store contains the collection of sites backed by a HashMap.
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class SiteStore extends AbstractSiteData{
    
    /**
     * The "official" namespace URI of the site catalog schema.
     */
    public static final String SCHEMA_NAMESPACE = "http://pegasus.isi.edu/schema/sitecatalog";

    /**
     * The "not-so-official" location URL of the DAX schema definition.
     */
    public static final String SCHEMA_LOCATION = "http://pegasus.isi.edu/schema/sc-3.0.xsd";

    /**
     * The version to report.
     */
    public static final String SCHEMA_VERSION = "3.0";
    
    /**
     * The internal map that maps a site catalog entry to the site handle.
     */
    private Map<String, SiteCatalogEntry> mStore;
    
    /**
     * The default constructor.
     */
    public SiteStore(){
        initialize();
    }
    
    /**
     * The intialize method.
     */
    public void initialize() {        
        mStore = new HashMap<String, SiteCatalogEntry>( );
    }
    
    
    /**
     * Adds a site catalog entry to the store.
     * 
     * @param site  the site catalog entry.
     * 
     * @return previous value associated with specified key, or null 
     *         if there was no mapping for key
     */
    public SiteCatalogEntry addEntry( SiteCatalogEntry entry ){
        return this.mStore.put( entry.getSiteHandle() , entry );
    }
    
    /**
     * Returns an iterator to SiteCatalogEntry objects in the store.
     * 
     * @return Iterator<SiteCatalogEntry>
     */
    public Iterator<SiteCatalogEntry> entryIterator(){
        return this.mStore.values().iterator();
    }
    
    /**
     * Writes out the contents of the replica store as XML document
     * 
     * @param writer
     * @param indent
     * @throws java.io.IOException
     */
    public void toXML( Writer writer, String indent ) throws IOException {
        String newLine = System.getProperty( "line.separator", "\r\n" );
        indent = (indent != null && indent.length() > 0 ) ?
                 indent:
                 "";
        String newIndent = indent + "\t";
        
        //write out the xml header first.
        this.writeXMLHeader( writer, indent );
        
        //iterate through all the entries and spit them out.
        for( Iterator<SiteCatalogEntry> it = this.entryIterator(); it.hasNext(); ){
            it.next().toXML( writer, newIndent );
        }
        
        //write out the footer
        writer.write( indent );
        writer.write( "</sitecatalog>" );
        writer.write( newLine );
    }
    
    /**
     * Writes the header of the XML output. The output contains the special
     * strings to start an XML document, some comments, and the root element.
     * The latter points to the XML schema via XML Instances.
     *
     * @param stream is a stream opened and ready for writing. This can also
     *               be a string stream for efficient output.
     * @param indent is a <code>String</code> of spaces used for pretty
     *               printing. The initial amount of spaces should be an empty
     *               string. The parameter is used internally for the recursive 
     *               traversal.
     * 
     * @exception IOException if something fishy happens to the stream.
     */
    public void writeXMLHeader( Writer stream, String indent )
                                                    throws IOException  {
        
        String newline = System.getProperty( "line.separator", "\r\n" );
        indent = (indent != null && indent.length() > 0 ) ?
                 indent:
                 "";
        
        stream.write( indent );
        stream.write( "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" );
        stream.write( newline );

        stream.write( indent );        
        stream.write( "<!-- generated: " );
        stream.write( Currently.iso8601(false) );
        stream.write( " -->" );
        stream.write( newline );

        // who generated this document
        stream.write( indent );
        stream.write( "<!-- generated by: " );
        stream.write( System.getProperties().getProperty("user.name", "unknown") );
        stream.write( " [" );
        stream.write( System.getProperties().getProperty("user.region","??") );
        stream.write( "] -->" );
        stream.write( newline );

        // root element with elementary attributes
        stream.write( indent );
        stream.write( '<' );
        
        stream.write( "sitecatalog xmlns" );       
        stream.write( "=\"");
        stream.write( SCHEMA_NAMESPACE );
        stream.write( "\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"" );
        stream.write( SCHEMA_NAMESPACE );
        stream.write( ' ' );
        stream.write( SCHEMA_LOCATION );
        stream.write( '"' );
        writeAttribute( stream, "version", SCHEMA_VERSION );
        stream.write( '>' );
        stream.write( newline );
   }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone(){
        SiteStore obj;
        try{
            obj = ( SiteStore ) super.clone();
            obj.initialize();
           
             //iterate through all the entries and spit them out.
            for( Iterator<SiteCatalogEntry> it = this.entryIterator(); it.hasNext(); ){
                obj.addEntry( (SiteCatalogEntry)it.next().clone( ));
            }
        }
        catch( CloneNotSupportedException e ){
            //somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException("Clone not implemented in the base class of " + this.getClass().getName(),
                                       e );
        }
        return obj;
    }

     
    
}
