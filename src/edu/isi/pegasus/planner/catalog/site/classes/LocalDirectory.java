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

import java.io.Writer;
import java.io.IOException;

import java.util.Iterator;
import java.util.List;

/**
 * This data class represents a local directory on a site.
 * Can be local scratch or local storage. 
 * 
 * @version $Revision$
 * @author  Karan Vahi
 * 
 */
public class LocalDirectory extends DirectoryType{
    
    /**
     * The default constructor.
     */
    public LocalDirectory(){
        super();
    }
    
    /**
     * The overloaded constructor
     * 
     * @param  fs  list of file servers
     * @param  imt the internal mount point.
     */
    public LocalDirectory( List<FileServer> fs, InternalMountPoint imt ){
        super( fs, imt );
    }
    
    /**
     * Writes out the xml description of the object. 
     *
     * @param writer is a Writer opened and ready for writing. This can also
     *               be a StringWriter for efficient output.
     * @param indent the indent to be used.
     *
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML( Writer writer, String indent ) throws IOException {
        String newLine = System.getProperty( "line.separator", "\r\n" );
        String newIndent = indent + "\t";
        
        //write out the  xml element
        writer.write( indent );
        writer.write( "<local>" );
      
        //iterate through all the file servers
        for( Iterator<FileServer> it = this.getFileServersIterator(); it.hasNext(); ){
            FileServer fs = it.next();
            fs.toXML( writer, newIndent );
        }
        
        //write out the internal mount point
        this.getInternalMountPoint().toXML( writer, newIndent );
        
        writer.write( indent );
        writer.write( "</local>" );
        writer.write( newLine );
    }


}
