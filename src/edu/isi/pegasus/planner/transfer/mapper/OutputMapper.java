/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.isi.pegasus.planner.transfer.mapper;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * 
 * The interface that defines how to map the output files to a stage out site.
 * 
 *
 * @author vahi
 */
public interface OutputMapper {
   
    /**
     * The version of the API of the Output Mapper
     */
    public static final String VERSION = "1.0";

    /**
     * Initializes the mappers.
     *
     * @param bag   the bag of objects that is useful for initialization.
     * @param workflow   the workflow refined so far.
     *
     * 
     * @throws MapperException if unable to construct URL for any reason
     */
    public void initialize( PegasusBag bag, ADag workflow) throws MapperException;
    
    
    /**
     * 
     * Returns a URL for the lfn on the output site.
     * 
     * @param lfn          the lfn
     * @param site         the output site
     * @param operation    whether we want a GET or a PUT URL
     * 
     * @return the URL to be constructed 
     * 
     * @throws MapperException if unable to construct URL for any reason
     */
    public String getURL( String lfn , String site , FileServer.OPERATION operation ) throws MapperException;
    
    /**
     * Returns the full path on remote output site, where the lfn will reside, 
     * using the FileServer passed.
     *
     * @param lfn     the logical filename of the file.
     * @param server  the file server to use
     * 
     * @return the URL on the File Server
     * 
     * @throws MapperException if unable to construct URL for any reason
     */
    public String getURL( String lfn , FileServer server ) throws MapperException;
}
