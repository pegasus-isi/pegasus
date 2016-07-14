/**
 *  Copyright 2007-2016 University Of Southern California
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

package edu.isi.pegasus.planner.mapper;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.io.File;
import java.util.Properties;

/**
 * The interface that maps a directory on the shared scratch of staging site for the job.
 *
 * @author Karan Vahi
 */
public interface StagingMapper extends Mapper {
    
    /**
     * Prefix for the property subset to use with this mapper.
     */
    public static final String PROPERTY_PREFIX = "pegasus.dir.staging.mapper";
    
    /**
     * Internal API version for the Submit Mapper
     */
    public static final String VERSION = "1.0";
    
    /**
     * Initializes the submit mapper
     * 
     * @param bag           the bag of Pegasus objects
     * @param properties    properties that can be used to control the behavior of the mapper
     */
    public void initialize( PegasusBag bag, Properties properties );
    
    /**
     * Returns a virtual relative directory for the job. 
     * 
     * @param job   the job
     * @param site  site catalog entry
     * @param lfn   the lfn
     * 
     * @return 
     */
    public File mapToRelativeDirectory(Job job, SiteCatalogEntry site, String lfn );
    
    /**
     * Maps a LFN to a location on the filsystem of a site and returns a single
     * externally accessible URL corresponding to that location.
     * 
     * 
     * @param job
     * @param addOn
     * @param site         the staging site
     * @param operation    whether we want a GET or a PUT URL
     * @param lfn          the lfn
     * 
     * @return the URL to file that was mapped
     * 
     * @throws MapperException if unable to construct URL for any reason
     */
    public String map(  Job job, File addOn,  SiteCatalogEntry site, FileServer.OPERATION operation, String lfn  ) throws MapperException;

    
}
