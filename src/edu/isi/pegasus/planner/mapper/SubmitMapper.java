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

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.io.File;
import java.util.Properties;

/**
 * The interface that maps a directory for a job.
 *
 * @author Karan Vahi
 */
public interface SubmitMapper extends Mapper {
    
    /**
     * Prefix for the property subset to use with this mapper.
     */
    public static final String PROPERTY_PREFIX = "pegasus.dir.submit.mapper";
    
    /**
     * Initializes the submit mapper
     * 
     * @param bag           the bag of Pegasus objects
     * @param properties    properties that can be used to control the behavior of the mapper
     * @param base          the base directory relative to which all job directories are created
     */
    public void initialize( PegasusBag bag, Properties properties,  File base );
    
    public File getRelativeDir(Job job);

    public File getDir(Job job);
    
}
