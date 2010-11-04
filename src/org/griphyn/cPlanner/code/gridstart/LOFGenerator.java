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
package org.griphyn.cPlanner.code.gridstart;

import java.util.Set;
import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.SubInfo;

/**
 * The interface that dictates the contents of the LOF files and allows for
 * modification of the job for LOF purposes.
 * 
 * The implementing classes of this interface are only called if the following
 * property is set to true
 * 
 * <pre>
 *      pegasus.gridstart.generate.lof   true
 * </pre>
 * 
 * To specify what implementing class to use, user can specify
 * <pre>
 *      pegasus.gridstart.generate.lof.impl
 * </pre>
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public interface LOFGenerator {

    /**
     * Initializes the GridStart implementation.
     *
     * @param bag   the bag of objects that is used for initialization.
     * @param dag   the concrete dag so far.
     */
    public void initialize( PegasusBag bag, ADag dag );
    
    /**
     * Modifies a job for LOF file creation. If a LOF file has to be generated
     * call out to generateListofFilenamesFile
     * 
     * 
     * @param job the job to be modified.
     */
    public void modifyJobForLOFFiles( SubInfo job );
    
    /**
     * Writes out the list of filenames file for the job.
     *
     * @param job  the job
     * 
     * @param files  the list of <code>PegasusFile</code> objects contains the files
     *               whose stat information is required.
     *
     * @param directory  the submit directory
     * 
     * @param basename   the basename of the file that is to be created
     *
     * @return the full path to lof file created, else null if no file is written out.
     */
     public String generateListofFilenamesFile(  
                                                SubInfo job, 
                                                Set files,
                                                String directory,
                                                String basename );
}
