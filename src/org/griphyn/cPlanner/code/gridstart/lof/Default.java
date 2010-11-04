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



package org.griphyn.cPlanner.code.gridstart.lof;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.code.gridstart.LOFGenerator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * The default version for LOF generator.
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class Default implements LOFGenerator{
    
    /**
     * The submit directory
     */
    private String mSubmitDir;
    
    /**
     * Handle to the LogManager
     */
    private LogManager mLogger;

    /**
     * Initializes the GridStart implementation.
     *
     * @param bag   the bag of objects that is used for initialization.
     * @param dag   the concrete dag so far.
     */
    public void initialize( PegasusBag bag, ADag dag ){
        mLogger = bag.getLogger();
        mSubmitDir    = bag.getPlannerOptions().getSubmitDirectory();
    }
    
    /**
     * Modifies a job for LOF file creation. If a LOF file has to be generated
     * call out to generateListofFilenamesFile
     * 
     * 
     * @param job the job to be modified.
     */
    public void modifyJobForLOFFiles( SubInfo job ){
        //inefficient check here again. just a prototype
        //we need to generate -S option only for non transfer jobs
        //generate the list of filenames file for the input and output files.
        if (! (job instanceof TransferJob)) {
            generateListofFilenamesFile( job, 
                                         job.getInputFiles(),
                                         mSubmitDir,
                                         job.getID() + ".in.lof");
            
        }
        
        //for cleanup jobs no generation of stats for output files
        if (job.getJobType() != SubInfo.CLEANUP_JOB) {
            generateListofFilenamesFile( job,
                                        job.getOutputFiles(),
                                         mSubmitDir,
                                        job.getID() + ".out.lof");

        
        }
        
    }
    
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
     public String generateListofFilenamesFile( SubInfo job,
                                                Set files, 
                                                String directory,
                                                String basename ){
         
         //sanity check
         if ( files == null || files.isEmpty() ){
             return null;
         }

         String result = null;
         //writing the stdin file
        try {
            File f = new File( directory, basename );
            FileWriter input;
            input = new FileWriter( f );
            PegasusFile pf;
            for( Iterator it = files.iterator(); it.hasNext(); ){
                pf = ( PegasusFile ) it.next();
                input.write( pf.getLFN() );
                input.write( "\n" );
            }
            //close the stream
            input.close();
            result = f.getAbsolutePath();

        } catch ( IOException e) {
            mLogger.log("Unable to write the lof file " + basename, e ,
                        LogManager.ERROR_MESSAGE_LEVEL);
        }

        return result;
     
         
     }

}
