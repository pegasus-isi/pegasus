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

import org.griphyn.cPlanner.code.gridstart.*;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.classes.TCType;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.util.Separator;

/**
 * An extension to Kickstart Gridstart that tells kickstart to add
 * polling callouts to PDPS to check if the input data is already there
 * or not. It only creates an input LOF file for the compute jobs.
 * 
 * Each line in the LOF file is a destination URL for an input file on the head 
 * node of the compute site
 *
 * To use this the following properties need to be set
 * <pre>
 *      pegasus.gridstart.generate.lof.impl   PDPS
 *      pegasus.gridstart.generate.lof   true
 * </pre>
 * 
 * The path to the polling executable is discovered in the transformation catalog.
 * The code looks for an executable named pdps::poll
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class PDPS implements LOFGenerator {
    
    
    /**
     * The namespace for the polling executable.
     */
    public static final String POLL_TRANSFORMATION_NAMESPACE = "pdps";
    
    /**
     * The logical name for the polling script.
     */
    public static final String POLL_TRANSFORMATION_LOGICAL_NAME = "poll";

    /**
     * The version for the transformation.
     */
    public static final String POLL_TRANSFORMATION_VERSION = null;
    
    /**
     * The complete TC name for pdps polling.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                    POLL_TRANSFORMATION_NAMESPACE,
                                                                    POLL_TRANSFORMATION_LOGICAL_NAME,
                                                                    POLL_TRANSFORMATION_VERSION  );

    /**
     * Handle to Transformation Catalog.
     */
    protected TransformationCatalog mTCHandle;
    
    /**
     * Handle to the site catalog store.
     */
    protected SiteStore mSiteStore;
    
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
        mTCHandle = bag.getHandleToTransformationCatalog();
        mSiteStore = bag.getHandleToSiteStore();
    }
    
    /**
     * Modifies a job for LOF file creation. This is done  only when
     * the lof property pegasus.gridstart.generate.lof is set to true.
     * Currently, only input LOF files are created for the compute jobs,
     * as the Kickstart PREJOB invocations.
     * 
     * @param job the job to be modified.
     */
    public void modifyJobForLOFFiles( SubInfo job ) {
        //System.out.println( job.getID() );
        
        //only generate for compute jobs or clustered jobs.
        if ( job.getJobType() == SubInfo.COMPUTE_JOB ||
             job.getJobType() == SubInfo.STAGED_COMPUTE_JOB ) {
            String inputLOFBasename = job.getID() + ".in.lof";
            String submitDirFN = generateListofFilenamesFile( job,
                                                              job.getInputFiles(),
                                                              mSubmitDir,
                                                              inputLOFBasename );
            
            job.condorVariables.addIPFileForTransfer( submitDirFN );
            StringBuffer pollCommand = new StringBuffer();
            
            //retrieve the path to the polling script
            List entries = null;
            try{
                entries = mTCHandle.getTCEntries( PDPS.POLL_TRANSFORMATION_NAMESPACE,
                                                  PDPS.POLL_TRANSFORMATION_LOGICAL_NAME,
                                                  PDPS.POLL_TRANSFORMATION_VERSION,
                                                  job.getSiteHandle(),
                                                  TCType.INSTALLED  );
            }
            catch (Exception e) {
                //non sensical catching
                mLogger.log("Unable to retrieve entries from TC " +
                            e.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL );
            }



            TransformationCatalogEntry entry = ( entries == null ) ?
                                                 null  :
                                                 (TransformationCatalogEntry) entries.get(0);

            if( entry == null ){
                //NOW THROWN AN EXCEPTION

                //should throw a TC specific exception
                StringBuffer error = new StringBuffer();
                error.append("Could not find entry in tc for lfn ").
                    append( COMPLETE_TRANSFORMATION_NAME ).
                    append(" at site ").append( job.getSiteHandle() );

                mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
                throw new RuntimeException( error.toString() );
            }
            
            pollCommand.append( entry.getPhysicalTransformation() ).append( " -f " ).
                        append( inputLOFBasename );
            
            job.envVariables.construct( Kickstart.KICKSTART_PREJOB, pollCommand.toString() );
            
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

         //definite inconsitency as url prefix and mount point
         //are not picked up from the same server
         SiteCatalogEntry s = mSiteStore.lookup( job.getSiteHandle() );
         String execDirectoryURL = 
                 s.getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix() +
                                   mSiteStore.getWorkDirectory( job );
         
         
         String result = null;
         //writing the stdin file
        try {
            File f = new File( directory, basename );
            FileWriter input;
            input = new FileWriter( f );
            PegasusFile pf;
            for( Iterator it = files.iterator(); it.hasNext(); ){
                pf = ( PegasusFile ) it.next();
                //we write out the destination URL to the file
                input.write( execDirectoryURL + File.separator );
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
