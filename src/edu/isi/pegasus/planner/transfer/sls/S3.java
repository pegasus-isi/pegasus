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

package edu.isi.pegasus.planner.transfer.sls;


import edu.isi.pegasus.common.logging.LogManager;


import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.refiner.CreateDirectory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Iterator;
import java.util.Set;


/**
 * This implementation of the SLS API allows us to use pegasus-transfer to retrieve
 * data from S3 bucket for worker node execution.
 *
 * @author Karan Vahi
 * @author Mats Rynge
 * @version $Revision$
 */
public class S3 extends Transfer3 {

    
    /**
     * An instance to the Create Direcotry Implementation being used in Pegasus.
     */
    private edu.isi.pegasus.planner.refiner.createdir.S3 mCreateDirImpl;
    
    /**
     * Initializes the SLS implementation.
     *
     * @param bag the bag of objects. Contains access to catalogs etc.
     */
    public void initialize( PegasusBag bag ) {
        super.initialize(bag);
        
        mCreateDirImpl = 
                (edu.isi.pegasus.planner.refiner.createdir.S3) CreateDirectory.loadCreateDirectoryImplementationInstance(bag);
    }
    
    /**
     * Modifies a job for the first level staging to headnode.This is to add
     * any files that needs to be staged to the head node for a job specific
     * to the SLS implementation. If any file needs to be added, a <code>FileTransfer</code>
     * object should be created and added as an input or an output file.
     * A job is not modified the staging of the sls file is turned of by
     * setting the property specified by STAGE_SLS_FILE_PROPERTY_KEY
     *
     *
     * @param job           the job
     * @param submitDir     the submit directory
     * @param slsInputLFN   the sls input file if required, that is used for
     *                      staging in from the head node to worker node directory.
     * @param slsOutputLFN  the sls output file if required, that is used
     *                      for staging in from the head node to worker node directory.
     * @return boolean
     *
     * @see #STAGE_SLS_FILE_PROPERTY_KEY
     */
    public boolean modifyJobForFirstLevelStaging( Job job,
                                                  String submitDir,
                                                  String slsInputLFN,
                                                  String slsOutputLFN ) {
        
        return super.modifyJobForFirstLevelStaging(job, submitDir, slsInputLFN, slsOutputLFN);
    }


    /**
     * Generates a second level staging file of the input files to the worker
     * node directory.
     *
     * @param job the job for which the file is being created
     * @param fileName the name of the file that needs to be written out.
     * @param submitDir the submit directory where it has to be written out.
     * @param headNodeDirectory the directory on the head node of the
     *   compute site.
     * @param workerNodeDirectory the worker node directory
     *
     * @return the full path to lof file created, else null if no file is
     *   written out.
     *
     */
    public File generateSLSOutputFile(Job job, String fileName,
                                      String submitDir,
                                      String headNodeDirectory,
                                      String workerNodeDirectory) {


        //sanity check
        if ( !needsSLSOutput( job ) ){
            mLogger.log( "Not Writing out a SLS output file for job " + job.getName() ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }

        File sls = null;
        Set files = job.getOutputFiles();

        
        String s3bucketURL = mCreateDirImpl.getBucketNameURL( job.getSiteHandle()  );
        
        String destDir = headNodeDirectory;
        String sourceDir = workerNodeDirectory;


        //writing the stdin file
        try {
            StringBuffer name = new StringBuffer();
            name.append( "sls_" ).append( job.getName() ).append( ".out" );
            sls = new File( submitDir, name.toString() );
            FileWriter input = new FileWriter( sls );
            PegasusFile pf;

            //To do. distinguish the sls file from the other input files
            for( Iterator it = files.iterator(); it.hasNext(); ){
                pf = ( PegasusFile ) it.next();

                //source
                input.write( "file://" );
                input.write( sourceDir ); input.write( File.separator );
                input.write( pf.getLFN() );
                input.write( "\n" );

                //destination point to the bucket
                input.write( s3bucketURL ); input.write( File.separator );
                input.write( pf.getLFN() );
                input.write( "\n" );

            }
            //close the stream
            input.close();

        } catch ( IOException e) {
            mLogger.log( "Unable to write the sls output file for job " + job.getName(), e ,
                         LogManager.ERROR_MESSAGE_LEVEL);
        }

        return sls;


    }

}
