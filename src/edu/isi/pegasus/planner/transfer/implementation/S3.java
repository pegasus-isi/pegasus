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


package edu.isi.pegasus.planner.transfer.implementation;

import java.io.FileWriter;
import java.util.Collection;
import java.util.Iterator;

import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.refiner.CreateDirectory;
import edu.isi.pegasus.planner.refiner.createdir.Implementation;


/**
 * A S3 implementation that extends on Transfer3 to creates the URL's to refer
 * to a S3 bucket instead of a directory on the head node of a cloud site.
 * 
 * @author Karan Vahi
 * @author Mats Rynge
 * @version $Revision$
 */

public class S3 extends Transfer3 {


    
    /**
     * The submit directory for the workflow.
     */
    private String mSubmitDirectory;

    
    /**
     * An instance to the Create Direcotry Implementation being used in Pegasus.
     */
    private edu.isi.pegasus.planner.refiner.createdir.S3 mS3CreateDirImpl;
    
    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param bag   the bag of initialization objects.
     */
    public S3( PegasusBag bag ) {
        super( bag );
        mSubmitDirectory = bag.getPlannerOptions().getSubmitDirectory();
        
        Implementation createDirImpl = 
                CreateDirectory.loadCreateDirectoryImplementationInstance(bag);
        //sanity check on the implementation
        if ( !( createDirImpl instanceof edu.isi.pegasus.planner.refiner.createdir.S3 )){
            throw new RuntimeException( "Only S3 Create Dir implementation can be used with S3 First Level Staging" );
        }
        mS3CreateDirImpl = (edu.isi.pegasus.planner.refiner.createdir.S3 )createDirImpl;
       
    }

    /**
     * hook to post process the created transfer jobs - we want to add the s3cfg file
     */
    public void postProcess( TransferJob job ){
        this.checkAndTransferS3cfg(job);
    }


    /**
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. A value of false, results in the
     * direct or peer to peer transfers being done.
     * <p>
     * A value of false does not preclude third party transfers. They still can
     * be done, by setting the property "pegasus.transfer.*.thirdparty.sites".
     *
     * @return boolean indicating whether to always use third party transfers
     *         or not.
     *
     */
    public boolean useThirdPartyTransferAlways(){
        return true;
    }

    /**
     * Writes to a FileWriter stream the stdin which goes into the magic script
     * via standard input
     *
     * @param writer    the writer to the stdin file.
     * @param files    Collection of <code>FileTransfer</code> objects containing
     *                 the information about sourceam fin and destURL's.
     * @param stagingSite the site where the data will be populated by first
     *                    level staging jobs.
     * @param jobClass    the job Class for the newly added job. Can be one of the
     *                    following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     *
     * @throws Exception
     */
    protected void writeJumboStdIn(FileWriter writer, Collection files, String stagingSite, int jobClass ) throws
        Exception {
        
        String bucket = this.mS3CreateDirImpl.getBucketNameURL( stagingSite) ;
        for(Iterator it = files.iterator();it.hasNext();){
            FileTransfer ft = (FileTransfer) it.next();
            NameValue source = ft.getSourceURL();
            //we want to leverage multiple dests if possible
            NameValue dest   = ft.getDestURL( true );

            //determine the type of command to issue on the basis of 
            //type of transfer job
            boolean stagein = ( jobClass == TransferJob.STAGE_IN_JOB ) ;
        
            String sourceURL;
            String destURL;
            if( stagein ){
                //stagein data to the bucket
                sourceURL = source.getValue() ;
                destURL =  bucket + "/"  + ft.getLFN();
            } 
            else{
                //stageout data from the bucket
                 sourceURL =  bucket + "/" + ft.getLFN();
                 destURL = dest.getValue();
                    
            }
        
            //write to the file one URL pair at a time
            StringBuffer urlPair = new StringBuffer( );
            urlPair.append( "#" ).append( source.getKey() ).append( "\n" ).
                    append( sourceURL ).append( "\n" ).
                    append( "#" ).append( dest.getKey() ).append( "\n" ).
                    append( destURL ).append( "\n" );
            writer.write( urlPair.toString() );
            writer.flush();
        }


    }

   

    
}
