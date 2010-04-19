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

package org.griphyn.cPlanner.transfer.implementation;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.FileTransfer;


import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.engine.createdir.WindwardImplementation;

import edu.isi.pegasus.common.logging.LogManager;


import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.common.util.Separator;


import java.util.Properties;

import java.io.File;
import java.util.List;
import org.griphyn.cPlanner.engine.CreateDirectory;
import org.griphyn.cPlanner.engine.createdir.Implementation;
import org.griphyn.cPlanner.namespace.VDS;

/**
 * The implementation that creates transfer jobs that retrieve data from the 
 * a S3 bucket using the s3cmd command. The S3cmd allows only for one transfer
 * at a time. Hence, this mode extends the SingleTransfer Per Node API.
 *
 * <p>
 * The s3cmd client is always invoked on the submit node.
 * 
 * <p>
 * To stagein data, it <code>put</code> data in the bucket. To stageout data,
 * we retrieve data from the bucket using the <code>get</code> command.
 * 
 * 
 * <p>
 * In order to use the transfer implementation implemented by this class,
 * <pre>
 *        - the property pegasus.transfer.*.impl must be set to value S3Cmd.
 * </pre>
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class S3Cmd extends AbstractSingleFTPerXFERJob {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "amazon";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "s3cmd";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;
    
    /**
     * The complete TC name for dc-client.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                 TRANSFORMATION_NAMESPACE,
                                                                 TRANSFORMATION_NAME,
                                                                 TRANSFORMATION_VERSION  );

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "amazon";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "s3cmd";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION =
                    "S3 transfer client that allows us to put and retreive data from S3 buckets";
    
    
    
    /**
     * An instance to the Create Direcotry Implementation being used in Pegasus.
     */
    private org.griphyn.cPlanner.engine.createdir.S3 mS3CreateDirImpl;
    
    
    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param  bag  bag of intialization objects.
     */
    public S3Cmd( PegasusBag bag ){
        super( bag );
        Properties p = mProps.matchingSubset( WindwardImplementation.ALLEGRO_PROPERTIES_PREFIX, false  );
        
        Implementation createDirImpl = 
                CreateDirectory.loadCreateDirectoryImplementationInstance(bag);
        //sanity check on the implementation
        if ( !( createDirImpl instanceof org.griphyn.cPlanner.engine.createdir.S3 )){
            throw new RuntimeException( "Only S3 Create Dir implementation can be used with S3 First Level Staging" );
        }
        mS3CreateDirImpl = (org.griphyn.cPlanner.engine.createdir.S3 )createDirImpl;
        
    }

    /**
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. A value of false, results in the
     * direct or peer to peer transfers being done.
     * <p>
     * A value of false does not preclude third party transfers. They still can
     * be done, by setting the property "vds.transfer.*.thirdparty.sites".
     *
     * @return boolean indicating whether to always use third party transfers
     *         or not.
     *
     */
    public boolean useThirdPartyTransferAlways(){
        return true;
    }

    /**
     * Returns a boolean indicating whether the transfer protocol being used by
     * the implementation preserves the X Bit or not while staging.
     *
     * @return boolean
     */
    public boolean doesPreserveXBit(){
        return false;
    }


    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public  String getDescription(){
        return S3Cmd.DESCRIPTION;
    }

    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param site the site for which the path is required.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry( String site ){
       List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.getTCEntries( S3Cmd.TRANSFORMATION_NAMESPACE,
                                                S3Cmd.TRANSFORMATION_NAME,
                                                S3Cmd.TRANSFORMATION_VERSION,
                                                site,
                                                TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " + getCompleteTCName()
                + " :" + e.getMessage(),LogManager.ERROR_MESSAGE_LEVEL);
        }

        //see if any record is returned or not
        return(tcentries == null)?
               null:
              (TransformationCatalogEntry) tcentries.get(0);
    }
    
    

    /**
     * It constructs the arguments to the transfer executable that need to be passed
     * to the executable referred to in this transfer mode. The client is invoked with
     * the following five parameters
     * <pre>
     *    - The uuid of the datasource
     *    - Hostname to disseminate to (such as wind.isi.edu)
     *    - Port on the host to use (such as 4567)
     *    - Directory on the host to use
     *    - The name of the allegrograph database to use
     * </pre>
     * 
     *
     * @param job  the transfer job that is being created.
     * @param file the FileTransfer that needs to be done.
     * 
     * @return  the argument string
     */
    protected String generateArgumentString( TransferJob job, FileTransfer file ){
        StringBuffer sb = new StringBuffer();
               
        //add any arguments that might have been passed through properties
        if( job.vdsNS.containsKey( VDS.TRANSFER_ARGUMENTS_KEY) ){
            sb.append( " " ).append( ( String )job.vdsNS.get( VDS.TRANSFER_ARGUMENTS_KEY ) );
        }
        
        //determine the type of command to issue on the basis of 
        //type of transfer job
        String type = ( job.getJobType() == TransferJob.STAGED_COMPUTE_JOB ||
                        job.getJobType() == TransferJob.STAGE_IN_JOB ) ?
                        "put" : //used for stagein
                        "get" ; //used for stageout
        
        sb.append(  " " );
        sb.append( type );
        sb.append( " " );
        
        if( type.equals( "put" ) ){
            //stagein data to the bucket
            sb.append(  " " );
            sb.append( file.getSourceURL().getValue() );
            sb.append( " " );
            
            /*
            sb.append( "s3://" ).
               append( mBucketName ).*/
            sb.append( this.mS3CreateDirImpl.getBucketNameURL( job.getNonThirdPartySite() )).
               append( "/" ).
               append( file.getLFN() );
        } 
        else{
            //stagein data to the bucket
            /*
            sb.append( "s3://" ).
               append( mBucketName ).*/
            sb.append( this.mS3CreateDirImpl.getBucketNameURL( job.getNonThirdPartySite() )).
               append( "/" ).
               append( file.getLFN() );
            
            
            String dest = file.getDestURL().getValue();
            //some sanitization if reqd 
            if( dest.startsWith( "file:///" ) ){
                dest = dest.substring( 7 );
            }
            sb.append(  " " );
            sb.append( dest );
            sb.append( " " );
            
        }
        
        return sb.toString(); 

    }


    /**
     * Returns the namespace of the derivation that this implementation
     * refers to.
     *
     * @return the namespace of the derivation.
     */
    protected String getDerivationNamespace(){
        return S3Cmd.DERIVATION_NAMESPACE;
    }


    /**
     * Returns the logical name of the derivation that this implementation
     * refers to.
     *
     * @return the name of the derivation.
     */
    protected String getDerivationName(){
        return S3Cmd.DERIVATION_NAME;
    }

    /**
     * Returns the version of the derivation that this implementation
     * refers to.
     *
     * @return the version of the derivation.
     */
    protected String getDerivationVersion(){
        return S3Cmd.DERIVATION_VERSION;
    }

    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name.
     */
    protected String getCompleteTCName(){
        return S3Cmd.COMPLETE_TRANSFORMATION_NAME;
    }
}
