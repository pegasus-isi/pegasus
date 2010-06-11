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
import java.io.File;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.FileTransfer;

import edu.isi.pegasus.common.logging.LogManager;


import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.common.util.Separator;


import java.util.Properties;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.engine.createdir.WindwardImplementation;

/**
 * The implementation that creates stageout transfer jobs that retrieve data from 
 * the workflow specific knowledge base and puts in the 
 * BAE Data Characterization Catalog.
 * 
 * <p>
 * The client is generally invoked on the remote execution sites, unless
 * the user uses the thirdparty transfer option, in which case the transfer is
 * invoked on the submit host. The path to the executable client is determined
 * automatically on the basis of the environment variable DC_HOME set for 
 * the site in the site catalog.
 * 
 * <p>
 * The client is invoked with the following five parameters
 * <pre>
 *    -c,--creator <creator>                Creator
 *    -h,--dbhost <host name>               Database host name for data source
 *    -k,--kb <knowledge base>              Knowledge base name (full path)
 *    -p,--port <port>                      Database port for data source location
 *    -s,--source name <source name>        source name 
 * </pre>
 * 
 * <p>
 * In order to use the transfer implementation implemented by this class,
 * <pre>
 *        - the property pegasus.transfer.*.impl must be set to value BAERIC.
 * </pre>
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class BAERIC extends AbstractSingleFTPerXFERJob {

    /**
     * The environment variable from which to construct the path to DC_HOME
     */
    public static final String DC_HOME = "DC_HOME";
    
    
    
    /**
     * The creator to be associated with the datasets.
     */
    public static final String CREATOR = "SR";
    
    /**
     * The source of the datasets.
     */
    public static final String SOURCE = "WF_EXECUTION";
    
    /**
     * The allegro graph namespace that is to prepended.
     */
    public static final String ALLEGRO_NAMESPACE_PREFIX = "http://anchor/teo#";
    
    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "bae";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "dc-ric";

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
    public static final String DERIVATION_NAMESPACE = "bae";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "dc-ric";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION =
                    "BAE RIC client that populates that data in a Data Catalog";
    
    /***
     * The host for the AllegroGraph KB store.
     */
    private String mAllegroHost;
    
    /***
     * The port for the AllegroGraph KB store.
     */
    private String mAllegroPort;
    
    /**
     * the request id.
     */
    private String mRequestID;
    
    
    /**
     * Http URL for log4j properties.
     */
    private String mHttpLog4jURL;
   
    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param  bag  bag of intialization objects.
     */
    public BAERIC( PegasusBag bag ){
        super( bag );
        Properties p = mProps.matchingSubset( WindwardImplementation.ALLEGRO_PROPERTIES_PREFIX, false  );
        mLogger.log( "Allegro Graph properties set are " + p, LogManager.DEBUG_MESSAGE_LEVEL );
        mAllegroHost = p.getProperty( "host" );
        mAllegroPort = p.getProperty( "port" );
        String base  = p.getProperty( "basekb" );
        mRequestID        = mProps.getWingsRequestID();
        
        mHttpLog4jURL = mProps.getHttpLog4jURL();
        if( mHttpLog4jURL == null || mHttpLog4jURL.length() == 0 ){
            mLogger.log( "No http log4j url specified for request " + mRequestID, 
                         LogManager.WARNING_MESSAGE_LEVEL );
        }
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
     *]
     */
    public boolean useThirdPartyTransferAlways(){
        return false;
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
        return BAERIC.DESCRIPTION;
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
        TransformationCatalogEntry defaultTCEntry = null;
        mLogger.log( "Creating a default TC entry for " + BAERIC.COMPLETE_TRANSFORMATION_NAME +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL  );

        //fetch the DC_HOME environment variable
        SiteCatalogEntry s = mSiteStore.lookup( site );
        String dcHome = ( s == null )? null : s.getEnvironmentVariable( BAERIC.DC_HOME  );
        
        if( dcHome == null ){
           //cannot create default TC
           mLogger.log( BAERIC.DC_HOME + " is not set in site catalog for site " + site ,
                        LogManager.ERROR_MESSAGE_LEVEL  );
           //set the flag back to true
           return defaultTCEntry;
       }

       //construct the path to it
       StringBuffer path = new StringBuffer();
       path.append( dcHome ).append( File.separator ).
           append( "dc-register-ingest-characterize-client" ).append( File.separator ).
           append( "dc-ric.sh" );


       defaultTCEntry = new TransformationCatalogEntry( BAERIC.TRANSFORMATION_NAMESPACE,
                                                        BAERIC.TRANSFORMATION_NAME,
                                                        BAERIC.TRANSFORMATION_VERSION  );

       defaultTCEntry.setPhysicalTransformation( path.toString() );
       defaultTCEntry.setResourceId( site );
       defaultTCEntry.setType( TCType.INSTALLED );
       defaultTCEntry.addProfile( new Profile( Profile.ENV, BAERIC.DC_HOME, dcHome  )  );

       return defaultTCEntry;
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
               
        
        sb.append( " -g " ).
           append( "<" ).
           append( "\"" ).
           append( BAE.ALLEGRO_NAMESPACE_PREFIX ).
           append( file.getLFN() ).
           append( "\"" ).
           append( ">" ).
           append( " -u " ).append( file.getLFN() ).
           append( " -h " ).append( mAllegroHost ).
           append( " -p " ).append( mAllegroPort ).
           append( " -k " ).append( file.getDestURL().getValue() ).
           append( " -c " ).append( mProps.getProperty( "pegasus.windward.wf.id" ) ).
           append( " -s "  ).append( BAERIC.SOURCE ).
           append( " -o ").
           append( this.mHttpLog4jURL );
        
        //append some logging parameters
        sb.append( " -l " ).append( LoggingKeys.REQUEST_ID ).append( "=" ).append( mRequestID ).
           append( " -l " ).append( LoggingKeys.DAG_ID ).append( "=" ).append(  mProps.getProperty( "pegasus.windward.wf.id" ) ).
           append( " -l " ).append( LoggingKeys.JOB_ID ).append( "=" ).append( job.getID() );
        
        
        return sb.toString(); 

    }


    /**
     * Returns the namespace of the derivation that this implementation
     * refers to.
     *
     * @return the namespace of the derivation.
     */
    protected String getDerivationNamespace(){
        return BAERIC.DERIVATION_NAMESPACE;
    }


    /**
     * Returns the logical name of the derivation that this implementation
     * refers to.
     *
     * @return the name of the derivation.
     */
    protected String getDerivationName(){
        return BAERIC.DERIVATION_NAME;
    }

    /**
     * Returns the version of the derivation that this implementation
     * refers to.
     *
     * @return the version of the derivation.
     */
    protected String getDerivationVersion(){
        return BAERIC.DERIVATION_VERSION;
    }

    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name.
     */
    protected String getCompleteTCName(){
        return BAERIC.COMPLETE_TRANSFORMATION_NAME;
    }
}
