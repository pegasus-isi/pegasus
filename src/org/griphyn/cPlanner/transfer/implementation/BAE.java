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
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.FileTransfer;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.util.Separator;

import java.io.FileWriter;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.engine.createdir.WindwardImplementation;

/**
 * The implementation that creates transfer jobs that retrieve data from the 
 * BEA Data Characterization Catalog and puts in the workflow specific knowledge
 * base.
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
 *    - The uuid of the datasource
 *    - Hostname to disseminate to (such as wind.isi.edu)
 *    - Port on the host to use (such as 4567)
 *    - Directory on the host to use
 *    - The name of the allegrograph database to use
 * </pre>
 * 
 * <p>
 * In order to use the transfer implementation implemented by this class,
 * <pre>
 *        - the property pegasus.transfer.*.impl must be set to value BAE.
 * </pre>
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class BAE extends AbstractSingleFTPerXFERJob {

    /**
     * The environment variable from which to construct the path to DC_HOME
     */
    public static final String DC_HOME = "DC_HOME";
    
    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "bae";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "dc-transfer";

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
    public static final String DERIVATION_NAME = "dc-transfer";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION =
                    "BAE DC transfer client that populates that data in a Data Catalog";
    
    /***
     * The host for the AllegroGraph KB store.
     */
    private String mAllegroHost;
    
    /***
     * The port for the AllegroGraph KB store.
     */
    private String mAllegroPort;
    
    /***
     * The directory in the  allegrograph server.
     */
    private String mAllegroDirectory;
    
    /***
     * The database in the  allegrograph server.
     */
    private String mAllegroDatabase;
    
    
    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param  bag  bag of intialization objects.
     */
    public BAE( PegasusBag bag ){
        super( bag );
        Properties p = mProps.matchingSubset( WindwardImplementation.ALLEGRO_PROPERTIES_PREFIX, false  );
        mLogger.log( "Allegro Graph properties set are " + p, LogManager.DEBUG_MESSAGE_LEVEL );
        mAllegroHost = p.getProperty( "host" );
        mAllegroPort = p.getProperty( "port" );
        String base  = p.getProperty( "basekb" );
        File f = new File( base, mPOptions.getRelativeSubmitDirectory() );
        mAllegroDirectory = f.getParent();
        mAllegroDatabase  = f.getName();
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
     * @see PegasusProperties#getThirdPartySites(String)
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
        return this.DESCRIPTION;
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
        mLogger.log( "Creating a default TC entry for " + BAE.COMPLETE_TRANSFORMATION_NAME +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //fetch the DC_HOME environment variable
        SiteCatalogEntry s = mSiteStore.lookup( site );
        String dcHome = ( s == null )? null : s.getEnvironmentVariable( BAE.DC_HOME );
        
        if( dcHome == null ){
           //cannot create default TC
           mLogger.log( BAE.DC_HOME + " is not set in site catalog for site " + site ,
                        LogManager.ERROR_MESSAGE_LEVEL );
           //set the flag back to true
           return defaultTCEntry;
       }

       //construct the path to it
       StringBuffer path = new StringBuffer();
       path.append( dcHome ).append( File.separator ).
           append( "bin" ).append( File.separator ).
           append( "dc-transfer" );


       defaultTCEntry = new TransformationCatalogEntry( BAE.TRANSFORMATION_NAMESPACE,
                                                        BAE.TRANSFORMATION_NAME,
                                                        BAE.TRANSFORMATION_VERSION );

       defaultTCEntry.setPhysicalTransformation( path.toString() );
       defaultTCEntry.setResourceId( site );
       defaultTCEntry.setType( TCType.INSTALLED );
       defaultTCEntry.setProfile( new Profile( Profile.ENV, BAE.DC_HOME, dcHome ) );

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
        
        sb.append( file.getLFN() ).
           append( " " ).
           append( mAllegroHost ).
           append( " " ).
           append( mAllegroPort ).
           append( " " ).
           append( mAllegroDirectory ).
           append( " " ).
           append( mAllegroDatabase );
           
        
        return sb.toString(); 

    }


    /**
     * Returns the namespace of the derivation that this implementation
     * refers to.
     *
     * @return the namespace of the derivation.
     */
    protected String getDerivationNamespace(){
        return BAE.DERIVATION_NAMESPACE;
    }


    /**
     * Returns the logical name of the derivation that this implementation
     * refers to.
     *
     * @return the name of the derivation.
     */
    protected String getDerivationName(){
        return BAE.DERIVATION_NAME;
    }

    /**
     * Returns the version of the derivation that this implementation
     * refers to.
     *
     * @return the version of the derivation.
     */
    protected String getDerivationVersion(){
        return BAE.DERIVATION_VERSION;
    }

    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name.
     */
    protected String getCompleteTCName(){
        return BAE.COMPLETE_TRANSFORMATION_NAME;
    }
}
