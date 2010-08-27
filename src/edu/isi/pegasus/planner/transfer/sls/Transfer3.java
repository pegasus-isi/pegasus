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

import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.namespace.ENV;


import edu.isi.pegasus.planner.transfer.SLS;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import java.io.File;

import org.griphyn.cPlanner.classes.Profile;
import java.util.List;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.common.util.Separator;
import java.util.ArrayList;

/**
 * This uses the python based transfer executable pegasus-transfer distributed with
 * Pegasus worker package to do the second level staging.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Transfer3 extends Transfer implements SLS {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "pegasus-transfer";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "pegasus-transfer";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION = "Python based Transfer Script for 3.0";

  
    
    /**
     * The default constructor.
     */
    public Transfer3() {
        super();
    }


    /**
     * Initializes the SLS implementation.
     *
     * @param bag the bag of objects. Contains access to catalogs etc.
     */
    public void initialize( PegasusBag bag ) {
        super.initialize(bag);

    }

    
    /**
     * Constructs a command line invocation for a job, with a given sls file.
     * The SLS maybe null. In the case where SLS impl does not read from a file,
     * it is advised to create a file in generateSLSXXX methods, and then read
     * the file in this function and put it on the command line.
     *
     * @param job          the job that is being sls enabled
     * @param slsFile      the slsFile can be null
     *
     * @return invocation string
     */
    public String invocationString( SubInfo job, File slsFile ){
        //sanity check
        if( slsFile == null ) { return null; }

        StringBuffer invocation = new StringBuffer();

        TransformationCatalogEntry entry = this.getTransformationCatalogEntry( job.getSiteHandle() );
        if( entry == null ){
            //cannot create an invocation
            return null;

        }

        //we need to set the x bit on proxy correctly first
        if( mLocalUserProxyBasename != null ){
            StringBuffer proxy = new StringBuffer( );
            proxy.append( slsFile.getParent() ).append( File.separator ).
                  append( mLocalUserProxyBasename);
            invocation.append( "/bin/bash -c \"chmod 600 " ).
                              append( proxy.toString() ).append(" && ");

            //backdoor to set the X509_USER_PROXY
            job.envVariables.construct( ENV.X509_USER_PROXY_KEY, proxy.toString() );
        }
        invocation.append( entry.getPhysicalTransformation() );

        
        //append any extra arguments set by user
        //in properties
        if( mExtraArguments != null ){
            invocation.append( " " ).append( mExtraArguments );
        }
        

        //add the required arguments to transfer
        invocation.append( " -f " );
        //we add absolute path if the sls files are staged via
        //first level staging
        if( this.mStageSLSFile ){
            invocation.append( slsFile.getAbsolutePath() );
            
        }
        else{
            //only the basename
            invocation.append( slsFile.getName() );
        }



        if( mLocalUserProxyBasename != null ){
            invocation.append( "\"" );
        }

        return invocation.toString();

    }

    
    
    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry(String siteHandle){
        List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.lookup( Transfer3.TRANSFORMATION_NAMESPACE,
                                                Transfer3.TRANSFORMATION_NAME,
                                                Transfer3.TRANSFORMATION_VERSION,
                                                siteHandle,
                                                TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " +
                Separator.combine( Transfer3.TRANSFORMATION_NAMESPACE,
                                   Transfer3.TRANSFORMATION_NAME,
                                   Transfer3.TRANSFORMATION_VERSION ) +
                " Cause:" + e, LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return ( tcentries == null ) ?
                 this.defaultTCEntry( Transfer3.TRANSFORMATION_NAMESPACE,
                                      Transfer3.TRANSFORMATION_NAME,
                                      Transfer3.TRANSFORMATION_VERSION,
                                      siteHandle ): //try using a default one
                 (TransformationCatalogEntry) tcentries.get(0);



    }


    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog.
     *
     * @param namespace  the namespace of the transfer transformation
     * @param name       the logical name of the transfer transformation
     * @param version    the version of the transfer transformation
     *
     * @param site  the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    protected  TransformationCatalogEntry defaultTCEntry(
                                                          String namespace,
                                                          String name,
                                                          String version,
                                                          String site ){

        TransformationCatalogEntry defaultTCEntry = null;
        //check if PEGASUS_HOME is set
        String home = mSiteStore.getPegasusHome( site );
        //if PEGASUS_HOME is not set, use VDS_HOME
        home = ( home == null )? mSiteStore.getVDSHome( site ): home;

        mLogger.log( "Creating a default TC entry for " +
                     Separator.combine( namespace, name, version ) +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //if home is still null
        if ( home == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for " +
                         Separator.combine( namespace, name, version ) +
                         " as PEGASUS_HOME or VDS_HOME is not set in Site Catalog" ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            //set the flag back to true
            return defaultTCEntry;
        }


        //remove trailing / if specified
        home = ( home.charAt( home.length() - 1 ) == File.separatorChar )?
            home.substring( 0, home.length() - 1 ):
            home;

        //construct the path to it
        StringBuffer path = new StringBuffer();
        path.append( home ).append( File.separator ).
            append( "bin" ).append( File.separator ).
            append( name );


        defaultTCEntry = new TransformationCatalogEntry( namespace,
                                                         name,
                                                         version );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site );
        defaultTCEntry.setType( TCType.INSTALLED );
        defaultTCEntry.setSysInfo( this.mSiteStore.lookup( site ).getSysInfo() );

        //register back into the transformation catalog
        //so that we do not need to worry about creating it again
        try{
            mTCHandle.insert( defaultTCEntry , false );
        }
        catch( Exception e ){
            //just log as debug. as this is more of a performance improvement
            //than anything else
            mLogger.log( "Unable to register in the TC the default entry " +
                          defaultTCEntry.getLogicalTransformation() +
                          " for site " + site, e,
                          LogManager.DEBUG_MESSAGE_LEVEL );
        }
        mLogger.log( "Created entry with path " + defaultTCEntry.getPhysicalTransformation(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
        return defaultTCEntry;
    }


   
    /**
     * Returns the environment profiles that are required for the default
     * entry to sensibly work. Tries to retrieve the following variables
     *
     * <pre>
     * PEGASUS_HOME
     * GLOBUS_LOCATION
     * LD_LIBRARY_PATH
     * </pre>
     *
     *
     * @param site the site where the job is going to run.
     *
     * @return List of environment variables, else empty list if none are found
     */
    protected List getEnvironmentVariables( String site ){
        List result = new ArrayList(2) ;

        String pegasusHome =  mSiteStore.getEnvironmentVariable( site, "PEGASUS_HOME" );
        if( pegasusHome != null ){
            //we have both the environment variables
            result.add( new Profile( Profile.ENV, "PEGASUS_HOME", pegasusHome ) );
        }

        String globus = mSiteStore.getEnvironmentVariable( site, "GLOBUS_LOCATION" );
        if( globus != null ){
            //check for LD_LIBRARY_PATH
            String ldpath = mSiteStore.getEnvironmentVariable( site, "LD_LIBRARY_PATH" );
            if ( ldpath == null ){
                //construct a default LD_LIBRARY_PATH
                ldpath = globus;
                //remove trailing / if specified
                ldpath = ( ldpath.charAt( ldpath.length() - 1 ) == File.separatorChar )?
                                ldpath.substring( 0, ldpath.length() - 1 ):
                                ldpath;

                ldpath = ldpath + File.separator + "lib";
                mLogger.log( "Constructed default LD_LIBRARY_PATH " + ldpath,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            }

            //we have both the environment variables
            result.add( new Profile( Profile.ENV, "GLOBUS_LOCATION", globus) );
            result.add( new Profile( Profile.ENV, "LD_LIBRARY_PATH", ldpath) );
        }

        return result;
    }


}
