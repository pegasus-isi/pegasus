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
package edu.isi.pegasus.planner.mapper.staging;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import static edu.isi.pegasus.planner.refiner.TransferEngine.REFINER_NAME;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.griphyn.vdl.euryale.HashedFileFactory;
import org.griphyn.vdl.euryale.VirtualHashedFileFactory;

/**
 * Hashed Mapper that bins all files associated with a job to a particular directory
 * on the staging site. 
 *
 * @author Karan Vahi
 */
public class Hashed implements StagingMapper{
    
    /**
     * The property key that indicates the multiplicator factor
     */
    public static final String MULIPLICATOR_PROPERTY_KEY = "hashed.multiplier";
    
    /**
     * //each job creates at creates the following files
            //  - submit file
            //  - out file
            //  - error file
            //  - prescript log
            //  - the partition directory
     */
    public static final int DEFAULT_MULTIPLICATOR_FACTOR = 5;
    
    /**
     * Short description.
     */
    private static final String DESCRIPTION = "Hashed Directory Mapper";
    
    /**
     * The property key that indicates the  number of levels to use
     */
    public static final String LEVELS_PROPERTY_KEY = "hashed.levels";
    
    /**
     * The default number of levels.
     */
    public static final int DEFAULT_LEVELS = 2;
    
    /**
     * The root of the directory tree under which other directories are created
     */
    private File mBaseDir;
    
    /**
     * Handle to the logger
     */
    private LogManager mLogger;
    
    /**
     * The File Factory to use
     */
    private HashedFileFactory mFactory;
    
    /**
     * A Map that tracks for each staging site, the LFN to the Add on's component
     * as determined from the factory. We need it to ensure that raw input 
     * files don't get staged multiple times to the same staging site, if they 
     * are required by more than one job.
     */
    private Map<String,Map<String,String>> mSiteLFNAddOnMap;
    
    /**
     * We track last seen job as this mapper assigns files per job encountered.
     */
    private String mLastSeenJobID;
    private String mLastAddon;
    
    private SiteStore mSiteStore;

    /**
     * Initializes the submit mapper
     * 
     * @param bag           the bag of Pegasus objects
     * @param properties    properties that can be used to control the behavior of the mapper
     */
    public void initialize( PegasusBag bag, Properties properties ){
        mLogger  = bag.getLogger();
        mSiteStore = bag.getHandleToSiteStore();
        Map<String,String> m = new HashMap();
        
         // create hashed, and levelled directories
        try {
            //we are interested in relative paths only
            //the intial path is determined from the site catalog entries
            HashedFileFactory creator = new VirtualHashedFileFactory( "" );

            int multiplicator = Hashed.DEFAULT_MULTIPLICATOR_FACTOR;
            if( properties.containsKey( Hashed.MULIPLICATOR_PROPERTY_KEY) ){
                multiplicator = Integer.parseInt( properties.getProperty(MULIPLICATOR_PROPERTY_KEY));
            }
            
            int levels = Hashed.DEFAULT_LEVELS;
            if( properties.containsKey( Hashed.LEVELS_PROPERTY_KEY) ){
                levels = Integer.parseInt( properties.getProperty(LEVELS_PROPERTY_KEY));
            }
            
            //each job creates at creates the following files
            //  - submit file
            //  - out file
            //  - error file
            //  - prescript log
            //  - the partition directory
            creator.setMultiplicator( multiplicator );

            //we want a minimum of one level always for clarity
            creator.setLevels(levels);

            //for the time being and test set files per directory to 50
            //mSubmitDirectoryCreator.setFilesPerDirectory( 10 );
            //mSubmitDirectoryCreator.setLevelsFromTotals( 100 );
            
            mFactory = creator;
        }
        catch ( IOException e ) {
            throw new RuntimeException(  e );
        }
    }
    
    /**
     * Maps a LFN to a location on the filesystem of a site and returns a single
     * externally accessible URL corresponding to that location.
     * 
     * 
     * @param job
     * @param lfn          the lfn
     * @param site         the staging site
     * @param operation    whether we want a GET or a PUT URL
     * 
     * @return the URL to file that was mapped
     * 
     * @throws MapperException if unable to construct URL for any reason
     */
    public String map(  Job job, String lfn  , SiteCatalogEntry site, FileServer.OPERATION operation ) throws MapperException{
        StringBuffer url = new StringBuffer();

        FileServer getServer = site.selectHeadNodeScratchSharedFileServer( operation );
        String siteHandle = site.getSiteHandle();
        if( getServer == null ){
            this.complainForScratchFileServer(job, operation, siteHandle);
        }

        url.append( getServer.getURLPrefix() ).
            append( mSiteStore.getExternalWorkDirectory(getServer, siteHandle ));
        
        //figure out the addon
        if( this.mLastSeenJobID == null || !job.getID().equals( this.mLastSeenJobID ) ){
            //we create a new add on as a new job encountered
            this.mLastSeenJobID = job.getID();
            try {
                this.mLastAddon = this.mFactory.createRelativeFile(lfn).getParent();
            } catch (IOException ex) {
                throw new MapperException( "Unable to determine relative shared scratch directory for LFN " + lfn +
                                           " on site " + site,
                                           ex);
            }
        }
        
        //check if we already have placed this file on the staging site
        //use that addOn then.
        url.append( File.separatorChar ).append( trackAndRetrieveLFNAddOn(siteHandle, lfn, mLastAddon) );
        
        if( lfn != null ){
            url.append( File.separatorChar ).append( lfn );
        }

        return url.toString();
    }

    public String description() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    /**
     * Tracks the lfn with addOn's on the various sites.
     * 
     * @param site
     * @param lfn
     * @param addOn 
     * 
     * @return  the actual addon to use. Returns existing addon if exists on site
     */
    private String trackAndRetrieveLFNAddOn( String site, String lfn, String addOn ){
        String actualAddon = addOn;
        if(  mSiteLFNAddOnMap.containsKey( site )  ){
            Map<String,String> m = mSiteLFNAddOnMap.get( site );
            String existing = m.get( lfn );
            if( existing == null ){
                //no existing addon tracked for LFN
                m.put( lfn, addOn );
            }
            else{
                actualAddon = existing;
            }
        }
        else{           
            Map<String,String> m = new HashMap();
            m.put( lfn, addOn );
            mSiteLFNAddOnMap.put( site, m );
        }
        return actualAddon;
    }

     /**
     * Complains for a missing head node file server on a site for a job
     *
     * @param job       the job
     * @param operation the operation
     * @param site      the site
     */
    private void complainForScratchFileServer( Job job,
                                                FileServer.OPERATION operation,
                                                String site) {
        this.complainForScratchFileServer( job.getID(), operation, site);
    }

    /**
     * Complains for a missing head node file server on a site for a job
     *
     * @param jobname  the name of the job
     * @param operation the file server operation
     * @param site     the site
     */
    private void complainForScratchFileServer( String jobname,
                                                FileServer.OPERATION operation,
                                                String site) {
        StringBuffer error = new StringBuffer();
        error.append( "[" ).append( REFINER_NAME ).append( "] ");
        if( jobname != null ){
            error.append( "For job (" ).append( jobname).append( ")." );
        }
        error.append( " File Server not specified for shared-scratch filesystem for site: ").
              append( site );
        throw new RuntimeException( error.toString() );

    }

}
