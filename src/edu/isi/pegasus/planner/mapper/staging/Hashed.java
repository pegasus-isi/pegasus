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
public class Hashed extends Abstract{
    
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
    private static final String DESCRIPTION = "Hashed Directory Staging Mapper";
    
    /**
     * The property key that indicates the  number of levels to use
     */
    public static final String LEVELS_PROPERTY_KEY = "hashed.levels";
    
    /**
     * The default number of levels.
     */
    public static final int DEFAULT_LEVELS = 2;
    
    
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
    private File mLastAddon;
    
   

    /**
     * Initializes the submit mapper
     * 
     * @param bag           the bag of Pegasus objects
     * @param properties    properties that can be used to control the behavior of the mapper
     */
    public void initialize( PegasusBag bag, Properties properties ){
        super.initialize(bag, properties);
        mSiteLFNAddOnMap = new HashMap<String,Map<String,String>>();
        
         // create hashed, and levelled directories
        try {
            //we are interested in relative paths only
            //the intial path is determined from the site catalog entries
            HashedFileFactory creator = new VirtualHashedFileFactory( "." );

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
     * Returns a virtual relative directory for the job. 
     * 
     * @param job   the job
     * @param site  site catalog entry
     * @param lfn   the lfn
     * 
     * @return 
     */
    public File mapToRelativeDirectory(Job job, SiteCatalogEntry site, String lfn) {
        //figure out the addon
        if( this.mLastSeenJobID == null || !job.getID().equals( this.mLastSeenJobID ) ){
            //we create a new add on as a new job encountered
            this.mLastSeenJobID = job.getID();
            try {
                this.mLastAddon = this.mFactory.createRelativeFile(lfn).getParentFile();
            } catch (IOException ex) {
                throw new MapperException( "Unable to determine relative shared scratch directory for LFN " + lfn +
                                           " on site " + site,
                                           ex);
            }
        }
        return this.mLastAddon;
    }

    
    /**
     * Returns description of mapper.
     * @return 
     */
    public String description() {
        return Hashed.DESCRIPTION;
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

     
   
}
