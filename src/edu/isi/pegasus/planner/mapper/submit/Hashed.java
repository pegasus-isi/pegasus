/**
 *  Copyright 2007-2016 University Of Southern California
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
package edu.isi.pegasus.planner.mapper.submit;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.mapper.SubmitMapper;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.griphyn.vdl.euryale.HashedFileFactory;

/**
 * A Hashed Submit Directory mapper that distributes the jobs across a hashed
 * directory structure
 * 
 * @author Karan Vahi
 */
public class Hashed implements SubmitMapper{
    
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
     * Default constructor.
     */
    public Hashed(){
        
    }

    
    /**
     * Initializes the submit mapper
     * 
     * @param bag           the bag of Pegasus objects
     * @param properties    properties that can be used to control the behavior of the mapper
     * @param base          the base directory relative to which all job directories are created
     */
    public void initialize(PegasusBag bag, Properties properties, File base) {
        mBaseDir = base;
        mLogger  = bag.getLogger();
        PlannerOptions options = bag.getPlannerOptions();
        
         // create hashed, and levelled directories
        try {
            //we are interested in relative paths
            HashedFileFactory creator = new HashedFileFactory( options.getSubmitDirectory() );

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

    public File getRelativeDir(Job job) {
        File f ;
        try {
            f = mFactory.createRelativeFile("pegasus");
        } catch (IOException ex) {
            throw new RuntimeException( "Error while determining relative submit dir for job " + job.getID() , ex);
        }
        return f.getParentFile();
    }

    public File getDir(Job job) {
        File f;
        try {
            f = mFactory.createFile( "pegasus");
        } catch (IOException ex) {
            throw new RuntimeException( "Error while determining relative submit dir for job " + job.getID() , ex);
        }
        return f.getParentFile();
    }
    
    /**
     * Returns a short description of the mapper.
     * 
     * @return 
     */
    public String description(){
        StringBuilder sb = new StringBuilder();
        sb.append(  Hashed.DESCRIPTION ).
           append( " with multiplier as " ).append( this.mFactory.getMultiplicator() ).
           append( " and levels " ).append( this.mFactory.getLevels() );
        return sb.toString();
    }
}
