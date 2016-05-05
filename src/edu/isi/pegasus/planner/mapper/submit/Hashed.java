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
import edu.isi.pegasus.planner.mapper.Creator;
import java.io.File;
import java.io.IOException;

import org.griphyn.vdl.euryale.FileFactory;
import org.griphyn.vdl.euryale.HashedFileFactory;

/**
 *
 * @author Karan Vahi
 */
public class Hashed implements Creator{
    
    /**
     * The root of the directory tree under which other directories are created
     */
    private File mBaseDir;
    
    /**
     * Handle to the logger
     */
    private LogManager mLogger;
    
    private FileFactory mFactory;
    
    /**
     * Default constructor.
     */
    public Hashed(){
        
    }

    public void initialize(PegasusBag bag, File base) {
        mBaseDir = base;
        mLogger  = bag.getLogger();
        PlannerOptions options = bag.getPlannerOptions();
        
         // create hashed, and levelled directories
        try {
            //we are interested in relative paths
            HashedFileFactory creator = new HashedFileFactory( options.getSubmitDirectory() );

            //each job creates at creates the following files
            //  - submit file
            //  - out file
            //  - error file
            //  - prescript log
            //  - the partition directory
            creator.setMultiplicator(5);

            //we want a minimum of one level always for clarity
            creator.setLevels(2);

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
    
}
