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

import org.griphyn.vdl.euryale.FileFactory;
import org.griphyn.vdl.euryale.VirtualFlatFileFactory;

/**
 * A Flat creator implementation that returns the base directory always.
 * 
 * @author Karan Vahi
 */
public class Flat implements SubmitMapper{
    
    /**
     * Short description.
     */
    private static final String DESCRIPTION = "Flat Submit Directory Mapper";
    
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
    public Flat(){
        
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
            mFactory = new VirtualFlatFileFactory( options.getSubmitDirectory() );
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
        return  Flat.DESCRIPTION;
    }
    
}
