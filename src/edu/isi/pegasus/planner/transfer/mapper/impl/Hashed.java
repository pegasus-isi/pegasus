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
package edu.isi.pegasus.planner.transfer.mapper.impl;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.transfer.mapper.MapperException;
import java.io.IOException;
import java.util.Iterator;
import org.griphyn.vdl.euryale.FileFactory;
import org.griphyn.vdl.euryale.VirtualDecimalHashedFileFactory;


/**
 * Maps the output files in a Hashed Directory structure on the output site.
 * 
 * @author Karan Vahi
 * @see org.griphyn.vdl.euryale.VirtualDecimalHashedFileFactory;
 */
public class Hashed extends AbstractFileFactoryBasedMapper {

    
    /**
     * The short name for the mapper
     */
    public static final String SHORT_NAME = "Hashed";

    
    
    /**
     * Initializes the mappers.
     *
     * @param bag   the bag of objects that is useful for initialization.
     * @param workflow   the workflow refined so far.
     *
     */
    public void initialize( PegasusBag bag, ADag workflow)  throws MapperException{
        super.initialize(bag, workflow);
    }
    
    /**
     * Method that instantiates the FileFactory
     * 
     * @param bag   the bag of objects that is useful for initialization.
     * @param workflow   the workflow refined so far.
     * 
     * @return the handle to the File Factory to use 
     */
    public  FileFactory instantiateFileFactory( PegasusBag bag, ADag workflow ){
        FileFactory factory;
        
        //all file factories intialized with the addon component only
        try {

            String addOn = mSiteStore.getRelativeStorageDirectoryAddon( );
            //get the total number of files that need to be stageout
            int totalFiles = 0;
            for ( Iterator it = workflow.jobIterator(); it.hasNext(); ){
                Job job = ( Job )it.next();

                //traverse through all the job output files
                for( Iterator opIt = job.getOutputFiles().iterator(); opIt.hasNext(); ){
                    if( !((PegasusFile)opIt.next()).getTransientTransferFlag() ){
                        //means we have to stage to output site
                        totalFiles++;
                    }
                }
            }

            factory =  new VirtualDecimalHashedFileFactory( addOn, totalFiles );

            //each stageout file  has only 1 file associated with it
            ((VirtualDecimalHashedFileFactory)factory).setMultiplicator( 1 );
        }catch ( IOException ioe ) {
            throw new MapperException( this.getErrorMessagePrefix() + "Unable to intialize the Flat File Factor " ,
                                            ioe );
        }
        return factory;
    }
    
    /**
     * Returns the short name for the implementation class.
     * 
     * @return 
     */
    public  String getShortName(){
        return Hashed.SHORT_NAME;
    }
   
    
    
}
