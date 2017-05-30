/**
 *  Copyright 2007-2017 University Of Southern California
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
package edu.isi.pegasus.planner.code.gridstart.container.impl;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 *
 * @author Karan Vahi
 */
public abstract class Abstract implements ContainerShellWrapper{
    
    /**
     * The LogManager object which is used to log all the messages.
     */
    protected LogManager mLogger;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The submit directory where the submit files are being generated for
     * the workflow.
     */
    protected String mSubmitDir;
    
    /**
     * the planner options.
     */
    protected PlannerOptions mPOptions;
    
    
    public Abstract(){
        
    }
    
    
    /**
     * Initiailizes the Container  shell wrapper
     * @param bag 
     */
    public void initialize( PegasusBag bag ){
        mLogger    = bag.getLogger();
        mPOptions  = bag.getPlannerOptions();
        mSubmitDir = mPOptions.getSubmitDirectory();
    }
    
    /**
     * Convenience method to slurp in contents of a file into memory.
     *
     * @param directory  the directory where the file resides
     * @param file    the file to be slurped in.
     * 
     * @return StringBuffer containing the contents
     */
    protected StringBuffer slurpInFile( String directory, String file ) throws  IOException{
        StringBuffer result = new StringBuffer();
        //sanity check
        if( file == null ){
            return result;
        }

        BufferedReader in = new BufferedReader( new FileReader( new File(  directory, file )) );

        String line = null;

        while(( line = in.readLine() ) != null ){
            //System.out.println( line );
            result.append( line ).append( '\n' );
        }

        in.close();


        return result;
    }
}
