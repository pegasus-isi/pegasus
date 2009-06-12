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


package org.griphyn.cPlanner.code.generator.condor.style;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.griphyn.cPlanner.code.generator.condor.CondorQuoteParserException;
import org.griphyn.cPlanner.code.generator.condor.CondorStyle;
import org.griphyn.cPlanner.code.generator.condor.CondorStyleException;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;

import org.griphyn.cPlanner.code.generator.condor.CondorQuoteParser;
import org.griphyn.cPlanner.namespace.VDS;

/**
 * Enables a job to be directly submitted to the condor pool of which the
 * submit host is a part of.
 * This style is applied for jobs to be run
 *        - on the submit host in the scheduler universe (local pool execution)
 *        - on the local condor pool of which the submit host is a part of
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CondorC extends Condor {

    
    /**
     * The constant for the remote universe key.
     */
    public static final String REMOTE_UNIVERSE_KEY =
                org.griphyn.cPlanner.namespace.Condor.REMOTE_UNIVERSE_KEY;

    
    /**
     * The name of the key that designates that files should be transferred
     * via Condor File Transfer mechanism.
     */
    public static final String SHOULD_TRANSFER_FILES_KEY =
             org.griphyn.cPlanner.namespace.Condor.SHOULD_TRANSFER_FILES_KEY;
    
    /**
     * The corresponding remote kye name that designates that files should be
     * transferred via Condor File Transfer mechanism.
     */
    public static final String REMOTE_SHOULD_TRANSFER_FILES_KEY = 
            org.griphyn.cPlanner.namespace.Condor.REMOTE_SHOULD_TRANSFER_FILES_KEY;
    /**
     * The name of key that designates when to transfer output.
     */
    public static final String WHEN_TO_TRANSFER_OUTPUT_KEY =
            org.griphyn.cPlanner.namespace.Condor.WHEN_TO_TRANSFER_OUTPUT_KEY;
    
    /**
     * The corresponding name of the remote key that designated when to transfer output.
     */
    public static final String REMOTE_WHEN_TO_TRANSFER_OUTPUT_KEY = 
            org.griphyn.cPlanner.namespace.Condor.REMOTE_WHEN_TO_TRANSFER_OUTPUT_KEY;
    
    /**
     * The name of the style being implemented.
     */
    public static final String STYLE_NAME = "CondorC";

    /**
     * The default constructor.
     */
    public CondorC() {
        super();
    }

    /**
     * Applies the CondorC style to the job.
     *
     * @param job  the job on which the style needs to be applied.
     *
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(SubInfo job) throws CondorStyleException{
        //lets apply the Condor style first and then make
        //some modifications
        super.apply(job);
        
        //the job universe key is translated to +remote_universe
        String remoteUniverse = (String) job.condorVariables.removeKey( Condor.UNIVERSE_KEY );
        job.condorVariables.construct( CondorC.REMOTE_UNIVERSE_KEY, remoteUniverse);
        
        //the universe for CondorC is always grid
        job.condorVariables.construct( CondorC.UNIVERSE_KEY, "grid" );
        
        //check if s_t_f and w_t_f keys are associated.
        try {
            String s_t_f = (String)job.condorVariables.removeKey( CondorC.SHOULD_TRANSFER_FILES_KEY );
            if( s_t_f != null ){       
                //convert to remote key and quote it
                job.condorVariables.construct( CondorC.REMOTE_SHOULD_TRANSFER_FILES_KEY,
                                               CondorQuoteParser.quote(s_t_f, true));
            }
            
            String w_t_f = (String)job.condorVariables.removeKey( CondorC.WHEN_TO_TRANSFER_OUTPUT_KEY );
            if( s_t_f != null ){       
                //convert to remote key and quote it
                job.condorVariables.construct( CondorC.REMOTE_WHEN_TO_TRANSFER_OUTPUT_KEY, 
                                               CondorQuoteParser.quote(w_t_f, true));
            }
            
            //initialdir makes sense only on submit node
            //so convert that to remote_initialdir
            String dir = (String)job.condorVariables.removeKey( "initialdir" );
            if( dir != null ){
                job.condorVariables.construct( "remote_initialdir", dir );
            }
            
        }
        catch ( CondorQuoteParserException ex) {
                throw new CondorStyleException( "Condor Quote Exception", ex);
        }
        
    }

}
