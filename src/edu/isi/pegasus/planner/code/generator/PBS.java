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


package edu.isi.pegasus.planner.code.generator;

import edu.isi.pegasus.common.util.FindExecutable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;


import edu.isi.pegasus.planner.code.CodeGeneratorException;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * This code generator generates a PBS submit script for the workflow, that
 * can be submitted directly using qsub.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PBS extends Abstract {

    
    /**
     * A boolean indicating whether grid start has been initialized or not.
     */
    protected boolean mInitializeGridStart;

    
    /**
     * The default constructor.
     */
    public PBS( ){
        super();
        mInitializeGridStart = true;
    }

    /**
     * Initializes the Code Generator implementation.
     *
     *  @param bag   the bag of initialization objects.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize( PegasusBag bag ) throws CodeGeneratorException{
        super.initialize( bag );
        mLogger = bag.getLogger();
    }

    /**
     * Generates the code for the concrete workflow in the GRMS input format.
     * The GRMS input format is xml based. One XML file is generated per
     * workflow.
     *
     * @param dag  the concrete workflow.
     *
     * @return handle to the GRMS output file.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode( ADag dag ) throws CodeGeneratorException{
        Collection result = new ArrayList( 1 );

        //create a writer to the braindump.txt in the directory.
        File f = new File( this.getPathtoPBSFile( dag ));
        
        try{
            PrintWriter writer =
                  new PrintWriter(new BufferedWriter(new FileWriter(f)));
            
            writer.println( "#!/bin/bash" ); 
            writer.println( "#PBS -l nodes=1:ppn=2 "); 
            writer.println( "#PBS -l walltime=1:00:00 ");
            writer.println( "#PBS -o " + this.getDAGFilename( dag, ".out") );
            writer.println( "#PBS -e " + this.getDAGFilename( dag, ".err") );
            writer.println( "#PBS -N " + this.pbsBasename(dag) );
            writer.println( "cd " + mSubmitFileDir );

            //construct PMC invocation
            StringBuffer sb = new StringBuffer();
            sb.append( "mpiexec" ).//later on load via TC
               append( " " ).
               append( FindExecutable.findExec( "pegasus-mpi-cluster" ) );

            //append the arguments
            sb.append( " --jobstate-log --per-task-stdio" );
            sb.append( " --maxwalltime " ).append( 60 );
            sb.append( " ").append( this.getDAGFilename( dag, ".pbs") );

            writer.println( sb.toString() );


//Call it WORKFLOW.pbs, where WORKFLOW is the name of the workflow from the DAX and save it in the submit directory. The user can modify it for their environment manually. The important thing is to add the 3 arguments: --jobstate-log, --per-task-stdio, and --max-wall-time.)
            
            writer.close();
        
        } 
        catch (IOException ioe) {
            throw new CodeGeneratorException( "IOException while writing out the PBS file for the workflow" ,
                                               ioe );
        }
        
        
        
        result.add( f );        
        return result;

    }

    /**
     * Generates the code for a single job in the input format of the workflow
     * executor being used.
     *
     * @param dag    the dag of which the job is a part of.
     * @param job    the <code>Job</code> object holding the information about
     *               that particular job.
     *
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void generateCode( ADag dag, Job job ) throws CodeGeneratorException{
       throw new CodeGeneratorException( "The code generator only works on the workflow level" );
    }   
    
    /**
     * Returns a Map containing additional braindump entries that are specific
     * to a Code Generator
     * 
     * @param workflow  the executable workflow
     * 
     * @return Map
     */
    public  Map<String, String> getAdditionalBraindumpEntries( ADag workflow ) {
        Map entries = new HashMap();
        entries.put( Braindump.GENERATOR_TYPE_KEY, "pbs" );
        entries.put( "script", this.getPathtoPBSFile( workflow ) );
        return entries;
    }

    /**
     * Returns the basename for the PBS file for the dag
     * 
     * @param dag the workflow
     * 
     * @return the basenmae
     */
    protected String pbsBasename( ADag dag ) {
        StringBuffer name = new StringBuffer();
        name.append(  dag.getLabel() ).append( "-" ).
             append( dag.dagInfo.index ).append( ".pbs" );
        return name.toString();
    }
    

    /**
     * Returns the basename for the PBS script file for the dag
     *
     * @param dag the workflow
     *
     * @return the basenmae
     */
    protected String getPathtoPBSFile( ADag dag ) {
        StringBuilder script = new StringBuilder();
        script.append( this.mSubmitFileDir ).append( File.separator ).
               append( this.pbsBasename(dag) );
        return script.toString();
    }

    

}
